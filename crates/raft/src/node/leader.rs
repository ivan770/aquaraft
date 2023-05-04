use alloc::{borrow::ToOwned, vec::Vec};
use core::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    num::NonZeroU64,
    pin::{pin, Pin},
    time::Duration,
};

use aquaraft_vecmap::VecMap;
use enstream::{enstream, HandlerFn, HandlerFnLifetime, Yielder};
use futures_util::{
    select_biased,
    stream::{FusedStream, StreamExt},
    FutureExt,
};

use crate::{
    messages::{AppendEntriesRequest, Request, Response, RpcEntity, ValidationOutcome},
    node::HEARTBEAT_TIMER_RANGE,
    option_ext::OptionExt,
    state::{LogEntry, LogEntryContents, LogEntryMetadata, PersistentState, UnifiedState},
    yielder_ext::TryYielderExt,
    AppendEntryOutcome, IncomingRaftNetworkEvent, MembershipChangeOutcome, NodeOutput,
    OutgoingRaftNetworkEvent,
};

#[cfg(feature = "diagnostics")]
use crate::{Diagnostics, DiagnosticsNodeType};

#[derive(Copy, Clone, Debug)]
struct NodeInformation {
    inflight_request: Option<NonZeroU64>,
    next_index: NonZeroU64,
    match_index: Option<NonZeroU64>,
}

impl NodeInformation {
    fn from_last_committed_entry_id(id: Option<NonZeroU64>) -> Self {
        Self {
            inflight_request: None,
            next_index: id
                .map(|entry| entry.saturating_add(1))
                .unwrap_or(NonZeroU64::MIN),
            match_index: None,
        }
    }
}

pub(crate) type LeaderNode<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
= impl FusedStream<Item = Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>
    + Send
    + Sync;

struct LeaderStreamState<'a, NodeId, PS, S> {
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    active_membership_change: Option<(NonZeroU64, bool)>,
}

type LeaderStreamStateFut<'yielder, 'a: 'yielder, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
= impl Future<Output = ()> + Send + Sync;

impl<'yielder, 'a, NodeId, RequestId, PS, S>
    HandlerFnLifetime<'yielder, Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>
    for LeaderStreamState<'a, NodeId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    type Fut = LeaderStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S>;
}

impl<'a, NodeId, RequestId, PS, S>
    HandlerFn<Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>
    for LeaderStreamState<'a, NodeId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    fn call<'yielder>(
        mut self,
        mut yielder: Yielder<'yielder, Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>,
    ) -> LeaderStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S> {
        async move {
            let timer_duration = self.unified_state.random(HEARTBEAT_TIMER_RANGE);

            let mut heartbeat_timer = pin!(
                yielder
                    .try_with_yield(
                        self.unified_state
                            .interval(Duration::from_millis(timer_duration))
                            .await,
                    )
                    .await
            );

            let mut active_membership_change = self
                .active_membership_change
                .map(|(id, should_shutdown)| (None, id, should_shutdown));

            let mut node_information = self
                .unified_state
                .connected_nodes()
                .cloned()
                .map(|node_id| {
                    (
                        node_id,
                        NodeInformation::from_last_committed_entry_id(
                            self.unified_state.last_committed_entry_id(),
                        ),
                    )
                })
                .collect::<VecMap<_, _>>();

            loop {
                select_biased! {
                    _ = heartbeat_timer.next().fuse() => {
                        let suitable_next_indices;

                        if let Some(last_metadata) = self.unified_state.last_log_metadata()
                            && let Some(min_log_entry_id) = {
                                let mut min_log_entry_id = None;

                                suitable_next_indices = node_information.iter_mut()
                                    .filter(|(_, info)| info.inflight_request.is_none())
                                    .inspect(|(_, info)| match min_log_entry_id {
                                        Some(existing_id) if info.next_index < existing_id => min_log_entry_id = Some(info.next_index),
                                        None => min_log_entry_id = Some(info.next_index),
                                        _ => {}
                                    })
                                    .collect::<Vec<_>>();

                                min_log_entry_id
                            }
                        {
                            // Logs are not empty, it's time to shine!

                            trace!(?last_metadata, %min_log_entry_id, "at least one next_index is delayed, preparing for log replication");

                            let start_id = NonZeroU64::new(min_log_entry_id.get().saturating_sub(1))
                                .unwrap_or(NonZeroU64::MIN);

                            let suitable_logs = yielder.try_with_yield(
                                self.unified_state
                                    .log_entries(start_id)
                                    .await
                            ).await;

                            let new_next_index = start_id.saturating_add(suitable_logs.len() as u64);

                            trace!(
                                %start_id,
                                log_len = %suitable_logs.len(),
                                "acquired log data"
                            );

                            for (connected_node, info) in suitable_next_indices {
                                let previous_log_entry_id = NonZeroU64::new(info.next_index.get().saturating_sub(1));

                                trace!(
                                    ?connected_node,
                                    ?previous_log_entry_id,
                                    %info.next_index,
                                    "replicating for a connected_node"
                                );

                                info.inflight_request = Some(new_next_index);

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::Send(
                                            connected_node.clone(),
                                            Request::AppendEntries(AppendEntriesRequest {
                                                term: self.unified_state.current_term(),
                                                prev_log_entry: previous_log_entry_id.map(|id| suitable_logs[(id.get() - start_id.get()) as usize].metadata),
                                                entries: suitable_logs[(info.next_index.get() - start_id.get()) as usize..].to_vec(),
                                                commit_index: self.unified_state.last_committed_entry_id()
                                            }),
                                        ),
                                    )))
                                    .await;
                            }
                        } else {
                            trace!("found no delayed next_index, broadcasting heartbeat");

                            let prev_log_entry = self.unified_state.last_log_metadata();

                            let new_next_index = prev_log_entry.map(|entry| entry.id.saturating_add(1))
                                .unwrap_or(NonZeroU64::MIN);

                            for (connected_node, info) in node_information.iter_mut().filter(|(_, info)| info.inflight_request.is_none()) {
                                info.inflight_request = Some(new_next_index);

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::Send(
                                            connected_node.clone(),
                                            Request::AppendEntries(AppendEntriesRequest {
                                                term: self.unified_state.current_term(),
                                                prev_log_entry,
                                                entries: Vec::new(),
                                                commit_index: self.unified_state.last_committed_entry_id()
                                            }),
                                        ),
                                    )))
                                    .await;
                            }
                        }
                    },
                    event = self.incoming_stream.next().fuse() => {
                        match event {
                            Some(
                                IncomingRaftNetworkEvent::AddNode(request_id, node_id)
                                | IncomingRaftNetworkEvent::RemoveNode(request_id, node_id),
                            ) if active_membership_change.is_some() => {
                                trace!(?node_id, "there is an active membership change already, rejecting client request");

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                                            request_id,
                                            MembershipChangeOutcome::Reject(node_id)
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::AddNode(request_id, node_id)) => {
                                trace!(?node_id, "no active membership changes were found, processing AddNode");

                                let last_log_metadata = self.unified_state.last_log_metadata();

                                let id = last_log_metadata.map(|metadata| metadata.id.saturating_add(1))
                                    .unwrap_or(NonZeroU64::MIN);

                                // Since nodes should apply membership changes as soon as possible, we apply those changes right now.
                                self.unified_state.add_connected_node(node_id.clone());

                                let new_node_configuration = self.unified_state
                                    .raw_node_configuration()
                                    .to_owned();

                                yielder.try_with_yield(
                                    self.unified_state
                                        .append_log_entry(LogEntry {
                                            contents: LogEntryContents::ChangeMembership(new_node_configuration),
                                            metadata: LogEntryMetadata {
                                                id,
                                                term: self.unified_state.current_term()
                                            }
                                        })
                                        .await
                                ).await;

                                active_membership_change = Some((Some(request_id), id, false));

                                node_information.insert(
                                    node_id,
                                    NodeInformation::from_last_committed_entry_id(
                                        self.unified_state
                                            .last_committed_entry_id()
                                    ),
                                );
                            },
                            Some(IncomingRaftNetworkEvent::RemoveNode(request_id, node_id)) => {
                                trace!(?node_id, "no active membership changes were found, processing RemoveNode");

                                let last_log_metadata = self.unified_state.last_log_metadata();

                                let id = last_log_metadata.map(|metadata| metadata.id.saturating_add(1))
                                    .unwrap_or(NonZeroU64::MIN);

                                // Since nodes should apply membership changes as soon as possible, we apply those changes right now.
                                self.unified_state.remove_connected_node(&node_id);

                                let new_node_configuration = self.unified_state
                                    .raw_node_configuration()
                                    .to_owned();

                                yielder.try_with_yield(
                                    self.unified_state
                                        .append_log_entry(LogEntry {
                                            contents: LogEntryContents::ChangeMembership(new_node_configuration),
                                            metadata: LogEntryMetadata {
                                                id,
                                                term: self.unified_state.current_term()
                                            }
                                        })
                                        .await
                                ).await;

                                if &node_id != self.unified_state.my_node_id() {
                                    node_information.remove(&node_id);
                                }

                                if node_information.is_empty() {
                                    yielder.try_with_yield(
                                        self.unified_state
                                            .apply_log_entries(id)
                                            .await
                                    ).await;

                                    yielder
                                        .yield_item(Ok(NodeOutput::ExternalEvent(
                                            OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                                                request_id,
                                                MembershipChangeOutcome::Success
                                            ),
                                        )))
                                        .await;

                                    // RemoveNode is self-referential, so the current node
                                    // is ought to be shutdown.
                                    if &node_id == self.unified_state.my_node_id() {
                                        return;
                                    }
                                } else {
                                    active_membership_change = Some((Some(request_id), id, &node_id == self.unified_state.my_node_id()));
                                }
                            },
                            Some(IncomingRaftNetworkEvent::AppendEntry(request_id, contents)) => {
                                let id = self.unified_state.last_log_metadata()
                                    .map(|metadata| metadata.id.saturating_add(1))
                                    .unwrap_or(NonZeroU64::MIN);

                                yielder.try_with_yield(
                                    self.unified_state
                                        .append_log_entry(LogEntry {
                                            contents: LogEntryContents::GenericEntry(contents),
                                            metadata: LogEntryMetadata {
                                                id,
                                                term: self.unified_state.current_term()
                                            }
                                        })
                                        .await
                                ).await;

                                // Immediately commit log entries if we are in a single-node cluster.
                                if node_information.is_empty() {
                                    yielder.try_with_yield(
                                        self.unified_state
                                            .apply_log_entries(id)
                                            .await
                                    ).await;
                                }

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::AppendEntryOutcome(
                                            request_id,
                                            AppendEntryOutcome::Success
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::PushRequest(node_id, request_id, request)) => {
                                trace!(?node_id, "received request while being a leader");

                                match request.validate(&node_id, &self.unified_state) {
                                    // Valid requests to leader are certainly unexpected, so we just reject them.
                                    ValidationOutcome::Valid | ValidationOutcome::RejectionRequired => {
                                        yielder
                                            .yield_item(Ok(NodeOutput::ExternalEvent(
                                                OutgoingRaftNetworkEvent::Respond(
                                                    node_id,
                                                    request_id,
                                                    request.reject(&self.unified_state)
                                                ),
                                            )))
                                            .await;
                                    },
                                    ValidationOutcome::ConversionToFollowerRequired => {
                                        yielder.try_with_yield(self.unified_state.set_current_term(request.term(), None).await)
                                            .await;

                                        yielder
                                            .yield_item(Ok(
                                                NodeOutput::ConvertToFollower(
                                                    self.unified_state,
                                                    self.incoming_stream,
                                                    Some((node_id, request_id, request)),
                                                    active_membership_change.map(|(_, id, should_shutdown)| (id, should_shutdown)),
                                                ),
                                            ))
                                            .await;

                                        unreachable!()
                                    },
                                }
                            }
                            Some(IncomingRaftNetworkEvent::PushResponse(node_id, _, response)) => {
                                trace!(?node_id, "received response while being a leader");

                                match response.validate(&node_id, &self.unified_state) {
                                    ValidationOutcome::Valid => if let Response::AppendEntries(append_entries) = response {
                                        trace!(?node_id, %append_entries.success, "received append_entries response");

                                        let inflight_request = node_information
                                            .get_mut(&node_id)
                                            .and_then(|info| {
                                                info.inflight_request
                                                    .take()
                                                    .map(|new_next_index| (new_next_index, info))
                                            });

                                        if let Some((new_next_index, info)) = inflight_request {
                                            if append_entries.success {
                                                trace!(?node_id, ?new_next_index, "changing next_index using inflight_requests");
                                                info.next_index = new_next_index;
                                                info.match_index = NonZeroU64::new(new_next_index.get().saturating_sub(1));

                                                let commit_index = node_information.select_nth_unstable_by(
                                                    node_information.len() / 2,
                                                    |(_, a), (_, b)| a.match_index.cmp(&b.match_index)
                                                ).and_then(|info| info.match_index);

                                                if let Some(commit_index) = commit_index
                                                    && self.unified_state.last_committed_entry_id().map(|known_id| commit_index > known_id).unwrap_or(true)
                                                {
                                                    yielder.try_with_yield(
                                                        self.unified_state
                                                            .apply_log_entries(commit_index)
                                                            .await
                                                    ).await;

                                                    if let Some((request_id, _, should_shutdown)) = active_membership_change.take_if(|(_, change_id, _)| commit_index >= *change_id) {
                                                        // Client notifications about membership changes are best-effort.
                                                        if let Some(request_id) = request_id {
                                                            yielder
                                                                .yield_item(Ok(NodeOutput::ExternalEvent(
                                                                    OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                                                                        request_id,
                                                                        MembershipChangeOutcome::Success
                                                                    ),
                                                                )))
                                                                .await;
                                                        }

                                                        if should_shutdown {
                                                            return;
                                                        }
                                                    }
                                                }
                                            } else if let Some(info) = node_information.get_mut(&node_id) {
                                                trace!(?node_id, new_index = %(info.next_index.get() - 1), "decrementing next_index");

                                                info.next_index = NonZeroU64::new(info.next_index.get() - 1)
                                                    .unwrap_or(NonZeroU64::MIN);
                                            }
                                        }
                                    },
                                    ValidationOutcome::ConversionToFollowerRequired => {
                                        yielder.try_with_yield(self.unified_state.set_current_term(response.term(), None).await)
                                            .await;

                                        yielder
                                            .yield_item(Ok(
                                                NodeOutput::ConvertToFollower(
                                                    self.unified_state,
                                                    self.incoming_stream,
                                                    None,
                                                    active_membership_change.map(|(_, id, should_shutdown)| (id, should_shutdown)),
                                                ),
                                            ))
                                            .await;

                                        unreachable!()
                                    },
                                    // Responses don't require rejection, so we just ignore invalid ones.
                                    ValidationOutcome::RejectionRequired => {}
                                }
                            }
                            #[cfg(feature = "diagnostics")]
                            Some(IncomingRaftNetworkEvent::AskForDiagnostics) => {
                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::Diagnostics(Diagnostics {
                                            node_type: DiagnosticsNodeType::Leader,
                                            known_nodes: self.unified_state.connected_nodes().cloned().collect(),
                                            current_term: self.unified_state.current_term(),
                                            voted_for: self.unified_state.voted_for().cloned(),
                                            last_log_metadata: self.unified_state.last_log_metadata(),
                                            log_len: self.unified_state.log_entries(NonZeroU64::MIN).await.unwrap().len(),
                                            commit_index: self.unified_state.last_committed_entry_id()
                                        }),
                                    )))
                                    .await
                            },
                            None => return,
                        }
                    }
                }
            }
        }
    }
}

#[inline]
pub(crate) fn leader<'a, NodeId, RequestId, PS, S>(
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    active_membership_change: Option<(NonZeroU64, bool)>,
) -> LeaderNode<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    enstream(LeaderStreamState {
        unified_state,
        incoming_stream,
        active_membership_change,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        messages::{
            AppendEntriesRequest, AppendEntriesResponse, Request, RequestVoteRequest,
            RequestVoteResponse, Response,
        },
        seq_test,
        state::{LogEntry, LogEntryContents, LogEntryMetadata},
        test_utils::{id, DebugStream, ShimmedState},
        AppendEntryOutcome, IncomingRaftNetworkEvent, MembershipChangeOutcome, NodeOutput,
        OutgoingRaftNetworkEvent,
    };

    #[tokio::test(start_paused = true)]
    async fn empty_heartbeats() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            // Send initial empty heartbeat.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            // Respond with success to leader, resulting in "second" node removal from inflight_requests.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            // Poll before the heartbeat timer timeouts.
            recv (None) from first;

            skip 300;

            // Send one more heartbeat, but without a response this time.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            skip 300;

            // No more heartbeats were sent by a leader, since we didn't respond to the previous one.
            recv (None) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_with_payloads() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            // Send initial empty heartbeat.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            // Respond with success to leader, resulting in "second" node removal from inflight_requests.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            // Append new entry onto the leader log.
            send (IncomingRaftNetworkEvent::AppendEntry(2, 123)) to first_sender;

            // Poll to ensure, that AppendEntry and PushResponse are resolved before the heartbeat timer timeouts.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success,
                ),
            ))) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            // Send heartbeat with the new entry inside.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![LogEntry {
                            contents: LogEntryContents::GenericEntry(123),
                            metadata: LogEntryMetadata {
                                id: id(1),
                                term: 1
                            }
                        }],
                        commit_index: None
                    }),
                ),
            ))) from first;

            // Append one more entry onto the leader log.
            send (IncomingRaftNetworkEvent::AppendEntry(2, 456)) to first_sender;

            // Respond with success to leader.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            // Poll twice to ensure, that both AppendEntry and PushResponse are resolved before the heartbeat timer timeouts.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success,
                ),
            ))) from first;
            recv (None) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            // New entry is expected to be present here.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: Some(LogEntryMetadata {
                            id: id(1),
                            term: 1
                        }),
                        entries: vec![LogEntry {
                            contents: LogEntryContents::GenericEntry(456),
                            metadata: LogEntryMetadata { id: id(2), term: 1 }
                        }],
                        commit_index: Some(id(1))
                    }),
                ),
            ))) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn min_log_entry_id() {
        seq_test! {
            node_configuration main_config, ["first", "second", "third"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        })
                    ),
                ),
            ]) from first;

            // Respond with success to leader.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            // Append one entry onto the leader log.
            send (IncomingRaftNetworkEvent::AppendEntry(2, 123)) to first_sender;

            // Append one entry onto the leader log.
            send (IncomingRaftNetworkEvent::AppendEntry(2, 456)) to first_sender;

            // Poll leader to ensure that AppendEntry requests are resolved.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success
                ),
            ))) from first;
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success
                ),
            ))) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![LogEntry {
                                contents: LogEntryContents::GenericEntry(123),
                                metadata: LogEntryMetadata { id: id(1), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(456),
                                metadata: LogEntryMetadata { id: id(2), term: 1 }
                            }],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![LogEntry {
                                contents: LogEntryContents::GenericEntry(123),
                                metadata: LogEntryMetadata { id: id(1), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(456),
                                metadata: LogEntryMetadata { id: id(2), term: 1 }
                            }],
                            commit_index: None
                        })
                    ),
                ),
            ]) from first;

            // Second node returns success and third node returns failure to test min_log_entry_id correctness on the next heartbeat timer tick.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: false
            }))) to first_sender;

            // Append one entry onto the leader log.
            send (IncomingRaftNetworkEvent::AppendEntry(2, 789)) to first_sender;

            // Poll to resolve the responses above and the AppendEntry request.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success
                ),
            ))) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 1 }),
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(789),
                                    metadata: LogEntryMetadata { id: id(3), term: 1 }
                                }
                            ],
                            commit_index: Some(id(2))
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![LogEntry {
                                contents: LogEntryContents::GenericEntry(123),
                                metadata: LogEntryMetadata { id: id(1), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(456),
                                metadata: LogEntryMetadata { id: id(2), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(789),
                                metadata: LogEntryMetadata { id: id(3), term: 1 }
                            }],
                            commit_index: Some(id(2))
                        }),
                    ),
                ),
            ]) from first;

            // Same as before, second node accepts the request, third node rejects it.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: false
            }))) to first_sender;

            // This time, poll without sending AppendEntry before.
            recv (None) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            // Check that even for up-to-date nodes we still send heartbeats.
            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: Some(LogEntryMetadata { id: id(3), term: 1 }),
                            entries: vec![],
                            commit_index: Some(id(3))
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![LogEntry {
                                contents: LogEntryContents::GenericEntry(123),
                                metadata: LogEntryMetadata { id: id(1), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(456),
                                metadata: LogEntryMetadata { id: id(2), term: 1 }
                            }, LogEntry {
                                contents: LogEntryContents::GenericEntry(789),
                                metadata: LogEntryMetadata { id: id(3), term: 1 }
                            }],
                            commit_index: Some(id(3))
                        }),
                    ),
                ),
            ]) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn commit_index() {
        seq_test! {
            node_configuration main_config, ["first", "second", "third", "fourth"];

            node first, first_sender, main_config;
            node second, second_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            // Required to correctly exchange term information
            send (IncomingRaftNetworkEvent::PushRequest("first", 1, Request::RequestVote(RequestVoteRequest {
                term: 1,
                last_log_entry: None
            }))) to second_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "first",
                    1,
                    Response::RequestVote(RequestVoteResponse {
                        term: 1,
                        vote_granted: true
                    }),
                ),
            ))) from second;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            // Since vote grant from this node is not required, we can reply false just to check
            // if the vote system works correctly.
            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: false
            }))) to first_sender;

            convert first to leader;

            // Send initial empty heartbeat.
            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        })
                    ),
                ),
            ]) from first;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::AppendEntry(2, 123)) to first_sender;
            send (IncomingRaftNetworkEvent::AppendEntry(2, 456)) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success
                ),
            ))) from first;
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::Success
                ),
            ))) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
            ]) from first;

            // Relay the AppendEntries RPC onto the second node.
            send (IncomingRaftNetworkEvent::PushRequest("first", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: None,
                entries: vec![
                    LogEntry {
                        contents: LogEntryContents::GenericEntry(123),
                        metadata: LogEntryMetadata {
                            id: id(1),
                            term: 1
                        }
                    },
                    LogEntry {
                        contents: LogEntryContents::GenericEntry(456),
                        metadata: LogEntryMetadata {
                            id: id(2),
                            term: 1
                        }
                    },
                ],
                commit_index: None
            }))) to second_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "first",
                    1,
                    Response::AppendEntries(AppendEntriesResponse {
                        term: 1,
                        success: true
                    }),
                ),
            ))) from second;

            // Only the second node sends successful response.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: false
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: false
            }))) to first_sender;

            recv (None) from first;

            // Skip some time for heartbeat timer to timeout.
            skip 300;

            // Entries should be uncommitted, since only one node accepted new entries.
            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 1 }),
                            entries: vec![],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        })
                    ),
                ),
            ]) from first;

            // Simulate that the first node has been disconnected, passing leadership onto the second node.
            recv (None) from second;

            skip 10005;

            convert second to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::RequestVote(RequestVoteResponse {
                term: 2,
                vote_granted: true
            }))) to second_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::RequestVote(RequestVoteResponse {
                term: 2,
                vote_granted: true
            }))) to second_sender;

            convert second to leader;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "first",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(123),
                                    metadata: LogEntryMetadata {
                                        id: id(1),
                                        term: 1
                                    }
                                },
                                LogEntry {
                                    contents: LogEntryContents::GenericEntry(456),
                                    metadata: LogEntryMetadata {
                                        id: id(2),
                                        term: 1
                                    }
                                },
                            ],
                            commit_index: None
                        })
                    ),
                ),
            ]) from second;

            // The first node is still disconnected, so we provide responses only for the third and the fourth node.
            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 2,
                success: true
            }))) to second_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 2,
                success: true
            }))) to second_sender;

            // Poll to ensure that the responses above are handled.
            recv (None) from second;

            skip 305;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 1 }),
                            entries: vec![],
                            commit_index: Some(id(2))
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 1 }),
                            entries: vec![],
                            commit_index: Some(id(2))
                        })
                    ),
                ),
            ]) from second;
        };
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn add_node() {
        seq_test! {
            node_configuration first_config, ["first"];

            node first, first_sender, first_config;

            // New node configurations are expected to have only one entry, which is their own node identifier.
            node_configuration second_config, ["second"];

            node second, second_sender, second_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            // We are in a single-node cluster.
            convert first to leader;

            // Ensure that the initial poll returns nothing, since there are no
            // nodes to send anything to. Also, initialize AppendEntries timer.
            recv (None) from first;

            send (IncomingRaftNetworkEvent::AddNode(1, "second")) to first_sender;

            // Handle the AddNode request above.
            recv (None) from first;

            skip 305;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![
                            LogEntry {
                                metadata: LogEntryMetadata { id: id(1), term: 1 },
                                // "contents" value may be reordered depending on the implementation details, so at any moment
                                // this test may become flaky.
                                contents: LogEntryContents::ChangeMembership(vec!["first", "second"])
                            }
                        ],
                        commit_index: None
                    }),
                ),
            ))) from first;

            // Redirect the request above to the second node.
            send (IncomingRaftNetworkEvent::PushRequest("first", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: None,
                entries: vec![
                    LogEntry {
                        metadata: LogEntryMetadata { id: id(1), term: 1 },
                        contents: LogEntryContents::ChangeMembership(vec!["first", "second"])
                    }
                ],
                commit_index: None
            }))) to second_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "first",
                    1,
                    Response::AppendEntries(AppendEntriesResponse {
                        term: 1,
                        success: true
                    }),
                ),
            ))) from second;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    1,
                    MembershipChangeOutcome::Success
                ),
            ))) from first;

            skip 305;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: Some(LogEntryMetadata { id: id(1), term: 1 }),
                        entries: vec![],
                        commit_index: Some(id(1))
                    }),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushRequest("first", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: Some(LogEntryMetadata { id: id(1), term: 1 }),
                entries: vec![],
                commit_index: Some(id(1))
            }))) to second_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "first",
                    1,
                    Response::AppendEntries(AppendEntriesResponse {
                        term: 1,
                        success: true
                    }),
                ),
            ))) from second;

            check diagnostics (crate::Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["first"],
                current_term: 1,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(1), term: 1 }),
                log_len: 1,
                commit_index: Some(id(1))
            }) using (second, second_sender);
        }
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn remove_node() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::RemoveNode(1, "second")) to first_sender;

            // Handle the RemoveNode request above.
            // Membership change is applied already since there is only one node left in a cluster.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    1,
                    MembershipChangeOutcome::Success
                ),
            ))) from first;

            skip 305;

            // There are no nodes left in cluster, so no heartbeats are expected.
            recv (None) from first;

            check diagnostics (crate::Diagnostics {
                node_type: crate::DiagnosticsNodeType::Leader,
                known_nodes: vec![],
                current_term: 1,
                voted_for: Some("first"),
                last_log_metadata: Some(LogEntryMetadata { id: id(1), term: 1 }),
                log_len: 1,
                commit_index: Some(id(1))
            }) using (first, first_sender);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reject_membership_change_when_unapplied() {
        seq_test! {
            node_configuration first_config, ["first", "second"];

            node first, first_sender, first_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::AddNode(1, "third")) to first_sender;

            // Handle the AddNode request above.
            recv (None) from first;

            skip 305;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    metadata: LogEntryMetadata { id: id(1), term: 1 },
                                    // "contents" value may be reordered depending on the implementation details, so at any moment
                                    // this test may become flaky.
                                    contents: LogEntryContents::ChangeMembership(vec!["first", "second", "third"])
                                }
                            ],
                            commit_index: None
                        })
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    metadata: LogEntryMetadata { id: id(1), term: 1 },
                                    // "contents" value may be reordered depending on the implementation details, so at any moment
                                    // this test may become flaky.
                                    contents: LogEntryContents::ChangeMembership(vec!["first", "second", "third"])
                                }
                            ],
                            commit_index: None
                        }),
                    ),
                ),
            ]) from first;

            send (IncomingRaftNetworkEvent::AddNode(2, "fourth")) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    2,
                    MembershipChangeOutcome::Reject("fourth"),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::RemoveNode(2, "second")) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    2,
                    MembershipChangeOutcome::Reject("second"),
                ),
            ))) from first;
        }
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn single_node_commit_index() {
        seq_test! {
            node_configuration main_config, ["first"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            convert first to leader;

            send (IncomingRaftNetworkEvent::AppendEntry(1, 123)) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    1,
                    AppendEntryOutcome::Success
                ),
            ))) from first;

            check diagnostics (crate::Diagnostics {
                node_type: crate::DiagnosticsNodeType::Leader,
                known_nodes: vec![],
                current_term: 1,
                voted_for: Some("first"),
                last_log_metadata: Some(LogEntryMetadata { id: id(1), term: 1 }),
                log_len: 1,
                commit_index: Some(id(1))
            }) using (first, first_sender);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_on_remove_node() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    }),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::RemoveNode(1, "first")) to first_sender;

            recv (None) from first;

            assert that first termination is false;

            skip 305;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![
                            LogEntry {
                                metadata: LogEntryMetadata { id: id(1), term: 1 },
                                contents: LogEntryContents::ChangeMembership(vec!["second"]),
                            }
                        ],
                        commit_index: None
                    }),
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushResponse(
                "second",
                1,
                Response::AppendEntries(AppendEntriesResponse {
                    term: 1,
                    success: true
                }),
            )) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    1,
                    MembershipChangeOutcome::Success,
                ),
            ))) from first;

            // Poll once more to complete termination process, since the node
            // yielded it's execution to send the MembershipChangeOutcome above.
            recv (None) from first;

            assert that first termination is true;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn leader_outage_in_membership_change() {
        seq_test! {
            node_configuration main_config, ["first", "second", "third", "fourth", "fifth"];

            node first, first_sender, main_config;
            node second, second_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fifth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![],
                            commit_index: None
                        }),
                    ),
                ),
            ]) from first;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fifth", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true
            }))) to first_sender;

            send (IncomingRaftNetworkEvent::AddNode(1, "sixth")) to first_sender;

            recv (None) from first;

            skip 305;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "second",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fifth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "sixth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 1,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
            ]) from first;

            send (IncomingRaftNetworkEvent::PushRequest(
                "first",
                1,
                Request::AppendEntries(AppendEntriesRequest {
                    term: 1,
                    prev_log_entry: None,
                    entries: vec![
                        LogEntry {
                            contents: LogEntryContents::ChangeMembership(vec![
                                "first",
                                "second",
                                "third",
                                "fourth",
                                "fifth",
                                "sixth"
                            ]),
                            metadata: LogEntryMetadata { id: id(1), term: 1 }
                        },
                    ],
                    commit_index: None
                }),
            )) to second_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "first",
                    1,
                    Response::AppendEntries(AppendEntriesResponse {
                        term: 1,
                        success: true,
                    }),
                ),
            ))) from second;

            skip 10005;

            convert second to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::RequestVote(RequestVoteResponse {
                term: 2,
                vote_granted: true
            }))) to second_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fourth", 1, Response::RequestVote(RequestVoteResponse {
                term: 2,
                vote_granted: true
            }))) to second_sender;

            send (IncomingRaftNetworkEvent::PushResponse("fifth", 1, Response::RequestVote(RequestVoteResponse {
                term: 2,
                vote_granted: true
            }))) to second_sender;

            convert second to leader;

            multirecv (&[
                NodeOutput::<_, usize, ShimmedState, DebugStream>::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "first",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "third",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fourth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "fifth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
                NodeOutput::ExternalEvent(
                    OutgoingRaftNetworkEvent::Send(
                        "sixth",
                        Request::AppendEntries(AppendEntriesRequest {
                            term: 2,
                            prev_log_entry: None,
                            entries: vec![
                                LogEntry {
                                    contents: LogEntryContents::ChangeMembership(vec![
                                        "first",
                                        "second",
                                        "third",
                                        "fourth",
                                        "fifth",
                                        "sixth"
                                    ]),
                                    metadata: LogEntryMetadata { id: id(1), term: 1 }
                                },
                            ],
                            commit_index: None
                        }),
                    ),
                ),
            ]) from second;

            send (IncomingRaftNetworkEvent::AddNode(1, "seventh")) to second_sender;

            // It was previously possible to change cluster membership configuration
            // after leader transition while the previous membership config change was not committed.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                    1,
                    MembershipChangeOutcome::Reject("seventh")
                ),
            ))) from second;
        }
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn reject_out_of_cluster_request_vote() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            skip 10005;

            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            convert first to leader;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Send(
                    "second",
                    Request::AppendEntries(AppendEntriesRequest {
                        term: 1,
                        prev_log_entry: None,
                        entries: vec![],
                        commit_index: None
                    })
                ),
            ))) from first;

            send (IncomingRaftNetworkEvent::PushResponse("third", 1, Response::AppendEntries(AppendEntriesResponse {
                term: 1,
                success: true,
            }))) to first_sender;

            // "third" node sends a request with term higher than the one inside "first".
            // However, "first" should still reject this request.
            send (IncomingRaftNetworkEvent::PushRequest("third", 1, Request::RequestVote(RequestVoteRequest {
                term: 2,
                last_log_entry: None,
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "third",
                    1,
                    Response::RequestVote(RequestVoteResponse {
                        term: 1,
                        vote_granted: false
                    })
                ),
            ))) from first;

            check diagnostics (crate::Diagnostics {
                node_type: crate::DiagnosticsNodeType::Leader,
                known_nodes: vec!["second"],
                current_term: 1,
                voted_for: Some("first"),
                last_log_metadata: None,
                log_len: 0,
                commit_index: None
            }) using (first, first_sender);
        }
    }
}
