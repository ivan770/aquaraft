mod traversal;

use core::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    num::NonZeroU64,
    pin::{pin, Pin},
    time::Duration,
};

use enstream::{enstream, HandlerFn, HandlerFnLifetime, Yielder};
use futures_util::{
    select_biased,
    stream::{self, FusedStream, StreamExt},
    FutureExt,
};

use crate::{
    messages::{
        AppendEntriesResponse, Request, RequestVoteResponse, Response, RpcEntity, ValidationOutcome,
    },
    node::TIMER_RANGE,
    option_ext::OptionExt,
    state::{LogEntryContents, PersistentState, UnifiedState},
    yielder_ext::TryYielderExt,
    AppendEntryOutcome, IncomingRaftNetworkEvent, MembershipChangeOutcome, NodeOutput,
    OutgoingRaftNetworkEvent,
};

#[cfg(feature = "diagnostics")]
use crate::{Diagnostics, DiagnosticsNodeType};

pub(crate) type FollowerNode<'a, NodeId, RequestId, PS, S>
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

struct FollowerStreamState<'a, NodeId, RequestId, PS, S>
where
    PS: PersistentState<NodeId>,
{
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    transition_request: Option<(NodeId, RequestId, Request<NodeId, PS::Contents>)>,
    active_membership_change: Option<(NonZeroU64, bool)>,
}

type FollowerStreamStateFut<'yielder, 'a: 'yielder, NodeId, RequestId, PS, S>
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
    for FollowerStreamState<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    type Fut = FollowerStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S>;
}

impl<'a, NodeId, RequestId, PS, S>
    HandlerFn<Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>
    for FollowerStreamState<'a, NodeId, RequestId, PS, S>
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
    ) -> FollowerStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S> {
        async move {
            let timer_duration = self.unified_state.random(TIMER_RANGE);

            let mut candidate_timer = pin!(self
                .unified_state
                .sleep(Duration::from_millis(timer_duration)));

            let mut known_leader = None;

            let mut event_stream = stream::iter(self.transition_request.map(
                |(node_id, request_id, request)| {
                    IncomingRaftNetworkEvent::PushRequest(node_id, request_id, request)
                },
            ))
            .chain(&mut self.incoming_stream);

            loop {
                select_biased! {
                    _ = (&mut candidate_timer).fuse() => {
                        yielder
                            .yield_item(Ok(
                                NodeOutput::ConvertToCandidate(
                                    self.unified_state,
                                    self.incoming_stream,
                                    self.active_membership_change
                                ),
                            ))
                            .await;

                        unreachable!();
                    },
                    event = event_stream.next().fuse() => {
                        match event {
                            Some(IncomingRaftNetworkEvent::AddNode(request_id, node_id) | IncomingRaftNetworkEvent::RemoveNode(request_id, node_id)) => {
                                trace!(?known_leader, "received AddNode or RemoveNode while being a follower, attempting to redirect it");

                                let outcome = match known_leader.as_ref().cloned() {
                                    Some(known_leader) => MembershipChangeOutcome::RedirectRequired {
                                        to: known_leader,
                                        changed_node: node_id
                                    },
                                    None => MembershipChangeOutcome::Reject(node_id)
                                };

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                                            request_id, outcome
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::AppendEntry(request_id, contents)) => {
                                trace!(?known_leader, "received AppendEntry while being a follower, attempting to redirect it");

                                let outcome = match known_leader.as_ref().cloned() {
                                    Some(known_leader) => AppendEntryOutcome::RedirectRequired(known_leader, contents),
                                    None => AppendEntryOutcome::Reject(contents)
                                };

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::AppendEntryOutcome(
                                            request_id, outcome
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::PushRequest(node_id, request_id, request)) => {
                                trace!(?node_id, "received request while being a follower");

                                // Validation scheme is a little bit different in a follower than in any other node.
                                // When we receive a ConversionToFollowerRequired outcome, we still need
                                // to handle the incoming request.
                                match request.validate(&node_id, &self.unified_state) {
                                    ValidationOutcome::Valid => {}
                                    ValidationOutcome::ConversionToFollowerRequired => {
                                        yielder.try_with_yield(self.unified_state.set_current_term(request.term(), None).await)
                                            .await;
                                    }
                                    ValidationOutcome::RejectionRequired => {
                                        yielder
                                            .yield_item(Ok(NodeOutput::ExternalEvent(
                                                OutgoingRaftNetworkEvent::Respond(
                                                    node_id,
                                                    request_id,
                                                    request.reject(&self.unified_state)
                                                ),
                                            )))
                                            .await;

                                        continue;
                                    }
                                }

                                match request {
                                    Request::RequestVote(request_vote) => {
                                        trace!(?node_id, "a vote was requested while being a follower");

                                        let last_log_metadata = self.unified_state.last_log_metadata();

                                        let up_to_date = match (last_log_metadata, request_vote.last_log_entry) {
                                            (Some(metadata), Some(request_metadata)) => request_metadata.up_to_date(&metadata),
                                            (Some(_), None) => false,
                                            (None, _) => true,
                                        };

                                        trace!(?node_id, %up_to_date, "candidate log status");

                                        let vote_granted = if up_to_date {
                                            let voted_for = yielder.try_with_yield(
                                                self.unified_state
                                                    .vote_for(node_id.clone())
                                                    .await
                                            ).await;

                                            if voted_for {
                                                trace!(?node_id, "vote granted, resetting a candidate timer");

                                                let timer_duration = self.unified_state.random(TIMER_RANGE);

                                                candidate_timer.set(self.unified_state.sleep(Duration::from_millis(
                                                    timer_duration,
                                                )));

                                                true
                                            } else {
                                                trace!(?node_id, "we already voted in this term, vote not granted");

                                                false
                                            }
                                        } else {
                                            trace!(?node_id, "since log is not up to date, vote not granted");

                                            false
                                        };

                                        yielder
                                            .yield_item(Ok(NodeOutput::ExternalEvent(
                                                OutgoingRaftNetworkEvent::Respond(
                                                    node_id,
                                                    request_id,
                                                    Response::RequestVote(RequestVoteResponse {
                                                        term: self.unified_state.current_term(),
                                                        vote_granted,
                                                    }),
                                                ),
                                            )))
                                            .await;
                                    },
                                    Request::AppendEntries(append_entries) => {
                                        let entries_len = append_entries.entries.len() as u64;

                                        trace!(
                                            ?node_id,
                                            %entries_len,
                                            "a request to append entries was received while being a follower"
                                        );

                                        known_leader = Some(node_id.clone());

                                        let timer_duration = self.unified_state.random(TIMER_RANGE);

                                        candidate_timer.set(self.unified_state.sleep(Duration::from_millis(
                                            timer_duration,
                                        )));

                                        let log_entries = yielder.try_with_yield(
                                            self.unified_state.log_entries(
                                                append_entries.prev_log_entry
                                                    .map(|metadata| metadata.id)
                                                    .unwrap_or(NonZeroU64::MIN)
                                            ).await
                                        ).await;

                                        macro_rules! handle_append_entries {
                                            ($append_entries:expr, $our_entries:expr) => {{
                                                let suitable_action = traversal::find_suitable_action(
                                                    $append_entries.entries.iter().map(|entry| entry.metadata),
                                                    $our_entries.iter().map(|entry| entry.metadata)
                                                );

                                                match suitable_action {
                                                    Some(traversal::SuitableAction::Push(idx)) => {
                                                        for entry in $append_entries.entries.into_iter().skip(idx) {
                                                            if let LogEntryContents::ChangeMembership(node_configuration) = &entry.contents {
                                                                trace!(?node_configuration, "applying ChangeMembership log entry");

                                                                let should_shutdown = yielder.try_with_yield(
                                                                    self.unified_state
                                                                        .apply_node_configuration_change(node_configuration.clone())
                                                                        .await
                                                                ).await;

                                                                self.active_membership_change = Some((entry.metadata.id, should_shutdown));
                                                            }

                                                            yielder.try_with_yield(
                                                                self.unified_state
                                                                    .append_log_entry(entry)
                                                                    .await
                                                            ).await;
                                                        }
                                                    },
                                                    Some(traversal::SuitableAction::Replace(from, idx)) => {
                                                        // Remove active membership change information since new
                                                        // log entries might not contain the same membership change information.
                                                        self.active_membership_change
                                                            .take_if(|(id, _)| *id >= from);

                                                        yielder.try_with_yield(
                                                            self.unified_state
                                                                .remove_log_entries(from)
                                                                .await
                                                        ).await;

                                                        for entry in $append_entries.entries.into_iter().skip(idx) {
                                                            if let LogEntryContents::ChangeMembership(node_configuration) = &entry.contents {
                                                                trace!(?node_configuration, "applying ChangeMembership log entry");

                                                                let should_shutdown = yielder.try_with_yield(
                                                                    self.unified_state
                                                                        .apply_node_configuration_change(node_configuration.clone())
                                                                        .await
                                                                ).await;

                                                                self.active_membership_change = Some((entry.metadata.id, should_shutdown));
                                                            }

                                                            yielder.try_with_yield(
                                                                self.unified_state
                                                                    .append_log_entry(entry)
                                                                    .await
                                                            ).await;
                                                        }
                                                    },
                                                    Some(traversal::SuitableAction::DoNothing) | None => {}
                                                }

                                                let commit_index = append_entries.prev_log_entry
                                                    .map(|metadata| metadata.id.saturating_add(entries_len))
                                                    .or_else(|| NonZeroU64::new(entries_len))
                                                    .and_then(|max_possible_commit_index| Some(append_entries.commit_index?.min(max_possible_commit_index)));

                                                if let Some(commit_index) = commit_index
                                                    && self.unified_state.last_committed_entry_id().map(|known_id| commit_index > known_id).unwrap_or(true)
                                                {
                                                    yielder.try_with_yield(
                                                        self.unified_state
                                                            .apply_log_entries(commit_index)
                                                            .await
                                                    ).await;

                                                    let should_shutdown = self.active_membership_change
                                                        .take_if(|(id, _)| commit_index >= *id)
                                                        .filter(|(_, should_shutdown)| *should_shutdown)
                                                        .is_some();

                                                    if should_shutdown {
                                                        return;
                                                    }
                                                }
                                            }}
                                        }

                                        let success = match (append_entries.prev_log_entry, &*log_entries) {
                                            (Some(prev_log_entry), [first, entries @ ..]) if prev_log_entry == first.metadata => {
                                                handle_append_entries!(append_entries, entries);
                                                true
                                            },
                                            (None, entries) => {
                                                handle_append_entries!(append_entries, entries);
                                                true
                                            },
                                            _ => false
                                        };

                                        yielder
                                            .yield_item(Ok(NodeOutput::ExternalEvent(
                                                OutgoingRaftNetworkEvent::Respond(
                                                    node_id,
                                                    request_id,
                                                    Response::AppendEntries(AppendEntriesResponse {
                                                        term: self.unified_state.current_term(),
                                                        success
                                                    }),
                                                ),
                                            )))
                                            .await;
                                    }
                                }
                            }
                            Some(IncomingRaftNetworkEvent::PushResponse(_, _, _)) => {}
                            #[cfg(feature = "diagnostics")]
                            Some(IncomingRaftNetworkEvent::AskForDiagnostics) => {
                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::Diagnostics(Diagnostics {
                                            node_type: DiagnosticsNodeType::Follower,
                                            known_nodes: self.unified_state.connected_nodes().cloned().collect(),
                                            current_term: self.unified_state.current_term(),
                                            voted_for: self.unified_state.voted_for().cloned(),
                                            last_log_metadata: self.unified_state.last_log_metadata(),
                                            log_len: self.unified_state.log_entries(NonZeroU64::MIN).await.unwrap().len(),
                                            commit_index: self.unified_state.last_committed_entry_id()
                                        }),
                                    )))
                                    .await
                            }
                            None => return,
                        }
                    }
                };
            }
        }
    }
}

#[inline]
pub(crate) fn follower<'a, NodeId, RequestId, PS, S>(
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    transition_request: Option<(NodeId, RequestId, Request<NodeId, PS::Contents>)>,
    active_membership_change: Option<(NonZeroU64, bool)>,
) -> FollowerNode<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    enstream(FollowerStreamState {
        unified_state,
        incoming_stream,
        transition_request,
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
        test_utils::id,
        AppendEntryOutcome, IncomingRaftNetworkEvent, NodeOutput, OutgoingRaftNetworkEvent,
    };

    #[cfg(feature = "diagnostics")]
    use crate::Diagnostics;

    #[tokio::test(start_paused = true)]
    async fn candidate_conversion() {
        seq_test! {
            node_configuration main_config, ["first"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn grant_vote() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::RequestVote(RequestVoteRequest {
                term: 0,
                last_log_entry: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::RequestVote(RequestVoteResponse {
                    term: 0,
                    vote_granted: true
                }),
            )))) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn outdated_log_vote() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }],
                commit_index: None
            }))) to first_sender;

            // Poll node to ensure that AppendEntries RPC is resolved.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::RequestVote(RequestVoteRequest {
                term: 1,
                last_log_entry: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::RequestVote(RequestVoteResponse {
                    term: 1,
                    vote_granted: false
                }),
            )))) from first;
        };
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn accept_new_entries() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // At this moment, the first node is completely empty, thus it should accept our entries.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }, LogEntry {
                    contents: LogEntryContents::GenericEntry(456),
                    metadata: LogEntryMetadata {
                        id: id(2),
                        term: 0
                    }
                }],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(2), term: 0 }),
                log_len: 2,
                commit_index: None
            }) using (first, first_sender);

            // Send even more entries to the first node, while also commiting the first two entries.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(789),
                    metadata: LogEntryMetadata {
                        id: id(3),
                        term: 0
                    }
                }, LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(4),
                        term: 0
                    }
                }],
                commit_index: Some(id(2))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(4), term: 0 }),
                log_len: 4,
                commit_index: Some(id(2))
            }) using (first, first_sender);

            // Try to send entries that the first node already has. This should result in
            // successful response, however, node state should not change in any way.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: Some(LogEntryMetadata { id: id(1), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(789),
                    metadata: LogEntryMetadata {
                        id: id(2),
                        term: 0
                    }
                }],
                commit_index: Some(id(2))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(4), term: 0 }),
                log_len: 4,
                commit_index: Some(id(2))
            }) using (first, first_sender);

            // Now, let's try to simulate a network partitioning with term increment.
            // This should replace the log entry with id = 4 and term = 0 with the entry that has id = 4 and term = 1
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: Some(LogEntryMetadata { id: id(3), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(4),
                        term: 1
                    }
                }],
                commit_index: Some(id(2))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 1,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 1,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(4), term: 1 }),
                log_len: 4,
                commit_index: Some(id(2))
            }) using (first, first_sender);

            // Attempt to send out of order AppendEntries RPC.
            // This should result in an unsuccessful response and next_index decrement on the leader part.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: Some(LogEntryMetadata { id: id(5), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(6),
                        term: 1
                    }
                }],
                commit_index: Some(id(2))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 1,
                    success: false
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 1,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(4), term: 1 }),
                log_len: 4,
                commit_index: Some(id(2))
            }) using (first, first_sender);
        };
    }

    #[tokio::test(start_paused = true)]
    async fn reject_append_entry() {
        seq_test! {
            node_configuration main_config, ["first"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::AppendEntry(1, 123)) to first_sender;

            // In this situation, follower is expected to reject a request, since there is no known leader yet.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    1,
                    AppendEntryOutcome::Reject(123),
                ),
            ))) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn redirect_append_entry() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Follower now knows the leader node id.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            send (IncomingRaftNetworkEvent::AppendEntry(2, 123)) to first_sender;

            // AppendEntry redirect is expected here, since in the previous request we provided information about the current leader.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    2,
                    AppendEntryOutcome::RedirectRequired("second", 123),
                ),
            ))) from first;
        };
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn accept_membership_changes() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // At this moment, the first node is completely empty, thus it should accept our entries.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }, LogEntry {
                    contents: LogEntryContents::ChangeMembership(vec!["first", "second", "third"]),
                    metadata: LogEntryMetadata {
                        id: id(2),
                        term: 0
                    }
                }],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second", "third"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(2), term: 0 }),
                log_len: 2,
                commit_index: None
            }) using (first, first_sender);

            // Network partitioning simulation
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: Some(LogEntryMetadata { id: id(1), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::ChangeMembership(vec!["first", "second"]),
                    metadata: LogEntryMetadata {
                        id: id(2),
                        term: 1
                    }
                }],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 1,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 1,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(2), term: 1 }),
                log_len: 2,
                commit_index: None
            }) using (first, first_sender);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_on_membership_change() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::ChangeMembership(vec!["second"]),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "second",
                    1,
                    Response::AppendEntries(AppendEntriesResponse {
                        term: 0,
                        success: true
                    }),
                ),
            ))) from first;

            assert that first termination is false;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: Some(LogEntryMetadata { id: id(1), term: 0 }),
                entries: vec![],
                commit_index: Some(id(1))
            }))) to first_sender;

            recv (None) from first;

            assert that first termination is true;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn entry_replace_shutdown() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::ChangeMembership(vec!["first", "second"]),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::ChangeMembership(vec!["second"]),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 1
                    }
                }],
                commit_index: Some(id(1))
            }))) to first_sender;

            recv (None) from first;

            assert that first termination is true;
        }
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn reject_out_of_cluster_request_vote() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Provide information about a cluster leader.
            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![],
                commit_index: None
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            // "third" node sends a request with term higher than the one inside "first".
            // However, "first" should still reject this request.
            send (IncomingRaftNetworkEvent::PushRequest("third", 1, Request::RequestVote(RequestVoteRequest {
                term: 1,
                last_log_entry: None,
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond(
                    "third",
                    1,
                    Response::RequestVote(RequestVoteResponse {
                        term: 0,
                        vote_granted: false
                    })
                ),
            ))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: None,
                log_len: 0,
                commit_index: None
            }) using (first, first_sender);
        }
    }

    #[cfg(feature = "diagnostics")]
    #[tokio::test(start_paused = true)]
    async fn max_possible_commit_index() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: None,
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(1),
                        term: 0
                    }
                }, LogEntry {
                    contents: LogEntryContents::GenericEntry(456),
                    metadata: LogEntryMetadata {
                        id: id(2),
                        term: 0
                    }
                }],
                // commit_index is higher than the entire log of a follower,
                // thus when applying entries a follower should consider using only existing entries
                commit_index: Some(id(5))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(2), term: 0 }),
                log_len: 2,
                commit_index: Some(id(2))
            }) using (first, first_sender);

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 0,
                prev_log_entry: Some(LogEntryMetadata { id: id(2), term: 0 }),
                entries: vec![LogEntry {
                    contents: LogEntryContents::GenericEntry(123),
                    metadata: LogEntryMetadata {
                        id: id(3),
                        term: 0
                    }
                }],
                commit_index: Some(id(5))
            }))) to first_sender;

            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::Respond("second", 1, Response::AppendEntries(AppendEntriesResponse {
                    term: 0,
                    success: true
                }),
            )))) from first;

            check diagnostics (Diagnostics {
                node_type: crate::DiagnosticsNodeType::Follower,
                known_nodes: vec!["second"],
                current_term: 0,
                voted_for: None,
                last_log_metadata: Some(LogEntryMetadata { id: id(3), term: 0 }),
                log_len: 3,
                commit_index: Some(id(3))
            }) using (first, first_sender);
        };
    }
}
