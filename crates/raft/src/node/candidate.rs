use core::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    num::NonZeroU64,
    pin::{pin, Pin},
    time::Duration,
};

use aquaraft_vecmap::VecSet;
use enstream::{enstream, HandlerFn, HandlerFnLifetime, Yielder};
use futures_util::{
    select_biased,
    stream::{FusedStream, StreamExt},
    FutureExt,
};

use crate::{
    messages::{Request, Response, RpcEntity, ValidationOutcome},
    node::TIMER_RANGE,
    state::{PersistentState, UnifiedState},
    yielder_ext::TryYielderExt,
    AppendEntryOutcome, IncomingRaftNetworkEvent, MembershipChangeOutcome, NodeOutput,
    OutgoingRaftNetworkEvent,
};

#[cfg(feature = "diagnostics")]
use crate::{Diagnostics, DiagnosticsNodeType};

pub(crate) type CandidateNode<'a, NodeId, RequestId, PS, S>
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
    + Sync
    + 'a;

struct CandidateStreamState<'a, NodeId, PS, S> {
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    active_membership_change: Option<(NonZeroU64, bool)>,
}

type CandidateStreamStateFut<'yielder, 'a: 'yielder, NodeId, RequestId, PS, S>
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
    for CandidateStreamState<'a, NodeId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    type Fut = CandidateStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S>;
}

impl<'a, NodeId, RequestId, PS, S>
    HandlerFn<Result<NodeOutput<'a, NodeId, RequestId, PS, S>, PS::Error>>
    for CandidateStreamState<'a, NodeId, PS, S>
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
    ) -> CandidateStreamStateFut<'yielder, 'a, NodeId, RequestId, PS, S> {
        async move {
            let timer_duration = self.unified_state.random(TIMER_RANGE);

            let mut restart_timer = pin!(self
                .unified_state
                .sleep(Duration::from_millis(timer_duration)));

            // There exists no meaningful way to change membership configuration from a
            // candidate node, so we can just assume that the cluster node count stays
            // constant at least for the duration of being a candidate.
            let connected_nodes_len = self.unified_state.connected_nodes().count();

            if connected_nodes_len == 0 {
                trace!(
                    "promoting outselves to being a leader since we are in a single-node cluster"
                );

                yielder
                    .yield_item(Ok(NodeOutput::ConvertToLeader(
                        self.unified_state,
                        self.incoming_stream,
                        self.active_membership_change,
                    )))
                    .await;

                unreachable!()
            }

            let mut voted_nodes = VecSet::with_capacity(connected_nodes_len);

            loop {
                select_biased! {
                    _ = (&mut restart_timer).fuse() => {
                        yielder.yield_item(Ok(NodeOutput::ConvertToCandidate(self.unified_state, self.incoming_stream, self.active_membership_change)))
                            .await;

                        unreachable!()
                    },
                    event = self.incoming_stream.next().fuse() => {
                        match event {
                            Some(IncomingRaftNetworkEvent::AddNode(request_id, node_id) | IncomingRaftNetworkEvent::RemoveNode(request_id, node_id)) => {
                                trace!("received AddNode or RemoveNode while being a candidate, rejecting it");

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::MembershipChangeOutcome(
                                            request_id,
                                            MembershipChangeOutcome::Reject(node_id),
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::AppendEntry(request_id, contents)) => {
                                trace!("received AppendEntry while being a candidate, rejecting it");

                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::AppendEntryOutcome(
                                            request_id,
                                            AppendEntryOutcome::Reject(contents)
                                        ),
                                    )))
                                    .await;
                            },
                            Some(IncomingRaftNetworkEvent::PushRequest(node_id, request_id, request)) => {
                                trace!(?node_id, "received request while being a candidate");

                                match request.validate(&node_id, &self.unified_state) {
                                    ValidationOutcome::Valid => match request {
                                        Request::RequestVote(_) => {
                                            trace!(?node_id, "a vote was requested while being a candidate, rejecting it");

                                            // Since we are a candidate node, we should always reject the request,
                                            // because `voted_for` is guaranteed to always be equal to the id of a current node.
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
                                        request @ Request::AppendEntries(_) => {
                                            trace!(?node_id, "received AppendEntries while being a candidate, falling back to being a follower");

                                            yielder
                                                .yield_item(Ok(NodeOutput::ConvertToFollower(
                                                    self.unified_state,
                                                    self.incoming_stream,
                                                    Some((node_id, request_id, request)),
                                                    self.active_membership_change
                                                )))
                                                .await;

                                            unreachable!()
                                        }
                                    },
                                    ValidationOutcome::ConversionToFollowerRequired => {
                                        yielder.try_with_yield(self.unified_state.set_current_term(request.term(), None).await)
                                            .await;

                                        yielder
                                            .yield_item(Ok(NodeOutput::ConvertToFollower(
                                                self.unified_state,
                                                self.incoming_stream,
                                                Some((node_id, request_id, request)),
                                                self.active_membership_change
                                            )))
                                            .await;

                                        unreachable!()
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
                                    }
                                }
                            },
                            Some(IncomingRaftNetworkEvent::PushResponse(node_id, _, response)) => {
                                trace!(?node_id, ?response, "received response while being a candidate");

                                match response.validate(&node_id, &self.unified_state) {
                                    ValidationOutcome::Valid => match response {
                                        Response::RequestVote(request_vote) if request_vote.vote_granted => {
                                            trace!(
                                                ?node_id,
                                                total_nodes = %connected_nodes_len,
                                                total_votes = %voted_nodes.len() + 1,
                                                "a vote was granted in our favor"
                                            );

                                            if voted_nodes.insert(node_id)
                                                && voted_nodes.len() >= connected_nodes_len.div_ceil(2)
                                            {
                                                trace!(
                                                    total_nodes = %connected_nodes_len,
                                                    acquired_votes = %voted_nodes.len(),
                                                    "promoting outselves to being a leader"
                                                );

                                                yielder
                                                    .yield_item(Ok(
                                                        NodeOutput::ConvertToLeader(
                                                            self.unified_state,
                                                            self.incoming_stream,
                                                            self.active_membership_change
                                                        ),
                                                    ))
                                                    .await;

                                                unreachable!()
                                            }
                                        },
                                        _ => {}
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
                                                    self.active_membership_change
                                                ),
                                            ))
                                            .await;

                                        unreachable!()
                                    }
                                    // Responses don't require rejection, so we just ignore invalid ones.
                                    ValidationOutcome::RejectionRequired => {}
                                }
                            }
                            #[cfg(feature = "diagnostics")]
                            Some(IncomingRaftNetworkEvent::AskForDiagnostics) => {
                                yielder
                                    .yield_item(Ok(NodeOutput::ExternalEvent(
                                        OutgoingRaftNetworkEvent::Diagnostics(Diagnostics {
                                            node_type: DiagnosticsNodeType::Candidate,
                                            known_nodes: self.unified_state.connected_nodes().cloned().collect(),
                                            current_term: self.unified_state.current_term(),
                                            voted_for: self.unified_state.voted_for().cloned(),
                                            last_log_metadata: self.unified_state.last_log_metadata(),
                                            log_len: self.unified_state.log_entries(NonZeroU64::MIN).await.unwrap().len(),
                                            commit_index: self.unified_state.last_committed_entry_id()
                                        }),
                                    )))
                                    .await;
                            }
                            None => return
                        }
                    }
                }
            }
        }
    }
}

#[inline]
pub(crate) fn candidate<'a, NodeId, RequestId, PS, S>(
    unified_state: UnifiedState<'a, NodeId, PS>,
    incoming_stream: Pin<&'a mut S>,
    active_membership_change: Option<(NonZeroU64, bool)>,
) -> CandidateNode<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    enstream(CandidateStreamState {
        unified_state,
        incoming_stream,
        active_membership_change,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        messages::{AppendEntriesRequest, Request, RequestVoteResponse, Response},
        seq_test, AppendEntryOutcome, IncomingRaftNetworkEvent, NodeOutput,
        OutgoingRaftNetworkEvent,
    };

    #[tokio::test(start_paused = true)]
    async fn single_node_leader() {
        seq_test! {
            node_configuration main_config, ["first"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            // Instantly convert to a leader since we are in a single-node cluster.
            convert first to leader;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn start_elections() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            // Assert that we are still inside of a candidate state.
            recv (None) from first;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn win_elections() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            // RaftNode sends RequestVoteRequests in general, however, since we are simulating its behaviour
            // we can just send mocked response immediately.
            send (IncomingRaftNetworkEvent::PushResponse("second", 1, Response::RequestVote(RequestVoteResponse {
                term: 1,
                vote_granted: true
            }))) to first_sender;

            // We have enough votes to become a leader.
            convert first to leader;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn step_down_on_append_entries() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            send (IncomingRaftNetworkEvent::PushRequest("second", 1, Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                prev_log_entry: None,
                entries: vec![],
                commit_index: None
            }))) to first_sender;

            convert first to follower;
        };
    }

    #[tokio::test(start_paused = true)]
    async fn reject_append_entry() {
        seq_test! {
            node_configuration main_config, ["first", "second"];

            node first, first_sender, main_config;

            // Send initial poll to initialize candidate timer.
            recv (None) from first;

            // Skip some time for candidate timer to timeout.
            skip 10005;

            // Convert to a candidate.
            convert first to candidate;

            send (IncomingRaftNetworkEvent::AppendEntry(1, 123)) to first_sender;

            // Candidate is expected to always reject any AppendEntry request, since no known leader
            // is available yet.
            recv (Some(NodeOutput::ExternalEvent(
                OutgoingRaftNetworkEvent::AppendEntryOutcome(
                    1,
                    AppendEntryOutcome::Reject(123)
                ),
            ))) from first;
        };
    }
}
