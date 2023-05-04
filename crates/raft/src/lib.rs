#![feature(
    assert_matches,
    generic_associated_types,
    int_roundings,
    nonzero_min_max,
    nonzero_ops,
    pin_macro,
    type_alias_impl_trait
)]
#![cfg_attr(test, feature(drain_filter))]
#![cfg_attr(not(test), no_std)]

extern crate alloc;

#[macro_use]
extern crate tracing;

use core::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    num::NonZeroU64,
    pin::{pin, Pin},
};

#[cfg(feature = "diagnostics")]
use alloc::vec::Vec;

use enstream::{enstream, HandlerFn, HandlerFnLifetime, Yielder};
use futures_util::{stream::FusedStream, StreamExt};

use messages::{Request, RequestVoteRequest, Response};
use node::{candidate, follower, leader, CandidateNode, FollowerNode, LeaderNode};
use pin_project::pin_project;
use state::{PersistentState, UnifiedState};
use tracing::Instrument;
use yielder_ext::TryYielderExt;

#[cfg(feature = "diagnostics")]
use state::LogEntryMetadata;

/// Raft protocol RPCs.
pub mod messages;

/// Raft persistent state.
pub mod state;

mod node;
mod option_ext;
mod yielder_ext;

#[cfg(test)]
mod test_utils;

/// Description of a node type.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticsNodeType {
    /// Describes a follower node.
    Follower,

    /// Describes a candidate node.
    Candidate,

    /// Describes a leader node.
    Leader,
}

/// Raft node diagnostics.
///
/// Contains various information about log entry statistics, node type
/// and election information.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostics<NodeId> {
    pub node_type: DiagnosticsNodeType,
    pub known_nodes: Vec<NodeId>,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub last_log_metadata: Option<LogEntryMetadata>,
    pub log_len: usize,
    pub commit_index: Option<NonZeroU64>,
}

/// Outcome of [`IncomingRaftNetworkEvent::AddNode`] and [`IncomingRaftNetworkEvent::RemoveNode`].
///
/// [`MembershipChangeOutcome`] is provided on a best-effort basis,
/// thus there is no guarantee that you will receive it at any membership change request outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipChangeOutcome<NodeId> {
    /// Membership changes were applied successfully,
    /// the client is free to modify cluster membership again.
    Success,

    /// A request to change cluster membership was received by a
    /// follower node with a known leader. To change cluster membership, you have to redirect your request.
    RedirectRequired { to: NodeId, changed_node: NodeId },

    /// Membership changes were rejected because there are
    /// non-applied membership changes already or a request was received by a candidate node.
    Reject(NodeId),
}

/// Outcome of [`IncomingRaftNetworkEvent::AppendEntry`].
///
/// [`AppendEntryOutcome`] is provided at best-effort basis, thus
/// there is no guarantee that you will receive it at any append entry request outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendEntryOutcome<NodeId, Contents> {
    /// New entry was successfully written onto the log.
    Success,

    /// A request to append an entry was received by a follower node with a known leader.
    /// To push a new entry you have to redirect your request.
    RedirectRequired(NodeId, Contents),

    /// A request to append an entry was received by a follower or a candidate node
    /// with no known leader.
    Reject(Contents),
}

/// Incoming events that change Raft cluster state or pass requests and responses from other state machines.
#[derive(Clone, Debug)]
pub enum IncomingRaftNetworkEvent<NodeId, RequestId, Contents> {
    /// Add new known node.
    AddNode(RequestId, NodeId),

    /// Remove existing known node.
    RemoveNode(RequestId, NodeId),

    /// Append a new entry onto the cluster state.
    AppendEntry(RequestId, Contents),

    /// Push new request received from the other node.
    PushRequest(NodeId, RequestId, Request<NodeId, Contents>),

    /// Push new response received from the other node.
    PushResponse(NodeId, RequestId, Response),

    /// Ask for Raft node diagnostics.
    #[cfg(feature = "diagnostics")]
    AskForDiagnostics,
}

/// Outgoing events that communicate with other Raft cluster state machines or provide feedback to client requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutgoingRaftNetworkEvent<NodeId, RequestId, Contents> {
    /// Send a request to a specific node in a cluster.
    Send(NodeId, Request<NodeId, Contents>),

    /// Respond to a received request.
    Respond(NodeId, RequestId, Response),

    /// [`IncomingRaftNetworkEvent::AppendEntry`] outcome.
    AppendEntryOutcome(RequestId, AppendEntryOutcome<NodeId, Contents>),

    /// [`IncomingRaftNetworkEvent::AddNode`] and [`IncomingRaftNetworkEvent::RemoveNode`] outcome.
    MembershipChangeOutcome(RequestId, MembershipChangeOutcome<NodeId>),

    /// Diagnostics provided from the [`IncomingRaftNetworkEvent::AskForDiagnostics`] call.
    #[cfg(feature = "diagnostics")]
    Diagnostics(Diagnostics<NodeId>),
}

#[derive(Debug)]
pub(crate) enum NodeOutput<'a, NodeId, RequestId, PS, S>
where
    PS: PersistentState<NodeId>,
{
    /// Convert the current node to a follower.
    ConvertToFollower(
        UnifiedState<'a, NodeId, PS>,
        Pin<&'a mut S>,
        Option<(NodeId, RequestId, Request<NodeId, PS::Contents>)>,
        Option<(NonZeroU64, bool)>,
    ),

    /// Convert the current node to a candidate.
    ConvertToCandidate(
        UnifiedState<'a, NodeId, PS>,
        Pin<&'a mut S>,
        Option<(NonZeroU64, bool)>,
    ),

    /// Convert the current node to a leader.
    ConvertToLeader(
        UnifiedState<'a, NodeId, PS>,
        Pin<&'a mut S>,
        Option<(NonZeroU64, bool)>,
    ),

    /// [`OutgoingRaftNetworkEvent`] that is going to be redirected back to the client.
    ExternalEvent(OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>),
}

#[cfg(test)]
impl<'a, NodeId, RequestId, PS, S> PartialEq for NodeOutput<'a, NodeId, RequestId, PS, S>
where
    NodeId: PartialEq,
    RequestId: PartialEq,
    PS: PersistentState<NodeId>,
    <PS as PersistentState<NodeId>>::Contents: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ExternalEvent(l0), Self::ExternalEvent(r0)) => l0 == r0,
            _ => false,
        }
    }
}

#[pin_project(project = RaftNodeProj)]
enum RaftNode<'a, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'a,
    RequestId: Send + Sync + 'a,
    PS: PersistentState<NodeId> + Send + Sync + 'a,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
        + Send
        + Sync
        + 'a,
{
    Follower(#[pin] FollowerNode<'a, NodeId, RequestId, PS, S>),
    Candidate(#[pin] CandidateNode<'a, NodeId, RequestId, PS, S>),
    Leader(#[pin] LeaderNode<'a, NodeId, RequestId, PS, S>),
}

/// Conversion of a [`IncomingRaftNetworkEvent`] stream into a Raft state machine.
pub trait RaftStreamExt<NodeId, RequestId, PS: PersistentState<NodeId>>:
    FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>>
{
    type IntoRaftEvents: FusedStream<
        Item = Result<OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>, PS::Error>,
    >;

    /// Convert a [`FusedStream`] of [`IncomingRaftNetworkEvent`] into a Raft state machine.
    ///
    /// # Params
    ///
    /// `node_id` specified unique node identifier for a current node.
    ///
    /// `persistent_state` specifies a uniquely allocated storage for Raft state machine.
    fn into_raft_events(self, node_id: NodeId, persistent_state: PS) -> Self::IntoRaftEvents;
}

impl<NodeId, RequestId, PS, S> RaftStreamExt<NodeId, RequestId, PS> for S
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug,
    RequestId: Send + Sync,
    PS: PersistentState<NodeId> + Send + Sync,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>> + Send + Sync,
{
    type IntoRaftEvents = impl FusedStream<
            Item = Result<OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>, PS::Error>,
        > + Send
        + Sync;

    fn into_raft_events(self, node_id: NodeId, persistent_state: PS) -> Self::IntoRaftEvents {
        enstream(RaftEventsState {
            node_id,
            persistent_state,
            incoming_stream: self,
        })
    }
}

struct RaftEventsState<NodeId, PS, S> {
    node_id: NodeId,
    persistent_state: PS,
    incoming_stream: S,
}

type RaftEventsStateFut<'yielder, NodeId, RequestId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug + 'yielder,
    RequestId: Send + Sync + 'yielder,
    PS: PersistentState<NodeId> + Send + Sync,
    PS::Contents: Send + Sync + 'yielder,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>> + Send + Sync,
= impl Future<Output = ()> + Send + Sync;

impl<'yielder, NodeId, RequestId, PS, S>
    HandlerFnLifetime<
        'yielder,
        Result<OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>, PS::Error>,
    > for RaftEventsState<NodeId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug,
    RequestId: Send + Sync,
    PS: PersistentState<NodeId> + Send + Sync,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>> + Send + Sync,
{
    type Fut = RaftEventsStateFut<'yielder, NodeId, RequestId, PS, S>;
}

impl<NodeId, RequestId, PS, S>
    HandlerFn<Result<OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>, PS::Error>>
    for RaftEventsState<NodeId, PS, S>
where
    NodeId: Clone + Eq + Hash + Send + Sync + Debug,
    RequestId: Send + Sync,
    PS: PersistentState<NodeId> + Send + Sync,
    S: FusedStream<Item = IncomingRaftNetworkEvent<NodeId, RequestId, PS::Contents>> + Send + Sync,
{
    fn call(
        self,
        mut yielder: Yielder<
            '_,
            Result<OutgoingRaftNetworkEvent<NodeId, RequestId, PS::Contents>, PS::Error>,
        >,
    ) -> RaftEventsStateFut<'_, NodeId, RequestId, PS, S> {
        let span = tracing::span!(tracing::Level::TRACE, "node", my_node_id = ?self.node_id);

        async move {
            let persistent_state = pin!(self.persistent_state);
            let incoming_stream = pin!(self.incoming_stream);

            let mut node = pin!(RaftNode::Follower(follower(
                yielder
                    .try_with_yield(UnifiedState::new(self.node_id, persistent_state).await)
                    .await,
                incoming_stream,
                None,
                None
            )));

            loop {
                let node_output = match node.as_mut().project() {
                    RaftNodeProj::Follower(mut follower) => {
                        yielder
                            .try_with_yield(follower.next().await.transpose())
                            .await
                    }
                    RaftNodeProj::Candidate(mut candidate) => {
                        yielder
                            .try_with_yield(candidate.next().await.transpose())
                            .await
                    }
                    RaftNodeProj::Leader(mut leader) => {
                        yielder
                            .try_with_yield(leader.next().await.transpose())
                            .await
                    }
                };

                match node_output {
                    Some(NodeOutput::ConvertToCandidate(
                        mut unified_state,
                        incoming_stream,
                        active_membership_change,
                    )) => {
                        let current_term = unified_state.current_term();

                        yielder
                            .try_with_yield(
                                unified_state
                                    .set_current_term(
                                        current_term + 1,
                                        Some(unified_state.my_node_id().clone()),
                                    )
                                    .await,
                            )
                            .await;

                        let last_log_entry = unified_state.last_log_metadata();

                        for connected_node in unified_state.connected_nodes() {
                            yielder
                                .yield_item(Ok(OutgoingRaftNetworkEvent::Send(
                                    connected_node.clone(),
                                    Request::RequestVote(RequestVoteRequest {
                                        term: current_term + 1,
                                        last_log_entry,
                                    }),
                                )))
                                .await;
                        }

                        node.set(RaftNode::Candidate(candidate(
                            unified_state,
                            incoming_stream,
                            active_membership_change,
                        )));
                    }
                    Some(NodeOutput::ConvertToFollower(
                        unified_state,
                        incoming_stream,
                        transition_request,
                        active_membership_change,
                    )) => node.set(RaftNode::Follower(follower(
                        unified_state,
                        incoming_stream,
                        transition_request,
                        active_membership_change,
                    ))),
                    Some(NodeOutput::ConvertToLeader(
                        unified_state,
                        incoming_stream,
                        active_membership_change,
                    )) => {
                        node.set(RaftNode::Leader(leader(
                            unified_state,
                            incoming_stream,
                            active_membership_change,
                        )));
                    }
                    Some(NodeOutput::ExternalEvent(event)) => yielder.yield_item(Ok(event)).await,
                    None => return,
                };
            }
        }
        .instrument(span)
    }
}
