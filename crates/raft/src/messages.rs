use core::{cmp::Ordering, fmt::Debug, hash::Hash, num::NonZeroU64};

use alloc::vec::Vec;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::state::{LogEntry, LogEntryMetadata, PersistentState, UnifiedState};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ValidationOutcome {
    Valid,
    RejectionRequired,
    ConversionToFollowerRequired,
}

/// An RPC to request a vote from a node in a cluster.
///
/// Derived from <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf#section.3.2>.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub last_log_entry: Option<LogEntryMetadata>,
}

/// A response to [`RequestVoteRequest`] RPC.
///
/// Derived from <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf#section.3.2>.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// An RPC to append new log entries or ping a node in a cluster.
///
/// Derived from <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf#section.3.2>.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest<NodeId, Contents> {
    pub term: u64,
    pub prev_log_entry: Option<LogEntryMetadata>,
    pub entries: Vec<LogEntry<NodeId, Contents>>,
    pub commit_index: Option<NonZeroU64>,
}

/// A response to [`AppendEntriesRequest`] RPC.
///
/// Derived from <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf#section.3.2>.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

pub(crate) trait RpcEntity {
    fn term(&self) -> u64;
}

/// Generalized Raft RPC.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Request<NodeId, Contents> {
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest<NodeId, Contents>),
}

impl<NodeId, Contents> Request<NodeId, Contents> {
    #[inline]
    pub(crate) fn reject<PS>(&self, unified_state: &UnifiedState<'_, NodeId, PS>) -> Response {
        match self {
            Request::RequestVote(_) => Response::RequestVote(RequestVoteResponse {
                term: unified_state.current_term(),
                vote_granted: false,
            }),
            Request::AppendEntries(_) => Response::AppendEntries(AppendEntriesResponse {
                term: unified_state.current_term(),
                success: false,
            }),
        }
    }
}

impl<NodeId: Clone + Eq + Hash + Debug, Contents> Request<NodeId, Contents> {
    #[inline]
    pub(crate) fn validate<PS: PersistentState<NodeId>>(
        &self,
        node_id: &NodeId,
        unified_state: &UnifiedState<'_, NodeId, PS>,
    ) -> ValidationOutcome {
        // https://groups.google.com/g/raft-dev/c/JEtBYaPpHXo/m/JbHpK5NBBAAJ
        if matches!(self, Request::RequestVote(_))
            && !unified_state.connected_nodes().contains(node_id)
        {
            return ValidationOutcome::RejectionRequired;
        }

        validate_term(self, node_id, unified_state)
    }
}

impl<NodeId, Contents> RpcEntity for Request<NodeId, Contents> {
    #[inline]
    fn term(&self) -> u64 {
        match self {
            Request::RequestVote(request_vote) => request_vote.term,
            Request::AppendEntries(append_entries) => append_entries.term,
        }
    }
}

/// Generalized response to a Raft RPC.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Response {
    RequestVote(RequestVoteResponse),
    AppendEntries(AppendEntriesResponse),
}

impl Response {
    #[inline]
    pub(crate) fn validate<NodeId: Debug, PS>(
        &self,
        node_id: &NodeId,
        unified_state: &UnifiedState<'_, NodeId, PS>,
    ) -> ValidationOutcome {
        validate_term(self, node_id, unified_state)
    }
}

impl RpcEntity for Response {
    #[inline]
    fn term(&self) -> u64 {
        match self {
            Response::RequestVote(request_vote) => request_vote.term,
            Response::AppendEntries(append_entries) => append_entries.term,
        }
    }
}

#[inline(always)]
fn validate_term<E: RpcEntity, NodeId: Debug, PS>(
    entity: &E,
    node_id: &NodeId,
    unified_state: &UnifiedState<'_, NodeId, PS>,
) -> ValidationOutcome {
    match entity.term().cmp(&unified_state.current_term()) {
        Ordering::Less => {
            trace!(
                ?node_id,
                entity_term = %entity.term(),
                our_term = %unified_state.current_term(),
                "rejecting entity with term less than ours"
            );

            ValidationOutcome::RejectionRequired
        }
        Ordering::Equal => ValidationOutcome::Valid,
        Ordering::Greater => {
            trace!(
                ?node_id,
                entity_term = %entity.term(),
                our_term = %unified_state.current_term(),
                "falling back to being a follower"
            );

            ValidationOutcome::ConversionToFollowerRequired
        }
    }
}
