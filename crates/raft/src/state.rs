use core::{fmt::Debug, hash::Hash, num::NonZeroU64, ops::Range, pin::Pin, time::Duration};

use alloc::vec::Vec;
use futures_util::{Future, Stream};
use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Log entry description.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry<NodeId, Contents> {
    pub metadata: LogEntryMetadata,
    pub contents: LogEntryContents<NodeId, Contents>,
}

/// Metadata that describes a log entry.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntryMetadata {
    /// Log entry unique, monotonically incrementing identifier.
    pub id: NonZeroU64,

    /// Election term that was active during log entry creation process.
    pub term: u64,
}

/// Contents of a log entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogEntryContents<NodeId, Contents> {
    /// Log entry with contents provided by a client.
    GenericEntry(Contents),

    /// Log entry with Raft cluster membership config.
    ChangeMembership(Vec<NodeId>),
}

impl LogEntryMetadata {
    /// Check if the `self` [`LogEntryMetadata`] is up-to-date compared to `other`.
    ///
    /// Algorithm is defined in section 5.4.1 of Raft specification.
    ///
    /// Returns `true` if `self` is up-to-date, `false` otherwise.
    #[inline]
    pub(crate) fn up_to_date(&self, other: &LogEntryMetadata) -> bool {
        if self.term == other.term {
            self.id >= other.id
        } else {
            self.term >= other.term
        }
    }
}

/// Raft node initial state.
///
/// Used for state machine initialization purposes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialState<NodeId> {
    /// Last encountered election term.
    ///
    /// Defaults to `0` when a node is being initialized for the first time.
    pub current_term: u64,

    /// Last known to be committed log entry identifier.
    ///
    /// Defaults to [`None`] when a node is being initialized for the first time
    /// or there are no log entries known to be committed.
    pub last_committed_entry_id: Option<NonZeroU64>,

    /// Last known log entry metadata.
    ///
    /// Defaults to [`None`] when a node is being initialized for the first time
    /// or there are no log entries in log.
    pub last_log_entry: Option<LogEntryMetadata>,

    /// Latest node membership configuration.
    pub node_configuration: Vec<NodeId>,

    /// Identifier of a node that received vote in the current election term.
    ///
    /// Defaults to [`None`] if no vote was given or no elections were held previously.
    pub voted_for: Option<NodeId>,
}

mod private {
    pub trait Sealed {}
    pub struct Bounds<T>(T);
    impl<T> Sealed for Bounds<T> {}
}
use private::{Bounds, Sealed};

/// [`PersistentState`] associated types tied to `'this` lifetime.
///
/// For [`PersistentState`] impl to be valid, this has to be implemented for any `'this` lifetime, similar to how GATs work.
pub trait PersistentStateLifetime<
    'this,
    NodeId,
    Contents,
    Error,
    ImplicitBounds: Sealed = Bounds<&'this Self>,
>
{
    /// Random number generator implementation.
    type Rng: Rng;

    /// Future returned by [`PersistentState::initial_state`].
    type InitialStateFuture: Future<Output = Result<InitialState<NodeId>, Error>> + Send + Sync;

    /// Future returned by [`PersistentState::set_current_term`].
    type SetCurrentTermFuture: Future<Output = Result<(), Error>> + Send + Sync;

    /// Future returned by [`PersistentState::set_voted_for`].
    type SetVotedForFuture: Future<Output = Result<(), Error>> + Send + Sync;

    /// Future returned by [`PersistentState::log_entries`].
    type LogEntriesFuture: Future<Output = Result<Vec<LogEntry<NodeId, Contents>>, Error>>
        + Send
        + Sync;

    /// Future returned by [`PersistentState::append_log_entry`].
    type AppendLogEntryFuture: Future<Output = Result<(), Error>> + Send + Sync;

    /// Future returned by [`PersistentState::remove_log_entries`].
    type RemoveLogEntriesFuture: Future<Output = Result<(), Error>> + Send + Sync;

    /// Future returned by [`PersistentState::apply_log_entries`].
    type ApplyLogEntriesFuture: Future<Output = Result<(), Error>> + Send + Sync;
}

/// Raft persistent state.
pub trait PersistentState<NodeId>:
    for<'this> PersistentStateLifetime<'this, NodeId, Self::Contents, Self::Error> + Send + Sync
{
    type Contents: Serialize + DeserializeOwned + Clone + Send + Sync;

    type Error: Debug + Send + Sync + 'static;

    /// Future returned by [`PersistentState::sleep`].
    type SleepFuture: Future<Output = Result<(), Self::Error>> + Send + Sync;

    /// Future returned by [`PersistentState::interval`].
    type IntervalFuture: Future<Output = Result<Self::IntervalStream, Self::Error>> + Send + Sync;

    /// A [`Stream`] of time intervals provided by [`PersistentState::IntervalFuture`].
    type IntervalStream: Stream<Item = Result<(), Self::Error>> + Send + Sync;

    /// Get initial persistent state.
    ///
    /// This method provides Raft state machine with initialization information.
    fn initial_state(
        self: Pin<&mut Self>
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::InitialStateFuture;

    /// Set current term.
    fn set_current_term(self: Pin<&mut Self>, current_term: u64, voted_for: Option<NodeId>) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::SetCurrentTermFuture;

    /// Set the `NodeId` that received a vote in the current voting term.
    fn set_voted_for(
        self: Pin<&mut Self>,
        voted_for: NodeId,
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::SetVotedForFuture;

    /// Get all log entries whose `id >= start`.
    ///
    /// Should return empty [`Vec`] if there are no suitable log entries.
    fn log_entries(
        self: Pin<&mut Self>,
        start: NonZeroU64,
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::LogEntriesFuture;

    /// Append a new log entry onto the log.
    ///
    /// While not required, it's recommended to do `log_entry` sanity checks, verifying that the
    /// log entry that preceding log entry has `id` equal to `log_entry.metadata.id - 1`.
    fn append_log_entry(
        self: Pin<&mut Self>,
        log_entry: LogEntry<NodeId, Self::Contents>,
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::AppendLogEntryFuture;

    /// Remove all log entries whose `id >= start`.
    fn remove_log_entries(
        self: Pin<&mut Self>,
        start: NonZeroU64
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::RemoveLogEntriesFuture;

    /// Apply all log entries whose `id <= end`.
    fn apply_log_entries(
        self: Pin<&mut Self>,
        end: NonZeroU64
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::ApplyLogEntriesFuture;

    /// Asynchronous sleep.
    ///
    /// This method should return a [`PersistentState::SleepFuture`], that resolves as soon as the provided [`Duration`] elapses.
    fn sleep(self: Pin<&mut Self>, duration: Duration) -> Self::SleepFuture;

    /// Asynchronous time intervals.
    ///
    /// This method should return a [`PersistentState::IntervalFuture`], that resolves to
    /// [`PersistentState::IntervalStream`] which, in turn, resolves as soon as a
    /// time interval elapses.
    fn interval(self: Pin<&mut Self>, duration: Duration) -> Self::IntervalFuture;

    /// Get mutable reference to [`Rng`] implementation.
    fn rng(
        self: Pin<&mut Self>,
    ) -> <Self as PersistentStateLifetime<'_, NodeId, Self::Contents, Self::Error>>::Rng;
}

#[derive(Debug)]
pub(crate) struct UnifiedState<'a, NodeId, PS> {
    current_term: u64,
    last_committed_entry_id: Option<NonZeroU64>,
    last_log_entry: Option<LogEntryMetadata>,
    node_configuration: Vec<NodeId>,
    my_node_id: NodeId,
    persistent_state: Pin<&'a mut PS>,
    voted_for: Option<NodeId>,
}

impl<'a, NodeId, PS> UnifiedState<'a, NodeId, PS> {
    #[inline]
    pub(crate) fn my_node_id(&self) -> &NodeId {
        &self.my_node_id
    }

    #[inline]
    pub(crate) fn current_term(&self) -> u64 {
        self.current_term
    }

    #[inline]
    pub(crate) fn last_committed_entry_id(&self) -> Option<NonZeroU64> {
        self.last_committed_entry_id
    }

    #[inline]
    pub(crate) fn last_log_metadata(&self) -> Option<LogEntryMetadata> {
        self.last_log_entry
    }

    #[cfg(feature = "diagnostics")]
    #[inline]
    pub(crate) fn voted_for(&self) -> Option<&NodeId> {
        self.voted_for.as_ref()
    }

    #[inline]
    pub(crate) fn raw_node_configuration(&self) -> &[NodeId] {
        &self.node_configuration
    }
}

impl<'a, NodeId, PS> UnifiedState<'a, NodeId, PS>
where
    NodeId: Clone + Eq + Hash,
    PS: PersistentState<NodeId>,
{
    #[inline]
    pub(crate) async fn new(
        my_node_id: NodeId,
        mut persistent_state: Pin<&'a mut PS>,
    ) -> Result<UnifiedState<'a, NodeId, PS>, PS::Error> {
        let InitialState {
            current_term,
            last_committed_entry_id,
            last_log_entry,
            node_configuration,
            voted_for,
        } = persistent_state.as_mut().initial_state().await?;

        Ok(Self {
            current_term,
            last_committed_entry_id,
            last_log_entry,
            node_configuration,
            my_node_id,
            persistent_state,
            voted_for,
        })
    }

    #[inline]
    pub(crate) async fn set_current_term(
        &mut self,
        current_term: u64,
        voted_for: Option<NodeId>,
    ) -> Result<(), PS::Error> {
        self.persistent_state
            .as_mut()
            .set_current_term(current_term, voted_for.clone())
            .await?;

        self.current_term = current_term;
        self.voted_for = voted_for;

        Ok(())
    }

    #[inline]
    pub(crate) async fn vote_for(&mut self, node_id: NodeId) -> Result<bool, PS::Error> {
        match &self.voted_for {
            Some(voted_for_id) if *voted_for_id == node_id => Ok(true),
            None => {
                self.persistent_state
                    .as_mut()
                    .set_voted_for(node_id.clone())
                    .await?;

                self.voted_for = Some(node_id);

                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    #[inline]
    pub(crate) fn connected_nodes(&self) -> impl Iterator<Item = &NodeId> + '_ {
        self.node_configuration
            .iter()
            .filter(|node_id| **node_id != self.my_node_id)
    }

    #[inline]
    pub(crate) async fn apply_node_configuration_change(
        &mut self,
        node_configuration: Vec<NodeId>,
    ) -> Result<bool, PS::Error> {
        self.node_configuration = node_configuration;

        Ok(!self.node_configuration.contains(&self.my_node_id))
    }

    #[inline]
    pub(crate) fn add_connected_node(&mut self, node_id: NodeId) {
        self.node_configuration.push(node_id);
    }

    #[inline]
    pub(crate) fn remove_connected_node(&mut self, node_id: &NodeId) {
        let position = self.node_configuration.iter().position(|id| id == node_id);

        if let Some(position) = position {
            // We don't care about the node configuration order, so it's fine
            // to use swap_remove here
            self.node_configuration.swap_remove(position);
        }
    }

    #[inline]
    pub(crate) async fn log_entries(
        &mut self,
        start: NonZeroU64,
    ) -> Result<Vec<LogEntry<NodeId, PS::Contents>>, PS::Error> {
        self.persistent_state.as_mut().log_entries(start).await
    }

    #[inline]
    pub(crate) async fn append_log_entry(
        &mut self,
        log_entry: LogEntry<NodeId, PS::Contents>,
    ) -> Result<(), PS::Error> {
        let log_entry_metadata = log_entry.metadata;

        self.persistent_state
            .as_mut()
            .append_log_entry(log_entry)
            .await?;

        self.last_log_entry = Some(log_entry_metadata);

        Ok(())
    }

    #[inline]
    pub(crate) async fn remove_log_entries(&mut self, start: NonZeroU64) -> Result<(), PS::Error> {
        self.persistent_state
            .as_mut()
            .remove_log_entries(start)
            .await
    }

    #[inline]
    pub(crate) async fn apply_log_entries(&mut self, end: NonZeroU64) -> Result<(), PS::Error> {
        self.persistent_state
            .as_mut()
            .apply_log_entries(end)
            .await?;

        self.last_committed_entry_id = Some(end);

        Ok(())
    }

    #[inline]
    pub(crate) fn sleep(
        &mut self,
        duration: Duration,
    ) -> impl Future<Output = Result<(), PS::Error>> {
        self.persistent_state.as_mut().sleep(duration)
    }

    #[inline]
    pub(crate) async fn interval(
        &mut self,
        duration: Duration,
    ) -> Result<impl Stream<Item = Result<(), PS::Error>>, PS::Error> {
        self.persistent_state.as_mut().interval(duration).await
    }

    #[inline]
    pub(crate) fn random(&mut self, range: Range<u64>) -> u64 {
        self.persistent_state.as_mut().rng().gen_range(range)
    }
}
