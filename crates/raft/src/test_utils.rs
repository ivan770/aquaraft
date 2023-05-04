use std::{
    convert::Infallible,
    fmt::{self, Debug, Formatter},
    future::{self, Future},
    num::NonZeroU64,
    pin::{pin, Pin},
    task::{Context, Poll},
    time::Duration,
};

use flume::r#async::RecvStream;
use futures_util::{
    stream::{self, FusedStream},
    FutureExt, Stream,
};
use pin_project::pin_project;
use rand::{prelude::ThreadRng, thread_rng};
use tokio::time::{interval, sleep};

use crate::{
    node::{CandidateNode, FollowerNode, LeaderNode},
    state::{InitialState, LogEntry, PersistentState, PersistentStateLifetime},
    IncomingRaftNetworkEvent,
};

type NodeId = &'static str;
type RequestId = usize;
type Contents = usize;

pub(crate) type DebugStream =
    EmptyDebug<RecvStream<'static, IncomingRaftNetworkEvent<NodeId, RequestId, Contents>>>;

#[pin_project]
pub(crate) struct EmptyDebug<T>(#[pin] pub T);

impl<T: Stream> Stream for EmptyDebug<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }
}

impl<T: FusedStream> FusedStream for EmptyDebug<T> {
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

impl<T> Debug for EmptyDebug<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EmptyDebug").finish()
    }
}

#[pin_project(project = TestNodeProj)]
pub(crate) enum TestNode<'a> {
    Follower(#[pin] FollowerNode<'a, NodeId, RequestId, ShimmedState, DebugStream>),
    Candidate(#[pin] CandidateNode<'a, NodeId, RequestId, ShimmedState, DebugStream>),
    Leader(#[pin] LeaderNode<'a, NodeId, RequestId, ShimmedState, DebugStream>),
}

#[macro_export]
macro_rules! seq_test {
    () => {};

    (node_configuration $name:ident, $nodes:expr; $($tail:tt)*) => {
        let $name = $nodes;
        seq_test!($($tail)*);
    };

    (node $node:ident, $node_sender:ident, $node_configuration:ident; $($tail:tt)*) => {
        let persistent_state = crate::test_utils::ShimmedState::new($node_configuration.to_vec());
        ::futures_util::pin_mut!(persistent_state);

        seq_test!(@node $node, $node_sender, persistent_state; $($tail)*);
    };

    (send ($event:expr) to $node_sender:ident; $($tail:tt)*) => {
        $node_sender.send($event).unwrap();
        seq_test!($($tail)*);
    };

    (recv ($event:expr) from $node:ident; $($tail:tt)*) => {
        assert_eq!(seq_test!(@recv from $node), $event);
        seq_test!($($tail)*);
    };

    (multirecv ($events:expr) from $node:ident; $($tail:tt)*) => {
        let mut received_events = (0..$events.len())
            .map(|_| {
                seq_test!(@recv from $node)
                    .unwrap_or_else(|| panic!("no more events were produced in multirecv from {}", stringify!($node)))
            })
            .collect::<Vec<_>>();

        for event in $events {
            received_events.drain_filter(|el| el == event)
                .next()
                .expect("no event matching filter was found");
        }

        seq_test!($($tail)*);
    };

    (check diagnostics ($diagnostics:expr) using ($node:ident, $node_sender:ident); $($tail:tt)*) => {
        $node_sender.send(crate::IncomingRaftNetworkEvent::AskForDiagnostics).unwrap();
        seq_test!(recv (Some(crate::NodeOutput::ExternalEvent(crate::OutgoingRaftNetworkEvent::Diagnostics($diagnostics)))) from $node; $($tail)*);
    };

    (convert $node:ident to follower; $($tail:tt)*) => {
        match seq_test!(@recv from $node) {
            Some(crate::NodeOutput::ConvertToFollower(unified_state, incoming_stream, transition_request, active_membership_change)) => {
                $node.set(crate::test_utils::TestNode::Follower(crate::node::follower::follower(unified_state, incoming_stream, transition_request, active_membership_change)))
            },
            output @ _ => panic!("unable to convert to follower, received {:?} from {}", output, stringify!($node))
        };

        seq_test!($($tail)*);
    };

    (convert $node:ident to candidate; $($tail:tt)*) => {
        match seq_test!(@recv from $node) {
            Some(crate::NodeOutput::ConvertToCandidate(mut unified_state, incoming_stream, last_membership_change)) => {
                let current_term = unified_state.current_term();

                unified_state
                    .set_current_term(
                        current_term + 1,
                        Some(stringify!($node)),
                    )
                    .await
                    .unwrap();

                $node.set(crate::test_utils::TestNode::Candidate(crate::node::candidate::candidate(unified_state, incoming_stream, last_membership_change)))
            },
            output @ _ => panic!("unable to convert to candidate, received {:?} from {}", output, stringify!($node))
        };

        seq_test!($($tail)*);
    };

    (convert $node:ident to leader; $($tail:tt)*) => {
        match seq_test!(@recv from $node) {
            Some(crate::NodeOutput::ConvertToLeader(unified_state, incoming_stream, active_membership_change)) => {
                $node.set(crate::test_utils::TestNode::Leader(crate::node::leader::leader(unified_state, incoming_stream, active_membership_change)))
            },
            output @ _ => panic!("unable to convert to leader, received {:?} from {}", output, stringify!($node))
        };

        seq_test!($($tail)*);
    };

    (skip $duration:expr; $($tail:tt)*) => {
        ::tokio::time::advance(::std::time::Duration::from_millis($duration)).await;
        seq_test!($($tail)*);
    };

    (assert that $node:ident termination is $val:expr; $($tail:tt)*) => {
        {
            use ::futures_util::stream::FusedStream;

            let is_terminated = match $node.as_mut().project() {
                crate::test_utils::TestNodeProj::Follower(follower) => follower.is_terminated(),
                crate::test_utils::TestNodeProj::Candidate(candidate) => candidate.is_terminated(),
                crate::test_utils::TestNodeProj::Leader(leader) => leader.is_terminated(),
            };

            assert_eq!(is_terminated, $val);
        }

        seq_test!($($tail)*);
    };

    (@recv from $node:ident) => {{
        use ::futures_util::{FutureExt, StreamExt};

        let received_event = match $node.as_mut().project() {
            crate::test_utils::TestNodeProj::Follower(mut follower) => follower.next().now_or_never(),
            crate::test_utils::TestNodeProj::Candidate(mut candidate) => candidate.next().now_or_never(),
            crate::test_utils::TestNodeProj::Leader(mut leader) => leader.next().now_or_never(),
        };

        received_event.flatten().transpose().unwrap()
    }};

    (@node $node:ident, $node_sender:ident, $node_persistent_state:expr; $($tail:tt)*) => {
        #[allow(unused_variables)]
        let ($node_sender, receiver) = ::flume::unbounded();
        let receiver = crate::test_utils::EmptyDebug(receiver.into_stream());
        ::futures_util::pin_mut!(receiver);

        let $node = crate::test_utils::TestNode::Follower(crate::node::follower::follower(crate::state::UnifiedState::new(
            stringify!($node),
            $node_persistent_state
        ).await.unwrap(), receiver, None, None));
        #[allow(unused_mut)]
        let mut $node = ::std::pin::pin!($node);

        seq_test!($($tail)*);
    };
}

/// Create new log entry identifier from the provided raw value.
///
/// # Panics
///
/// This function will panic if the provided value is not suitable as a log entry identifier (only values that are >= 1 can be used).
pub(crate) const fn id(val: u64) -> NonZeroU64 {
    match NonZeroU64::new(val) {
        Some(val) => val,
        None => unreachable!(),
    }
}

#[derive(Default, Debug)]
pub struct ShimmedState {
    current_term: u64,
    log: Vec<LogEntry<&'static str, usize>>,
    node_configuration: Vec<&'static str>,
    applied_log: Vec<LogEntry<&'static str, usize>>,
    voted_for: Option<&'static str>,
}

impl ShimmedState {
    pub fn new(node_configuration: Vec<&'static str>) -> Self {
        Self {
            node_configuration,
            ..Default::default()
        }
    }
}

type InitialStateFuture<'a> = impl Future<Output = Result<InitialState<NodeId>, Infallible>>;
type SetCurrentTermFuture<'a> = impl Future<Output = Result<(), Infallible>>;
type SetVotedForFuture<'a> = impl Future<Output = Result<(), Infallible>>;
type LogEntriesFuture<'a> =
    impl Future<Output = Result<Vec<LogEntry<&'static str, usize>>, Infallible>>;
type AppendLogEntryFuture<'a> = impl Future<Output = Result<(), Infallible>>;
type RemoveLogEntriesFuture<'a> = impl Future<Output = Result<(), Infallible>>;
type ApplyLogEntriesFuture<'a> = impl Future<Output = Result<(), Infallible>>;
type SleepFuture = impl Future<Output = Result<(), Infallible>>;
type IntervalFuture = impl Future<Output = Result<IntervalStream, Infallible>>;
type IntervalStream = impl Stream<Item = Result<(), Infallible>>;

impl<'a> PersistentStateLifetime<'a, &'static str, usize, Infallible> for ShimmedState {
    type Rng = ThreadRng;

    type InitialStateFuture = InitialStateFuture<'a>;

    type SetCurrentTermFuture = SetCurrentTermFuture<'a>;

    type SetVotedForFuture = SetVotedForFuture<'a>;

    type LogEntriesFuture = LogEntriesFuture<'a>;

    type AppendLogEntryFuture = AppendLogEntryFuture<'a>;

    type RemoveLogEntriesFuture = RemoveLogEntriesFuture<'a>;

    type ApplyLogEntriesFuture = ApplyLogEntriesFuture<'a>;
}

impl PersistentState<&'static str> for ShimmedState {
    type Contents = usize;

    type Error = Infallible;

    type SleepFuture = SleepFuture;

    type IntervalFuture = IntervalFuture;

    type IntervalStream = IntervalStream;

    fn initial_state(
        self: Pin<&mut Self>
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::InitialStateFuture{
        future::ready(Ok(InitialState {
            current_term: 0,
            last_committed_entry_id: None,
            last_log_entry: None,
            node_configuration: self.node_configuration.clone(),
            voted_for: None,
        }))
    }

    fn set_current_term(
        mut self: Pin<&mut Self>,
        current_term: u64,
        voted_for: Option<&'static str>
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::SetCurrentTermFuture{
        self.current_term = current_term;
        self.voted_for = voted_for;
        future::ready(Ok(()))
    }

    fn set_voted_for(
        mut self: Pin<&mut Self>,
        voted_for: &'static str,
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::SetVotedForFuture{
        self.voted_for = Some(voted_for);
        future::ready(Ok(()))
    }

    fn log_entries(
        self: Pin<&mut Self>,
        start: NonZeroU64,
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::LogEntriesFuture{
        future::ready(Ok(self
            .log
            .get((start.get() - 1) as usize..)
            .unwrap_or_default()
            .to_vec()))
    }

    fn append_log_entry(
        mut self: Pin<&mut Self>,
        log_entry: LogEntry<&'static str, Self::Contents>,
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::AppendLogEntryFuture{
        self.log.push(log_entry);
        future::ready(Ok(()))
    }

    fn remove_log_entries(
        mut self: Pin<&mut Self>,
        start: NonZeroU64,
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::RemoveLogEntriesFuture{
        self.log.truncate((start.get() - 1) as usize);
        future::ready(Ok(()))
    }

    fn apply_log_entries(
        mut self: Pin<&mut Self>,
        end: NonZeroU64
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::ApplyLogEntriesFuture{
        let start = self.applied_log.len();
        let log = self.log[start..end.get() as usize].to_vec();
        self.applied_log.extend_from_slice(&log);
        future::ready(Ok(()))
    }

    fn sleep(self: Pin<&mut Self>, duration: Duration) -> Self::SleepFuture {
        sleep(duration).map(Ok)
    }

    fn interval(self: Pin<&mut Self>, duration: Duration) -> Self::IntervalFuture {
        future::ready(Ok(stream::unfold(
            interval(duration),
            |mut state| async move {
                state.tick().await;
                Some((Ok(()), state))
            },
        )))
    }

    fn rng(
        self: Pin<&mut Self>,
    ) -> <Self as PersistentStateLifetime<'_, &'static str, Self::Contents, Self::Error>>::Rng {
        thread_rng()
    }
}
