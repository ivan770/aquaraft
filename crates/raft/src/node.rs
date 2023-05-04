mod candidate;
mod follower;
mod leader;

pub(crate) use candidate::{candidate, CandidateNode};
pub(crate) use follower::{follower, FollowerNode};
pub(crate) use leader::{leader, LeaderNode};

use core::ops::Range;

#[cfg(not(test))]
const TIMER_RANGE: Range<u64> = 1500..2000;

#[cfg(not(test))]
const HEARTBEAT_TIMER_RANGE: Range<u64> = 100..200;

#[cfg(test)]
const TIMER_RANGE: Range<u64> = 10000..10001;

#[cfg(test)]
const HEARTBEAT_TIMER_RANGE: Range<u64> = 250..251;

#[cfg(test)]
mod tests {
    use crate::seq_test;

    #[tokio::test(start_paused = true)]
    async fn stream_termination() {
        seq_test! {
            node_configuration main_config, ["first"];

            node first, first_sender, main_config;
        }

        drop(first_sender);

        seq_test! {
            recv (None) from first;

            assert that first termination is true;
        }
    }
}
