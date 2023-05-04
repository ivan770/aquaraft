use core::num::NonZeroU64;

use itertools::{EitherOrBoth, Itertools};

use crate::state::LogEntryMetadata;

pub(super) enum SuitableAction {
    Push(usize),
    Replace(NonZeroU64, usize),
    DoNothing,
}

#[inline]
pub(super) fn find_suitable_action<T, O>(their_entries: T, our_entries: O) -> Option<SuitableAction>
where
    T: IntoIterator<Item = LogEntryMetadata>,
    O: IntoIterator<Item = LogEntryMetadata>,
{
    their_entries
        .into_iter()
        .enumerate()
        .zip_longest(our_entries.into_iter())
        .find_map(|entry| match entry {
            EitherOrBoth::Left((idx, _)) => Some(SuitableAction::Push(idx)),
            EitherOrBoth::Both((idx, leader_entry), our_entry) if leader_entry != our_entry => {
                Some(SuitableAction::Replace(our_entry.id, idx))
            }
            // While we could just return None here, returning Some(DoNothing) allows
            // us to short-curcuit iterator in case if our log is much longer
            // than the leader-provided one.
            EitherOrBoth::Right(_) => Some(SuitableAction::DoNothing),
            _ => None,
        })
}
