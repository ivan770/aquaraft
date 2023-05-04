// Since there is no "Option::take_if" method, we have to do all this mess.
// This will (hopefully) get optimized away: https://godbolt.org/z/zzYYfx6j5
// What's even more interesting, is that unsafe usage makes rustc generate somewhat different (yet looking quite similar) x86 asm:
// https://godbolt.org/z/nMTW5j3Kd

pub(crate) trait OptionExt<T> {
    fn take_if<F: FnOnce(&T) -> bool>(&mut self, predicate: F) -> Option<T>;
}

impl<T> OptionExt<T> for Option<T> {
    #[inline]
    fn take_if<F: FnOnce(&T) -> bool>(&mut self, predicate: F) -> Option<T> {
        if self.as_ref().filter(|val| predicate(val)).is_some() {
            Some(self.take().unwrap())
        } else {
            None
        }
    }
}
