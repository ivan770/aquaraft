use core::future::Future;

use enstream::Yielder;

pub(crate) trait TryYielderExt<'yielder, T, E> {
    type TryWithYieldFut<'a, A>: Future<Output = A> + 'a
    where
        'yielder: 'a,
        Self: 'yielder,
        A: 'a,
        T: 'yielder,
        E: 'yielder;

    fn try_with_yield<'a, A>(&'a mut self, val: Result<A, E>) -> Self::TryWithYieldFut<'a, A>
    where
        'yielder: 'a;
}

impl<'yielder, T, E> TryYielderExt<'yielder, T, E> for Yielder<'yielder, Result<T, E>> {
    type TryWithYieldFut<'a, A> = impl Future<Output = A> + 'a
    where
        'yielder: 'a,
        Self: 'yielder,
        A: 'a,
        T: 'yielder,
        E: 'yielder;

    #[inline]
    fn try_with_yield<'a, A>(&'a mut self, val: Result<A, E>) -> Self::TryWithYieldFut<'a, A>
    where
        'yielder: 'a,
    {
        async move {
            match val {
                Ok(val) => val,
                Err(err) => {
                    self.yield_item(Err(err)).await;
                    unreachable!()
                }
            }
        }
    }
}
