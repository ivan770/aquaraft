#![no_std]

extern crate alloc;

use core::{borrow::Borrow, cmp::Ordering};

use alloc::vec::Vec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecMap<K, V> {
    inner: Vec<(K, V)>,
}

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K, V> VecMap<K, V>
where
    K: PartialEq,
{
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn get<Q: Borrow<K> + ?Sized>(&self, key: &Q) -> Option<&V> {
        self.inner
            .iter()
            .find(|(k, _)| k == key.borrow())
            .map(|(_, v)| v)
    }

    #[inline]
    pub fn get_mut<Q: Borrow<K> + ?Sized>(&mut self, key: &Q) -> Option<&mut V> {
        self.inner
            .iter_mut()
            .find(|(k, _)| k == key.borrow())
            .map(|(_, v)| v)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> + '_ {
        self.inner.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> + '_ {
        self.inner.iter_mut().map(|(k, v)| (&*k, v))
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) -> bool {
        if !self.inner.iter().any(|(k, _)| k == &key) {
            self.inner.push((key, value));
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn remove<Q: Borrow<K> + ?Sized>(&mut self, key: &Q) {
        let position = self.inner.iter().position(|(k, _)| k == key.borrow());

        if let Some(position) = position {
            self.inner.swap_remove(position);
        }
    }

    #[inline]
    pub fn select_nth_unstable_by<F: FnMut(&(K, V), &(K, V)) -> Ordering>(
        &mut self,
        index: usize,
        compare: F,
    ) -> Option<&mut V> {
        if index >= self.inner.len() {
            None
        } else {
            let (_, (_, value), _) = self.inner.select_nth_unstable_by(index, compare);
            Some(value)
        }
    }
}

impl<K, V> Extend<(K, V)> for VecMap<K, V>
where
    K: PartialEq,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        let iter = iter.into_iter();

        let (lo, hi) = iter.size_hint();
        self.inner.reserve(hi.unwrap_or(lo));

        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K, V> FromIterator<(K, V)> for VecMap<K, V>
where
    K: PartialEq,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut vec_map = VecMap::default();
        vec_map.extend(iter);
        vec_map
    }
}

pub struct VecSet<T> {
    inner: VecMap<T, ()>,
}

impl<T> Default for VecSet<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<T> VecSet<T>
where
    T: PartialEq,
{
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecMap::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn contains_key<Q: Borrow<T> + ?Sized>(&self, key: &Q) -> bool {
        self.inner.get(key).is_some()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn insert(&mut self, key: T) -> bool {
        self.inner.insert(key, ())
    }

    #[inline]
    pub fn remove<Q: Borrow<T> + ?Sized>(&mut self, key: &Q) {
        self.inner.remove(key);
    }
}
