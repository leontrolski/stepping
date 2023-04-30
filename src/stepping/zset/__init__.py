from __future__ import annotations

from collections import defaultdict
from itertools import groupby
from typing import Callable, Iterator

from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    K,
    MatchAll,
    Pair,
    T,
    U,
    V,
    ZSet,
    choose,
)
from stepping.zset.python import ZSetPython


def _changing_from_negative_to_positive(x: int, y: int) -> int:
    if x <= 0 and x + y > 0:
        return 1
    if x > 0 and x + y <= 0:
        return -1
    return 0


def neg(z: ZSet[T]) -> ZSet[T]:
    return ZSetPython((value, -count) for value, count in z.iter())


def mul(l: ZSet[T], r: int) -> ZSet[T]:
    if r == 0:
        return ZSetPython[T]()
    return ZSetPython((value, count * r) for value, count in l.iter())


def map(z: ZSet[T], f: Callable[[T], V]) -> ZSet[V]:
    return ZSetPython((f(value), count) for value, count in z.iter())


def filter(z: ZSet[T], f: Callable[[T], bool]) -> ZSet[T]:
    return ZSetPython((value, count) for value, count in z.iter() if f(value))


def _first_n(rows: Iterator[tuple[T, int]], n: int) -> Iterator[tuple[T, int]]:
    total = 0
    for value, count in rows:
        assert count > 0
        total += count
        if total > n:
            count = min(count, total - n)
        if count:
            yield value, count
        if total >= n:
            return


def first_n(z: ZSet[T], index: Index[T, K], n: int) -> ZSet[T]:
    rows = (
        (value, count) for _, value, count in z.iter_by_index_generic(index.generic)
    )
    return ZSetPython[T](indexes=(index.generic,)) + ZSetPython(_first_n(rows, n))


def iter_by_index_grouped(
    z: ZSet[T],
    index: Index[T, K],
    match_keys: tuple[K, ...] | MatchAll = MATCH_ALL,
) -> Iterator[tuple[K, Iterator[tuple[T, int]]]]:
    rows = z.iter_by_index_generic(index.generic, match_keys)
    grouped = groupby(rows, key=lambda row: row[0])
    while True:
        n = next(grouped, None)
        if n is None:
            break
        key, inner = n
        inner_ = ((value, count) for _, value, count in inner)
        yield key, inner_  # type: ignore


def join(
    l: ZSet[T],
    r: ZSet[U],
    on_left: Index[T, K],
    on_right: Index[U, K],
) -> ZSet[Pair[T, U]]:
    if on_right in r.indexes:
        return map(join(r, l, on_right, on_left), lambda p: Pair(p.right, p.left))
    if isinstance(l, ZSetPython) and l.empty():
        return ZSetPython[Pair[T, U]]()

    def lazy_join() -> Iterator[tuple[Pair[T, U], int]]:
        d: dict[Indexable, set[tuple[T, int]]] = defaultdict(set)

        if on_left in l.indexes:
            keys = tuple(choose(on_right, right) for right, _ in r.iter())
            for key, g in iter_by_index_grouped(l, on_left, keys):
                for left, count_left in g:
                    d[key].add((left, count_left))
        else:
            for left, count_left in l.iter():
                d[choose(on_left, left)].add((left, count_left))

        for right, count_right in r.iter():
            k = choose(on_right, right)
            for left, count_left in d[k]:
                new_count = count_left * count_right
                if new_count != 0:
                    yield Pair(left, right), new_count

    return ZSetPython(lazy_join())


def haitch(l: ZSet[T], r: ZSet[T]) -> ZSet[T]:
    """Proposition 6.3"""
    all_vs = set[T]()
    from_counts: dict[T, int] = defaultdict(int)
    to_counts: dict[T, int] = defaultdict(int)
    for v, count in l.iter():
        all_vs.add(v)
        from_counts[v] = count
    for v, count in r.iter():
        all_vs.add(v)
        to_counts[v] = count
    d = {
        v: _changing_from_negative_to_positive(from_counts[v], to_counts[v])
        for v in all_vs
    }
    return ZSetPython((v, count) for v, count in d.items() if count != 0)


def iter_by_index(
    z: ZSet[T], index: Index[T, K], match_keys: tuple[K, ...] | MatchAll = MATCH_ALL
) -> Iterator[tuple[K, T, int]]:
    return z.iter_by_index_generic(index.generic, match_keys)  # type: ignore


# def __contains__(self, x: T) -> bool:
#     return x in self.data
# def __gte__(self, other: ZSet[T]) -> bool:
#     return is_positive(self + -other)
# def is_set(z: ZSet[T]) -> bool:
#     return all(v == 1 for v in z.data.values())
# def is_positive(z: ZSet[T]) -> bool:
#     return all(v >= 1 for v in z.data.values())
# def distinct(z: ZSet[T]) -> ZSetPython[T]:
#     return ZSetPython({k: 1 for k, v in z.data.items() if v > 0})
# def cross_product(
# def outer_join(
