from __future__ import annotations

from collections import defaultdict
from itertools import groupby
from typing import Callable, Iterator

from stepping.types import MATCH_ALL, Index, Indexable, K, MatchAll, Pair, T, U, V, ZSet
from stepping.zset.python import ZSetPython


def map(z: ZSet[T], f: Callable[[T], V]) -> ZSet[V]:
    return ZSetPython((f(value), count) for value, count in z.iter())


def map_many(z: ZSet[T], f: Callable[[T], frozenset[V]]) -> ZSet[V]:
    return ZSetPython((v, count) for value, count in z.iter() for v in f(value))


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
    rows = ((value, count) for _, value, count in z.iter_by_index(index))
    return ZSetPython[T](indexes=(index,)) + ZSetPython(_first_n(rows, n))


def iter_by_index_grouped(
    z: ZSet[T],
    index: Index[T, K],
    match_keys: frozenset[K] | MatchAll = MATCH_ALL,
) -> Iterator[tuple[K, Iterator[tuple[T, int]]]]:
    rows = z.iter_by_index(index, match_keys)
    grouped = groupby(rows, key=lambda row: row[0])
    while True:
        n = next(grouped, None)
        if n is None:
            break
        key, inner = n
        inner_ = ((value, count) for _, value, count in inner)
        yield key, inner_


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
            keys = frozenset(on_right.f(right) for right, _ in r.iter())
            for key, g in iter_by_index_grouped(l, on_left, keys):
                for left, count_left in g:
                    d[key].add((left, count_left))
        else:
            for left, count_left in l.iter():
                d[on_left.f(left)].add((left, count_left))

        for right, count_right in r.iter():
            k = on_right.f(right)
            for left, count_left in d[k]:
                new_count = count_left * count_right
                if new_count != 0:
                    yield Pair(left, right), new_count

    return ZSetPython(lazy_join())


def _changing_from_negative_to_positive(x: int, y: int) -> int:
    if x <= 0 and x + y > 0:
        return 1
    if x > 0 and x + y <= 0:
        return -1
    return 0


def haitch(l: ZSet[T], r: ZSet[T]) -> ZSet[T]:
    """Proposition 6.3"""
    changes = frozenset[T]()
    from_counts: dict[T, int] = defaultdict(int)
    to_counts: dict[T, int] = defaultdict(int)
    for v, count in r.iter():
        changes |= {v}
        to_counts[v] = count
    # For effiency's sake, we assume `l` is the larger of the two ZSets.
    for v, count in l.iter(match=changes):
        from_counts[v] = count
    d = {
        v: _changing_from_negative_to_positive(from_counts[v], to_counts[v])
        for v in changes
    }
    return ZSetPython((v, count) for v, count in d.items() if count != 0)
