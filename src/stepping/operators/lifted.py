from functools import partial
from typing import Any, Callable

from stepping.operators import builder, group, linear, transform
from stepping.types import (
    EMPTY,
    Empty,
    Index,
    K,
    Pair,
    T,
    TAddable,
    TIndexable,
    TReducable,
    U,
    ZSet,
    get_annotation_zset,
)


def distinct_lifted(a: ZSet[T]) -> ZSet[T]:
    """Proposition 6.3"""
    integrated = linear.integrate_delay(a)
    haitched = linear.haitch(integrated, a)
    return haitched


def join_lifted(
    l: ZSet[T], r: ZSet[U], *, on_left: Index[T, K], on_right: Index[U, K]
) -> ZSet[Pair[T, U]]:
    """Theorem 5.5"""
    l_integrated = linear.integrate_indexed(l, indexes=(on_left,))
    r_integrated = linear.integrate_delay_indexed(r, indexes=(on_right,))
    joined_1 = linear.join(l_integrated, r, on_left=on_left, on_right=on_right)
    joined_2 = linear.join(l, r_integrated, on_left=on_left, on_right=on_right)
    added = linear.add(joined_1, joined_2)
    return added


def outer_join_lifted(
    l: ZSet[T], r: ZSet[U], *, on_left: Index[T, K], on_right: Index[U, K]
) -> ZSet[Pair[T, U | Empty]]:
    with builder.at_compile_time:
        just_left: Callable[[Pair[T, U]], T] = lambda p: p.left
        make_empty_right: Callable[[T], Pair[T, Empty]] = lambda l: Pair(l, EMPTY)

    joined = join_lifted(l, r, on_left=on_left, on_right=on_right)
    lefted = linear.map(joined, f=just_left)
    negged = linear.neg(lefted)
    left_added = linear.add(l, negged)
    empty_right_added = linear.map(left_added, f=make_empty_right)
    final_added = linear.add(empty_right_added, joined)  # type: ignore[type-var]
    return final_added  # type: ignore[return-value]


def _incrementalise_aggregate_linear(
    a: ZSet[T], *, f: Callable[[ZSet[T]], TAddable]
) -> ZSet[TAddable]:
    """Section 11.1"""
    reduced = linear.reduce(a, f=f)
    integrated = linear.integrate(reduced)
    setted = linear.make_set(integrated)
    differentiated = linear.differentiate(setted)
    return differentiated


def _f_sum(
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
    z: ZSet[T],
) -> TReducable:
    total = zero()
    for v, count in z.iter():
        total += pick_value(v) * count
    return total


def reduce_lifted(
    a: ZSet[T],
    *,
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
) -> ZSet[TReducable]:
    """Section 11.1"""

    with builder.at_compile_time:
        f_sum: Callable[[ZSet[T]], TReducable] = partial(  # type: ignore[assignment]
            _f_sum,
            zero,
            pick_value,
        )

    linearised = _incrementalise_aggregate_linear(a, f=f_sum)
    return linearised


def _f_count(z: ZSet[Any]) -> int:
    total = 0
    for _, count in z.iter():
        total += count
    return total


def count_lifted(
    a: ZSet[T],
) -> ZSet[int]:
    """Section 11.1"""
    linearised = _incrementalise_aggregate_linear(a, f=_f_count)
    return linearised


def first_n_lifted(
    a: ZSet[T],
    *,
    index: Index[T, K],
    n: int,
) -> ZSet[T]:
    """Section 11.1

    "Not incremental in general, since for handling deletions
    they may need to know the full set and not just its changes."
    """
    integrated = linear.integrate_indexed(a, indexes=(index,))
    first_n_ed = linear.first_n(integrated, index=index, n=n)
    differentiated = linear.differentiate(first_n_ed)
    return differentiated


def group_reduce_flatten_lifted(
    a: ZSet[T],
    *,
    by: Index[T, K],
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
) -> ZSet[Pair[TReducable, K]]:
    grouped = group.group(a, by=by)
    reduced = transform.per_group[grouped](
        lambda g: reduce_lifted(g, zero=zero, pick_value=pick_value)
    )
    flattened = group.flatten(reduced)
    return flattened


def _transitive_closure(
    a: ZSet[Pair[TIndexable, TIndexable]]
) -> ZSet[Pair[TIndexable, TIndexable]]:
    """Section 9.1

    As per:

        https://github.com/vmware/database-stream-processor/blob/main/crates
        /dbsp/src/operator/recursive.rs#L172

    "This circuit computes the fixed point of equation:"

        y = distinct(f(i + Δi, y))

    The paper implies we should lift everything again - this doesn't work,
    I'm not sure why - maybe the join operators etc. operate on streams of
    streams, whereas we call `run.iteration` recursively. Anywho, the tests
    pass :shrug.
    """
    with builder.at_compile_time:
        with_right: Index[Pair[TIndexable, TIndexable], TIndexable] = Index.pick(
            get_annotation_zset(builder.compile_typeof(a)),
            lambda row: row.right,
        )
        with_left: Index[Pair[TIndexable, TIndexable], TIndexable] = Index.pick(
            get_annotation_zset(builder.compile_typeof(a)),
            lambda row: row.left,
        )
        pick_outers: Callable[
            [Pair[Pair[TIndexable, TIndexable], Pair[TIndexable, TIndexable]]],
            Pair[TIndexable, TIndexable],
        ] = lambda p: Pair(p.left.left, p.right.right)

    delayed: ZSet[Pair[TIndexable, TIndexable]]

    joined = join_lifted(a, delayed, on_left=with_right, on_right=with_left)
    picked_outers = linear.map(joined, f=pick_outers)

    unioned = linear.add(a, picked_outers)
    distincted = distinct_lifted(unioned)
    delayed = linear.delay(distincted)
    return distincted


def transitive_closure_lifted(
    a: ZSet[Pair[TIndexable, TIndexable]]
) -> ZSet[Pair[TIndexable, TIndexable]]:
    recursed = transform.integrate_til_zero[a](lambda a: _transitive_closure(a))
    return recursed
