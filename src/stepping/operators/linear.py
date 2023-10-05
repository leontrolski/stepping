from typing import Callable

from stepping.graph import OperatorKind
from stepping.operators import builder
from stepping.types import (
    Index,
    Indexable,
    K,
    Pair,
    T,
    TAddable,
    TAddAndNegable,
    TNegable,
    U,
    V,
    ZSet,
)
from stepping.zset import functions
from stepping.zset.python import ZSetPython
from stepping.zset.sql.generic import ZSetSQL


@builder.vertex(OperatorKind.identity_dont_remove)
def identity_print(a: T) -> T:
    print(a)
    return a


@builder.vertex(OperatorKind.delay)
def delay(a: T) -> T:
    raise NotImplementedError("delay should never get called, it is special")


@builder.vertex(OperatorKind.delay)
def delay_indexed(a: ZSet[T], *, indexes: tuple[Index[T, Indexable], ...]) -> ZSet[T]:
    raise RuntimeError("delay should never get called, it is special")


@builder.vertex(OperatorKind.neg)
def neg(a: TNegable) -> TNegable:
    return -a


@builder.vertex(OperatorKind.add)
def add(a: TAddable, b: TAddable) -> TAddable:
    return a + b


def add3(a: TAddable, b: TAddable, c: TAddable) -> TAddable:
    added_two = add(a, b)
    added_three = add(added_two, c)
    return added_three


def integrate(a: TAddable) -> TAddable:
    """Definition 3.27"""
    delayed: TAddable

    added = add(delayed, a)
    delayed = delay(added)
    return added


def integrate_indexed(
    a: ZSet[T], *, indexes: tuple[Index[T, Indexable], ...]
) -> ZSet[T]:
    delayed: ZSet[T]
    added = add(delayed, a)
    delayed = delay_indexed(added, indexes=indexes)
    return added


def integrate_delay(a: TAddable) -> TAddable:
    delayed: TAddable
    added = add(delayed, a)
    delayed = delay(added)
    return delayed


def integrate_delay_indexed(
    a: ZSet[T], *, indexes: tuple[Index[T, Indexable], ...]
) -> ZSet[T]:
    delayed: ZSet[T]
    added = add(delayed, a)
    delayed = delay_indexed(added, indexes=indexes)
    return delayed


def differentiate(a: TAddAndNegable) -> TAddAndNegable:
    """Definition 3.25"""
    delayed = delay(a)
    negged = neg(delayed)
    added = add(negged, a)
    return added


@builder.vertex(OperatorKind.map)
def map(a: ZSet[T], *, f: Callable[[T], V]) -> ZSet[V]:
    return functions.map(a, f)


@builder.vertex(OperatorKind.map_many)
def map_many(a: ZSet[T], *, f: Callable[[T], frozenset[V]]) -> ZSet[V]:
    return functions.map_many(a, f)


@builder.vertex(OperatorKind.filter)
def filter(a: ZSet[T], *, f: Callable[[T], bool]) -> ZSet[T]:
    return functions.filter(a, f)


@builder.vertex(OperatorKind.reduce)
def reduce(a: ZSet[T], *, f: Callable[[ZSet[T]], V]) -> V:
    return f(a)


@builder.vertex(OperatorKind.make_set)
def make_set(a: T) -> ZSet[T]:
    return ZSetPython({a: 1})


@builder.vertex(OperatorKind.make_scalar)
def make_scalar(a: ZSet[T], *, zero: Callable[[], T]) -> T:
    values_counts = list(a.iter())
    if len(values_counts) == 0:
        return zero()
    if len(values_counts) == 1:
        ([value, count],) = values_counts
        if count == 1:
            return value
    raise RuntimeError("Can only make scalars from ZSets length 1, count 1")


@builder.vertex(OperatorKind.haitch)
def haitch(a: ZSet[T], b: ZSet[T]) -> ZSet[T]:
    return functions.haitch(a, b)


@builder.vertex(OperatorKind.join)
def join(
    l: ZSet[T], r: ZSet[U], *, on_left: Index[T, K], on_right: Index[U, K]
) -> ZSet[Pair[T, U]]:
    return functions.join(l, r, on_left, on_right)


@builder.vertex(OperatorKind.first_n)
def first_n(z: ZSet[T], *, index: Index[T, K], n: int) -> ZSet[T]:
    return functions.first_n(z, index, n)
