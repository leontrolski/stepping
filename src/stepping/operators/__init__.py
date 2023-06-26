from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Annotated, Callable

from stepping.graph import A1, A2, Graph
from stepping.operators.group import flatten as _flatten
from stepping.operators.group import group as _group
from stepping.operators.incremental import count as _count
from stepping.operators.incremental import distinct as _distinct
from stepping.operators.incremental import first_n as _first_n
from stepping.operators.incremental import join as _join
from stepping.operators.incremental import outer_join as _outer_join
from stepping.operators.incremental import reduce as _reduce
from stepping.operators.incremental import sum as _sum
from stepping.operators.linear import add as _add
from stepping.operators.linear import delay as _delay
from stepping.operators.linear import differentiate as _differentiate
from stepping.operators.linear import filter as _filter
from stepping.operators.linear import identity as _identity
from stepping.operators.linear import integrate as _integrate
from stepping.operators.linear import make_scalar as _make_scalar
from stepping.operators.linear import make_set as _make_set
from stepping.operators.linear import map as _map
from stepping.operators.linear import neg as _neg
from stepping.operators.transform import finalize as finalize
from stepping.operators.transform import lift_grouped as lift_grouped
from stepping.types import Empty, Field, Grouped, Index, K, Number, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import (
    T,
    TAddable,
    TAddAndNegable,
    TNegable,
    TReducable,
    U,
    V,
    ZSet,
    choose,
)


# fmt: off
def add(t: type[TAddable]) -> Graph[A2[TAddable, TAddable], A1[TAddable]]: return _add(_make_name(), t)
def count(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[int]]]: return _count(_make_name(), t)
def delay(t: type[T]) -> Graph[A1[T], A1[T]]: return _delay(_make_name(), t)
def differentiate(t: type[TAddAndNegable]) -> Graph[A1[TAddAndNegable], A1[TAddAndNegable]]: return _differentiate(_make_name(), t)
def distinct(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]: return _distinct(_make_name(), t)
def filter(t: type[T], f: Callable[[T], bool]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]: return _filter(_make_name(), t, f)
def first_n(t: type[T], index: Index[T, K], n: int) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]: return _first_n(_make_name(), t, index, n)
def flatten(t: type[T], k: type[K]) -> Graph[A1[Grouped[ZSet[T], K]], A1[ZSet[Pair[K, T]]]]: return _flatten(_make_name(), t, k)
def group(t: type[T], index: Index[T, K]) -> Graph[A1[ZSet[T]], A1[Grouped[ZSet[T], K]]]: return _group(_make_name(), t, index)
def identity(t: type[T]) -> Graph[A1[T], A1[T]]: return _identity(_make_name(), t)
def integrate(t: type[TAddable], output_delay: bool = False) -> Graph[A1[TAddable], A1[TAddable]]: return _integrate(_make_name(), t, output_delay)
def join(t: type[T], u: type[U], on_t: Index[T, K], on_u: Index[U, K]) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[Pair[T, U]]]]: return _join(_make_name(), t, u, on_t, on_u)
def make_scalar(t: type[T]) -> Graph[A1[ZSet[T]], A1[T]]: return _make_scalar(_make_name(), t)
def make_set(t: type[T]) -> Graph[A1[T], A1[ZSet[T]]]: return _make_set(_make_name(), t)
def map(t: type[T], v: type[V], f: Callable[[T], V]) -> Graph[A1[ZSet[T]], A1[ZSet[V]]]: return _map(_make_name(), t, v, f)
def neg(t: type[TNegable]) -> Graph[A1[TNegable], A1[TNegable]]: return _neg(_make_name(), t)
def outer_join(t: type[T], u: type[U], on_t: Index[T, K], on_u: Index[U, K]) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[Pair[T, U | Empty]]]]: return _outer_join(_make_name(), t, u, on_t, on_u)
def reduce(t: type[T], v: type[TReducable], pick_reducable: Callable[[T], TReducable]) -> Graph[A1[ZSet[T]], A1[ZSet[TReducable]]]: return _reduce(_make_name(), t, v, pick_reducable)
def sum(t: type[T], v: type[TReducable], pick_number: Callable[[T], TReducable]) -> Graph[A1[ZSet[T]], A1[ZSet[TReducable]]]: return _sum(_make_name(), t, v, pick_number)
# fmt: on

i = 0


def reset_vertex_counter() -> None:
    global i
    i = 0


def _make_name() -> str:
    global i
    i += 1
    return f"_N{i}"


def add_zset(t: type[T], ) -> Graph[A2[ZSet[T], ZSet[T]], A1[ZSet[T]]]:
    return add(R[ZSet[T]].sub(T=t))


def delay_zset(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return delay(R[ZSet[T]].sub(T=t))


def differentiate_zset(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return differentiate(R[ZSet[T]].sub(T=t))


def identity_zset(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return identity(R[ZSet[T]].sub(T=t))


def integrate_zset(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return integrate(R[ZSet[T]].sub(T=t))


def integrate_zset_indexed(
    t: type[T], index: Index[T, K]
) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return integrate(R[Annotated[ZSet[T], index]].sub(T=t, K=index.k))


def neg_zset(t: type[T]) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    return neg(R[ZSet[T]].sub(T=t))


# def sum_zset(t: type[T], v: TReducable, pick_number: Callable[[T], TReducable]) -> Graph[A1[ZSet[T]], A1[ZSet[TReducable]]]:
#     return sum(R[ZSet[T]].sub(T=t), R[TReducable].sub(TReducable=v), pick_number)


def join_flat(
    t: type[T],
    u: type[U],
    on_t: Index[T, K],
    on_u: Index[U, K],
    v: type[V],
) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[V]]]:
    if 1 == 1:  # to please mypy for the join(...) types
        assert is_dataclass(t)
        assert is_dataclass(u)
        assert is_dataclass(v)

    def flatten(p: Pair[T, U]) -> V:
        return v(**(asdict(p.left) | asdict(p.right)))  # type: ignore

    return join(t, u, on_t, on_u).connect(
        map(
            R[Pair[T, U]].sub(T=t, U=u),
            R[V].sub(V=v),
            flatten,
        )
    )


def group_reduce_flatten(
    t: type[T],
    group_by: Index[T, K],
    reduce_on: Field[T, TReducable],
) -> Graph[A1[ZSet[T]], A1[ZSet[Pair[K, TReducable]]],]:
    reduce_per_group = lift_grouped(
        group_by.k,
        reduce(
            t,
            reduce_on.k,
            pick_reducable=lambda v: choose(reduce_on, v),
        ),
    )
    return (
        group(t, group_by)
        .connect(reduce_per_group)
        .connect(flatten(reduce_on.k, group_by.k))
    )
