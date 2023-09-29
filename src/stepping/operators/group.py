from typing import Any

from stepping import zset
from stepping.graph import A1, Graph, OperatorKind, Path, VertexUnary
from stepping.operators import builder, linear
from stepping.types import (
    Empty,
    Grouped,
    Index,
    K,
    Pair,
    Signature,
    T,
    ZSet,
    pick_index,
)
from stepping.zset import functions
from stepping.zset.python import ZSetPython


@builder.vertex(OperatorKind.group)
def group(a: ZSet[T], *, by: Index[T, K]) -> Grouped[ZSet[T], K]:
    out = Grouped[ZSet[T], K]()
    for value, count in a.iter():
        key = by.f(value)
        group_value = out.get(key)
        if isinstance(group_value, Empty):
            out.set(key, ZSetPython({value: count}))
        else:
            out.set(key, group_value + ZSetPython({value: count}))
    return out


@builder.vertex(OperatorKind.flatten)
def flatten(a: Grouped[ZSet[T], K]) -> ZSet[Pair[T, K]]:
    out = ZSetPython[Pair[T, K]]()
    for z, key in a.iter():
        for v, count in z.iter():
            out += ZSetPython({Pair(v, key): count})
    return out


@builder.vertex(OperatorKind.make_indexed_pairs)
def make_indexed_pairs(
    a: Grouped[ZSet[T], K], *, index: Index[Pair[T, K], K]
) -> ZSet[Pair[T, K]]:
    z = ZSetPython[Pair[T, K]](indexes=(index,))
    for inner, key in a.iter():
        z += functions.map(inner, lambda value: Pair(value, key))
    return z


@builder.vertex(OperatorKind.make_grouped)
def make_grouped(
    a: ZSet[Pair[T, K]], *, index: Index[Pair[T, K], K]
) -> Grouped[ZSet[T], K]:
    out = Grouped[ZSet[T], K]()
    for key, inner in functions.iter_by_index_grouped(a, index):
        out.set(key, ZSetPython[T]((value.left, count) for value, count in inner))
    return out


@builder.vertex(OperatorKind.get_keys)
def get_keys(a: Grouped[Any, K]) -> frozenset[K]:
    keys = frozenset(key for _, key in a.iter())
    return keys


@builder.vertex(OperatorKind.pick_relevant)
def pick_relevant(
    keys: frozenset[K], a: ZSet[Pair[T, K]], *, index: Index[Pair[T, K], K]
) -> ZSet[Pair[T, K]]:
    out = ZSetPython[Pair[T, K]](indexes=(index,))
    out += ZSetPython(
        (value, count) for _, value, count in a.iter_by_index(index, keys)
    )
    return out


def _wrap_delay(
    a: Grouped[ZSet[T], K], *, index: Index[Pair[T, K], K]
) -> Grouped[ZSet[T], K]:
    added: ZSet[Pair[T, K]]

    indexed_pairs = make_indexed_pairs(a, index=index)
    new_delay = linear.delay_indexed(added, indexes=(index,))
    keys = get_keys(a)  # gets replaced by `first_vertex`
    relevant = pick_relevant(keys, new_delay, index=index)
    negative_relevant = linear.neg(relevant)
    relevant_grouped = make_grouped(relevant, index=index)
    added = linear.add3(new_delay, negative_relevant, indexed_pairs)
    return relevant_grouped


def wrap_delay(
    path: Path,
    t: type[T],
    k: type[K],
    first_vertex: VertexUnary[Any, Grouped[Any, K]],
) -> Graph[A1[Grouped[ZSet[T], K]], Grouped[ZSet[T], K]]:
    index = pick_index(
        Pair[t, k],  # type: ignore[valid-type]
        lambda p: p.right,
    )
    graph = builder.compile_generic(
        _wrap_delay,
        {"index": index},
        Signature(
            [("a", Grouped[ZSet[t], k])],  # type: ignore[valid-type]
            {"index": Index[index.t, index.k]},  # type: ignore
            Grouped[ZSet[t], k],  # type: ignore
        ),
        path,
    )
    for from_vertex, [to_vertex, i] in list(graph.internal):
        if to_vertex.operator_kind is OperatorKind.get_keys:
            to_vertex.t = first_vertex.v
            graph.internal.remove((from_vertex, (to_vertex, i)))
            graph.internal.add((first_vertex, (to_vertex, i)))

    return graph
