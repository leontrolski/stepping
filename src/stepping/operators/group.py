from dataclasses import replace
from typing import Any, get_args

from stepping import zset
from stepping.graph import A1, Graph, OperatorKind, Path, VertexUnary
from stepping.operators import builder, linear
from stepping.types import Empty, Grouped, Index, K, Signature, T, ZSet
from stepping.zset import functions
from stepping.zset.python import ZSetPython


@builder.vertex(OperatorKind.group)
def group(a: ZSet[T], *, by: Index[T, K]) -> Grouped[ZSet[T], K]:
    out = Grouped[ZSet[T], K](by.t, by.k)
    for value, count in a.iter():
        key = by.f(value)
        group_value = out.get(key)
        if isinstance(group_value, Empty):
            out.set(key, ZSetPython(a.t, [(value, count)]))
        else:
            out.set(key, group_value + ZSetPython(a.t, [(value, count)]))
    return out


@builder.vertex(OperatorKind.flatten)
def flatten(a: Grouped[ZSet[T], K]) -> ZSet[tuple[T, K]]:
    t = get_args(a.t)[0]
    return ZSetPython[tuple[T, K]](
        tuple[t, a.k],
        (((v, key), count) for z, key in a.iter() for v, count in z.iter()),
    )


@builder.vertex(OperatorKind.make_indexed_pairs)
def make_indexed_pairs(
    a: Grouped[ZSet[T], K], *, index: Index[tuple[T, K], K]
) -> ZSet[tuple[T, K]]:
    out = ZSetPython[tuple[T, K]](index.t, indexes=(index,))
    return out + ZSetPython[tuple[T, K]](
        index.t,
        (
            ((value, key), count)
            for inner, key in a.iter()
            for value, count in inner.iter()
        ),
    )


@builder.vertex(OperatorKind.make_grouped)
def make_grouped(
    a: ZSet[tuple[T, K]], *, index: Index[tuple[T, K], K]
) -> Grouped[ZSet[T], K]:
    out = Grouped[ZSet[T], K](get_args(index.t)[0], index.k)
    for key, inner in functions.iter_by_index_grouped(a, index):
        out.set(
            key,
            ZSetPython[T](
                get_args(a.t)[0],
                ((value[0], count) for value, count in inner),
            ),
        )
    return out


@builder.vertex(OperatorKind.get_keys)
def get_keys(a: Grouped[Any, K]) -> tuple[K, ...]:
    keys = tuple(key for _, key in a.iter())
    return keys


@builder.vertex(OperatorKind.pick_relevant)
def pick_relevant(
    keys: tuple[K, ...], a: ZSet[tuple[T, K]], *, index: Index[tuple[T, K], K]
) -> ZSet[tuple[T, K]]:
    out = ZSetPython[tuple[T, K]](a.t, indexes=(index,))
    out += ZSetPython(
        a.t, ((value, count) for _, value, count in a.iter_by_index(index, keys))
    )
    return out


def _wrap_delay(
    a: Grouped[ZSet[T], K], *, index: Index[tuple[T, K], K]
) -> Grouped[ZSet[T], K]:
    added: ZSet[tuple[T, K]]

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
    index = Index.pick(
        tuple[t, k],  # type: ignore[valid-type]
        lambda p: p[1],
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
    for from_p, [to_p, i] in list(graph.internal):
        to_vertex = graph.vertices[to_p]
        if to_vertex.operator_kind is OperatorKind.get_keys:
            graph.vertices[to_p] = replace(to_vertex, t=first_vertex.v)
            graph.internal.remove((from_p, (to_p, i)))
            graph.internal.add((first_vertex.path, (to_p, i)))

    return graph
