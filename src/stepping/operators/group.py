from typing import Annotated, Any

from stepping import zset
from stepping.graph import A1, A2, Graph, VertexBinary, VertexUnary, get_single_vertex
from stepping.operators.linear import add, delay, neg
from stepping.types import Empty, Grouped, Index, K, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import T, ZSet, choose, get_annotation_grouped, pick_index
from stepping.zset import iter_by_index, iter_by_index_grouped
from stepping.zset.python import ZSetPython


def group(
    name: str,
    t: type[T],
    index: Index[T, K],
) -> Graph[A1[ZSet[T]], A1[Grouped[ZSet[T], K]]]:
    name = "group_" + name
    TGrouped = R[ZSet[T]].sub(T=t)

    def z_group(a: ZSet[T]) -> Grouped[ZSet[T], K]:
        out = Grouped[ZSet[T], K](TGrouped, index.k)
        for value, count in a.iter():
            key = choose(index, value)
            group_value = out.get(key)
            if isinstance(group_value, Empty):
                out.set(key, ZSetPython({value: count}))
            else:
                out.set(key, group_value + ZSetPython({value: count}))
        return out

    return VertexUnary(
        R[ZSet[T]].sub(T=t),
        R[Grouped[ZSet[T], K]].sub(T=t, K=index.k),
        name,
        f=z_group,
    ).g


def flatten(
    name: str,
    t: type[T],
    k: type[K],
) -> Graph[A1[Grouped[ZSet[T], K]], A1[ZSet[Pair[K, T]]]]:
    name = "flatten_" + name

    def z_flatten(a: Grouped[ZSet[T], K]) -> ZSet[Pair[K, T]]:
        out = ZSetPython[Pair[K, T]]()
        for key, z in a.iter():
            for v, count in z.iter():
                out += ZSetPython({Pair(key, v): count})
        return out

    return VertexUnary(
        R[Grouped[ZSet[T], K]].sub(T=t, K=k),
        R[ZSet[Pair[K, T]]].sub(T=t, K=k),
        name,
        f=z_flatten,
    ).g


def make_indexed_pairs(
    name: str,
    t: type[T],
    index: Index[Pair[T, K], K],
) -> Graph[A1[Grouped[ZSet[T], K]], A1[ZSet[Pair[T, K]]]]:
    name = "make_indexed_pairs_" + name

    def z_make_indexed_pairs(g: Grouped[ZSet[T], K]) -> ZSet[Pair[T, K]]:
        z = ZSetPython[Pair[T, K]](indexes=(index.generic,))
        for key, inner in g.iter():
            z += zset.map(inner, lambda value: Pair(value, key))
        return z

    return VertexUnary(
        R[Grouped[ZSet[T], K]].sub(T=t, K=index.k),
        R[Annotated[ZSet[Pair[T, K]], index]].sub(T=t, K=index.k),
        name,
        z_make_indexed_pairs,
    ).g


def make_grouped(
    name: str,
    t: type[T],
    index: Index[Pair[T, K], K],
) -> Graph[A1[ZSet[Pair[T, K]]], A1[Grouped[ZSet[T], K]]]:
    name = "make_grouped_" + name
    TGrouped = R[ZSet[T]].sub(T=t)

    def z_make_grouped(z: ZSet[Pair[T, K]]) -> Grouped[ZSet[T], K]:
        out = Grouped[ZSet[T], K](TGrouped, index.k)
        for key, inner in iter_by_index_grouped(z, index):
            out.set(key, ZSetPython[T]((value.left, count) for value, count in inner))
        return out

    return VertexUnary(
        R[Annotated[ZSet[Pair[T, K]], index]].sub(T=t, K=index.k),
        R[Grouped[ZSet[T], K]].sub(T=t, K=index.k),
        name,
        z_make_grouped,
    ).g


def pick_relevant(
    name: str,
    t: type[T],
    index: Index[Pair[T, K], K],
) -> Graph[A2[frozenset[K], ZSet[Pair[T, K]]], A1[ZSet[Pair[T, K]]]]:
    name = "pick_relevant_" + name

    def pick_relevant(
        keys: frozenset[K],
        z: ZSet[Pair[T, K]],
    ) -> ZSet[Pair[T, K]]:
        out = ZSetPython[Pair[T, K]](indexes=(index.generic,))
        out += ZSetPython(
            (value, count) for _, value, count in iter_by_index(z, index, tuple(keys))
        )
        return out

    return VertexBinary(
        R[frozenset[K]].sub(K=index.k),
        R[Annotated[ZSet[Pair[T, K]], index]].sub(T=t, K=index.k),
        R[Annotated[ZSet[Pair[T, K]], index]].sub(T=t, K=index.k),
        name,
        pick_relevant,
    ).g


def wrap_delay(
    name: str,
    t: type[T],
    k: type[K],
    first_vertex: VertexUnary[Any, Grouped[Any, K]],
) -> Graph[A1[Grouped[ZSet[T], K]], Grouped[ZSet[T], K]]:
    def get_keys(grouped: Grouped[Any, K]) -> frozenset[K]:
        return frozenset(key for key, _ in grouped.iter())

    t_grouped = get_annotation_grouped(first_vertex.v)
    gk = VertexUnary(
        R[Grouped[Any, K]].sub(Any=t_grouped, K=k),
        R[frozenset[K]].sub(K=k),
        "get_keys_" + name,
        get_keys,
    )

    TIndex = R[Pair[T, K]].sub(T=t, K=k)
    index = pick_index(TIndex, lambda p: p.right)

    new_delay = get_single_vertex(
        delay(name, R[Annotated[ZSet[Pair[T, K]], index]].sub(T=t, K=index.k))
    )
    i = get_single_vertex(make_indexed_pairs(name, t, index))
    p = get_single_vertex(pick_relevant(name, t, index))
    g = get_single_vertex(make_grouped(name, t, index))
    a1 = get_single_vertex(add(f"{name}_optimization_add1", new_delay.t))
    a2 = get_single_vertex(add(f"{name}_optimization_add2", new_delay.t))
    n = get_single_vertex(neg(f"{name}_optimization_neg", new_delay.t))

    return Graph[A1[Grouped[ZSet[T], K]], Grouped[ZSet[T], K]](
        vertices=[i, a1, new_delay, gk, p, a2, n, g],
        input=[(i, 0)],
        internal={
            (i, (a1, 1)),
            (a1, (new_delay, 0)),
            (first_vertex, (gk, 0)),
            (gk, (p, 0)),
            (new_delay, (p, 1)),
            (p, (n, 0)),
            (p, (g, 0)),
            (n, (a2, 1)),
            (new_delay, (a2, 0)),
            (a2, (a1, 0)),
        },
        output=[g],
    )
