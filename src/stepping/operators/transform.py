from copy import deepcopy
from typing import Any, Callable, cast

from stepping.graph import (
    A1,
    Graph,
    Port,
    Vertex,
    VertexBinary,
    VertexKind,
    VertexUnary,
)
from stepping.operators import linear
from stepping.operators.group import wrap_delay
from stepping.types import Empty, Grouped, Indexable, K
from stepping.types import RuntimeComposite as R
from stepping.types import T, U, V, X, Y, ZSet, get_annotation_grouped_zset, is_type


def replace_vertex(
    graph: Graph[T, V],
    remove: VertexUnary[X, Y],
    replacement: Graph[A1[X], A1[Y]],
) -> Graph[T, V]:
    assert remove.identifier not in [vertex.identifier for vertex, _ in graph.input]
    assert remove.identifier not in [vertex.identifier for vertex in graph.output]
    vertices = [vertex for vertex in graph.vertices if vertex != remove]
    assert len(replacement.input) == 1
    assert len(replacement.output) == 1
    (new_end_port,) = replacement.input
    (new_start,) = replacement.output

    internal = {vp for vp in graph.internal}
    from_vertices = set[Vertex]()
    to_ports = set[Port]()
    for start, [end, i] in graph.internal:
        if end == remove:
            internal -= {(start, (end, i))}
            from_vertices.add(start)
        if start == remove:
            internal -= {(start, (end, i))}
            to_ports.add((end, i))

    for start in from_vertices:
        internal |= {(start, new_end_port)}
    for end_port in to_ports:
        internal |= {(new_start, end_port)}

    return Graph(
        vertices=vertices + replacement.vertices,
        input=graph.input,
        internal=internal | replacement.internal,
        output=graph.output,
    )


def remove_identities(graph: Graph[T, V]) -> Graph[T, V]:
    internal = {vp for vp in graph.internal}

    remove: Vertex | None = None
    for _, [end, __] in graph.internal:
        if end.kind is VertexKind.IDENTITY:
            remove = end

    from_vertices = set[Vertex]()
    to_ports = set[Port]()
    for start, [end, i] in graph.internal:
        if end == remove:
            internal -= {(start, (end, i))}
            from_vertices.add(start)
        if start == remove:
            internal -= {(start, (end, i))}
            to_ports.add((end, i))

    for start in from_vertices:
        for end_port in to_ports:
            internal |= {(start, end_port)}

    vertices = [vertex for vertex in graph.vertices if vertex != remove]

    # We could do dataclasses.replace(...) here, but we don't want to
    # repeat the type checking in __post_init__.
    graph = deepcopy(graph)
    graph.internal = internal
    graph.vertices = vertices
    return graph


def _lift_function_grouped_unary(
    v: type[V],
    k: type[K],
    f: Callable[[T], V],
) -> Callable[[Grouped[T, K]], Grouped[V, K]]:
    TGrouped = R[V].sub(V=v)

    def inner(a_grouped: Grouped[T, K]) -> Grouped[V, K]:
        out = Grouped[V, K](TGrouped, k)
        keys = a_grouped.keys()
        for key in keys:
            a = a_grouped.get(key)
            if isinstance(a, Empty):
                raise RuntimeError(f"{key} not in group")
            out.set(key, f(a))
        return out

    return inner


def _lift_add_grouped_binary(
    v: type[V], k: type[K], f: Callable[[T, U], V]
) -> Callable[[Grouped[T, K], Grouped[U, K]], Grouped[V, K]]:
    TGrouped = R[V].sub(V=v)

    def inner(a_grouped: Grouped[T, K], b_grouped: Grouped[U, K]) -> Grouped[V, K]:
        out = Grouped[V, K](TGrouped, k)
        keys = a_grouped.keys() | b_grouped.keys()
        for key in keys:
            a = a_grouped.get(key)
            b = b_grouped.get(key)
            # we don't know 0 + 0
            if isinstance(a, Empty) and isinstance(b, Empty):
                raise RuntimeError(f"{key} not in group")
            # we know      0 + b  == b
            # we know join({}, b) == {}
            if isinstance(a, Empty):
                out.set(key, b)  # type: ignore
            # we know      a + 0  == a
            # we know join(a, {}) == {}
            elif isinstance(b, Empty):
                out.set(key, a)  # type: ignore
            else:
                out.set(key, f(a, b))
        return out

    return inner


def lift_grouped(
    k: type[K],
    g: Graph[A1[T], A1[V]],
) -> Graph[A1[Grouped[T, K]], A1[Grouped[V, K]]]:
    g = replace_non_zset_delays(g)

    g = deepcopy(g)
    for vertex in g.vertices:
        if isinstance(vertex, VertexUnary):
            vertex.f = _lift_function_grouped_unary(vertex.v, k, vertex.f)
            vertex.t = R[Grouped[Any, Indexable]].sub(Any=vertex.t, Indexable=k)
            vertex.v = R[Grouped[Any, Indexable]].sub(Any=vertex.v, Indexable=k)
        elif isinstance(vertex, VertexBinary):
            if vertex.kind is not VertexKind.ADD:
                raise RuntimeError("Can only lift ADD binary vertices to grouped")
            vertex.f = _lift_add_grouped_binary(vertex.v, k, vertex.f)
            vertex.t = R[Grouped[Any, Indexable]].sub(Any=vertex.t, Indexable=k)
            vertex.u = R[Grouped[Any, Indexable]].sub(Any=vertex.u, Indexable=k)
            vertex.v = R[Grouped[Any, Indexable]].sub(Any=vertex.v, Indexable=k)

    g = replace_grouped_delays(g)
    return g  # type: ignore


def replace_non_zset_delays(g: Graph[T, V]) -> Graph[T, V]:
    for vertex in g.vertices:
        if not (isinstance(vertex, VertexUnary) and vertex.kind is VertexKind.DELAY):
            continue
        assert vertex.t == vertex.v
        if is_type(vertex.t, ZSet) or is_type(vertex.t, Grouped):
            continue
        make_set = linear.make_set("_" + vertex.identifier, vertex.v)
        new_delay = linear.delay(
            "_" + vertex.identifier, R[ZSet[Any]].sub(Any=vertex.v)
        )
        make_scalar = linear.make_scalar("_" + vertex.identifier, vertex.v)
        g = replace_vertex(g, vertex, make_set.connect(new_delay).connect(make_scalar))

    return g


def replace_grouped_delays(g: Graph[T, V]) -> Graph[T, V]:
    first_vertex, _ = g.input[0]
    first_vertex = cast(VertexUnary[Any, Grouped[Any, Any]], first_vertex)

    for vertex in g.vertices:
        if not (isinstance(vertex, VertexUnary) and vertex.kind is VertexKind.DELAY):
            continue
        assert vertex.t == vertex.v
        if not is_type(vertex.t, Grouped):
            continue
        t, k = get_annotation_grouped_zset(vertex.t)

        _, __, name = vertex.identifier.partition("__")
        replacement = wrap_delay("_" + name, t, k, first_vertex)
        g = replace_vertex(g, vertex, replacement)  # type: ignore
    return g


def finalize(g: Graph[T, V]) -> Graph[T, V]:
    g = replace_non_zset_delays(g)
    g = til_stable(remove_identities)(g)
    return g


# For some reason, couldn't get f: Callable[[T], T] to work
def til_stable(f: T) -> T:
    def inner(g):  # type: ignore
        prev = g
        for _ in range(999):  # give up after a bit
            g = f(g)  # type: ignore
            if g == prev:
                break
            prev = g
        return g

    return inner  # type: ignore
