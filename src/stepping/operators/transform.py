from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Generic, cast, get_args, get_origin, overload

from stepping import types
from stepping.graph import (
    A1,
    A2,
    Graph,
    OperatorKind,
    Port,
    Vertex,
    VertexBinary,
    VertexUnary,
    VertexUnaryDelay,
    VertexUnaryIntegrateTilZero,
)
from stepping.operators import builder, group, linear
from stepping.types import (
    Empty,
    Grouped,
    K,
    Store,
    T,
    U,
    V,
    X,
    Y,
    Z,
    ZSet,
    get_annotation_grouped_zset,
    is_type,
)


# fmt: off
@overload
def replace_vertex(
    graph: Graph[T, V],
    remove: VertexUnary[X, Y],
    replacement: Graph[A1[X], A1[Y]],
) -> Graph[T, V]: ...
@overload
def replace_vertex(
    graph: Graph[T, V],
    remove: VertexBinary[X, Y, Z],
    replacement: Graph[A2[X, Y], A1[Z]],
) -> Graph[T, V]: ...
# fmt: on
def replace_vertex(
    graph: Graph[T, V],
    remove: VertexUnary[X, Y] | VertexBinary[X, Y, Z],
    replacement: Graph[A1[X], A1[Y]] | Graph[A2[X, Y], A1[Z]],
) -> Graph[T, V]:
    assert remove.path not in [vertex.path for vertex, _ in graph.input]
    assert remove.path not in [vertex.path for vertex in graph.output]
    vertices = [vertex for vertex in graph.vertices if vertex != remove]
    if isinstance(remove, VertexUnary):
        assert len(replacement.input) == 1
    if isinstance(remove, VertexBinary):
        assert len(replacement.input) == 2
    assert len(replacement.output) == 1
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
        for new_end_port in replacement.input:
            internal |= {(start, new_end_port)}
    for end_port in to_ports:
        internal |= {(new_start, end_port)}

    return Graph(
        vertices=vertices + replacement.vertices,
        input=graph.input,
        internal=internal | replacement.internal,
        output=graph.output,
        run_no_output=graph.run_no_output,
    )


def remove_identities(graph: Graph[T, V]) -> Graph[T, V]:
    internal = {vp for vp in graph.internal}
    input = {vertex for vertex, _ in graph.input}

    remove: Vertex | None = None
    for vertex in graph.vertices:
        if vertex in graph.output or vertex in input:
            continue
        if vertex.operator_kind is OperatorKind.identity:
            remove = vertex

    if remove is None:
        return graph

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

    # assert from_vertices and to_ports

    vertices = [vertex for vertex in graph.vertices if vertex != remove]

    # We could do dataclasses.replace(...) here, but we don't want to
    # repeat the type checking in __post_init__.
    graph = deepcopy(graph)
    graph.internal = internal
    graph.vertices = vertices
    return graph


@dataclass
class LiftFunctionGroupedUnary(Generic[T, V, K]):
    f: Callable[[T], V]

    def __call__(self, a_grouped: Grouped[T, K]) -> Grouped[V, K]:
        out = Grouped[V, K]()
        keys = a_grouped.keys()
        for key in keys:
            a = a_grouped.get(key)
            if isinstance(a, Empty):
                raise RuntimeError(f"{key} not in group")
            out.set(key, self.f(a))
        return out


@dataclass
class LiftFunctionGroupedBinary(Generic[T, U, V, K]):
    f: Callable[[T, U], V]

    def __call__(
        self, a_grouped: Grouped[T, K], b_grouped: Grouped[U, K]
    ) -> Grouped[V, K]:
        out = Grouped[V, K]()
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
                out.set(key, self.f(a, b))
        return out


def lift_grouped(
    k: type[K],
    g: Graph[A1[T], A1[V]],
) -> Graph[A1[Grouped[T, K]], A1[Grouped[V, K]]]:
    g = replace_non_zset_delays(g)

    g = deepcopy(g)
    for vertex in g.vertices:
        if isinstance(vertex, VertexUnary):
            vertex.f = LiftFunctionGroupedUnary(vertex.f)
            vertex.t = Grouped[vertex.t, k]  # type: ignore
            vertex.v = Grouped[vertex.v, k]  # type: ignore
        elif isinstance(vertex, VertexBinary):
            if vertex.operator_kind is not OperatorKind.add:
                raise RuntimeError("Can only lift ADD binary vertices to grouped")
            vertex.f = LiftFunctionGroupedBinary(vertex.f)
            vertex.t = Grouped[vertex.t, k]  # type: ignore
            vertex.u = Grouped[vertex.u, k]  # type: ignore
            vertex.v = Grouped[vertex.v, k]  # type: ignore

    g = replace_grouped_delays(g)
    return g  # type: ignore


def _set_and_back(a: T, *, zero: Callable[[], T]) -> T:
    setted = linear.make_set(a)
    delayed = linear.delay(setted)
    scalared = linear.make_scalar(delayed, zero=zero)
    return scalared


def replace_non_zset_delays(g: Graph[T, V]) -> Graph[T, V]:
    for vertex in g.vertices:
        if not vertex.operator_kind is OperatorKind.delay:
            continue
        assert isinstance(vertex, VertexUnary)
        if is_type(vertex.t, ZSet) or is_type(vertex.t, Grouped):
            continue
        new_g = builder.compile_generic(
            _set_and_back,
            {"zero": vertex.t},
            types.Signature(
                [("a", vertex.t)],
                {"zero": Callable[[], vertex.t]},  # type: ignore
                vertex.t,
            ),
            path=vertex.path / "replace_non_zset_delays" / "new_g",
        )
        g = replace_vertex(g, vertex, new_g)

    return g


def replace_grouped_delays(g: Graph[T, V]) -> Graph[T, V]:
    first_vertex, _ = g.input[0]
    first_vertex = cast(VertexUnary[Any, Grouped[Any, Any]], first_vertex)

    for vertex in g.vertices:
        if not (
            isinstance(vertex, VertexUnary)
            and vertex.operator_kind is OperatorKind.delay
        ):
            continue
        assert vertex.t == vertex.v
        if not is_type(vertex.t, Grouped):
            continue
        t, k = get_annotation_grouped_zset(vertex.t)

        replacement = group.wrap_delay(
            vertex.path / "replace_grouped_delays" / "replacement",
            t,
            k,
            first_vertex,
        )
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


@dataclass
class GroupTransformer:
    k: type

    def lift(self, ret: type) -> type:
        return Grouped[ret, self.k]  # type: ignore

    def unlift(self, arg: type) -> type:
        assert get_origin(arg) is Grouped
        t, _ = get_args(arg)
        return t  # type: ignore

    def transform(self, g: Graph[Any, Any]) -> Graph[Any, Any]:
        return lift_grouped(self.k, g)


class PerGroup:
    def from_arg_types(self, args: list[tuple[str, type]]) -> GroupTransformer:
        assert len(args) == 1
        _, arg = args[0]
        assert get_origin(arg) is Grouped
        _, k = get_args(arg)
        return GroupTransformer(k)

    def __getitem__(
        self, key: Grouped[T, K]
    ) -> Callable[[Callable[[T], V]], Grouped[V, K]]:
        raise NotImplementedError()


per_group = PerGroup()


@dataclass
class IntegrateTilZeroTransformer:
    def lift(self, ret: type) -> type:
        return ret

    def unlift(self, arg: type) -> type:
        return arg

    def transform(self, g: Graph[Any, Any]) -> Graph[Any, Any]:
        # Add an extra identity to the start that won't get removed by `remove_identities`
        assert len(g.input) == 1
        [[input_vertex, i]] = g.input
        extra_identity_vertex = VertexUnary(
            input_vertex.t,
            input_vertex.t,
            OperatorKind.identity_dont_remove,
            input_vertex.path / "integrate_til_zero_identity",
            lambda a: a,
        )
        g = deepcopy(g)
        g.input = [(extra_identity_vertex, 0)]
        g.vertices.append(extra_identity_vertex)
        g.internal.add((extra_identity_vertex, (input_vertex, i)))

        # Add an extra vertex to the end that refers to the graph we were passed in
        assert len(g.output) == 1
        output_vertex = g.output[0]
        integrate_til_zero_vertex = VertexUnaryIntegrateTilZero(
            output_vertex.v,
            output_vertex.v,
            OperatorKind.integrate_til_zero,
            output_vertex.path / "integrate_til_zero",
            lambda a: a,
            graph=g,
        )
        g = deepcopy(g)
        g.input = [(extra_identity_vertex, 0)]
        g.vertices.append(integrate_til_zero_vertex)
        g.internal.add((output_vertex, (integrate_til_zero_vertex, 0)))
        g.output = [integrate_til_zero_vertex]
        return g


class IntegrateTilZero:
    def from_arg_types(
        self, args: list[tuple[str, type]]
    ) -> IntegrateTilZeroTransformer:
        return IntegrateTilZeroTransformer()

    def __getitem__(
        self, key: ZSet[T]
    ) -> Callable[[Callable[[ZSet[T]], ZSet[V]]], ZSet[V]]:
        raise NotImplementedError()


integrate_til_zero = IntegrateTilZero()


@dataclass
class CacheTransformer(Generic[T]):
    cache: Cache[T]

    def lift(self, ret: type) -> type:
        return ret

    def unlift(self, arg: type) -> type:
        return arg

    def transform(self, g: Graph[Any, Any]) -> Graph[Any, Any]:
        g = deepcopy(g)
        vertices_delay = [v for v in g.vertices if isinstance(v, VertexUnaryDelay)]
        if len(vertices_delay) != 1:
            raise RuntimeError("Graph contains more than one delay vertex")
        if self.cache.vertex_delay is not None and hash(
            self.cache.vertex_delay
        ) != hash(vertices_delay[0]):
            raise RuntimeError("Cache already has a different delay vertex registered")
        self.cache.vertex_delay = vertices_delay[0]
        g.run_no_output.append(self.cache.vertex_delay)
        return g


class Cache(Generic[T]):
    def zset(self, store: Store) -> ZSet[T]:
        if self.vertex_delay is None:
            raise RuntimeError(
                "Cache is missing delay vertex, "
                "probably compile the func referencing the cache"
            )
        zset = store.get(self.vertex_delay)
        return zset

    # Used at func compile time

    vertex_delay: VertexUnaryDelay[Any, ZSet[T]] | None = None

    def from_arg_types(self, args: list[tuple[str, type]]) -> CacheTransformer[T]:
        return CacheTransformer(self)

    def __getitem__(
        self, key: ZSet[T]
    ) -> Callable[[Callable[[ZSet[T]], ZSet[T]]], ZSet[T]]:
        raise NotImplementedError()
