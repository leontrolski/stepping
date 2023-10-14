from __future__ import annotations

import enum
import hashlib
import pathlib
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, get_args

from stepping import types

# fmt: off
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
X = TypeVar("X")
Y = TypeVar("Y")
Z = TypeVar("Z")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
U1 = TypeVar("U1")
U2 = TypeVar("U2")
U3 = TypeVar("U3")
U4 = TypeVar("U4")
X1 = TypeVar("X1")
X2 = TypeVar("X2")
X3 = TypeVar("X3")
Y1 = TypeVar("Y1")
Y2 = TypeVar("Y2")
Y3 = TypeVar("Y3")
class A1(Generic[T1]): ...
class A2(Generic[T1, T2]): ...
class A3(Generic[T1, T2, T3]): ...
class A4(Generic[T1, T2, T3, T4]): ...
# Maybe in the future, we can use:
# TT = TypeVarTuple("TT")
# class A(Generic[Unpack[TT]]): ...
# fmt: on


@dataclass(frozen=True)
class Path:
    inner: tuple[str, ...] = ()

    def __truediv__(self, other: str | Path) -> Path:
        if isinstance(other, Path):
            return Path(self.inner + other.inner)
        return Path(self.inner + (other,))

    def __repr__(self) -> str:
        return f"<Path ...{self.inner[-1]}>"

    def __str__(self) -> str:
        middle = list(self.inner)
        if len(middle) % 2 == 0:
            middle = [
                middle[i * 2][0] + ":" + middle[i * 2 + 1]
                for i in range(len(middle) // 2)
            ]
        else:
            middle = [n[0] for n in middle]
        return "/".join(middle)


class OperatorKind(enum.Enum):
    add = "add"
    delay = "delay"
    filter = "filter"
    first_n = "first_n"
    haitch = "haitch"
    identity = "identity"
    identity_dont_remove = "identity_dont_remove"
    join = "join"
    make_scalar = "make_scalar"
    make_set = "make_set"
    map = "map"
    map_many = "map_many"
    neg = "neg"
    reduce = "reduce"
    # group
    flatten = "flatten"
    get_keys = "get_keys"
    group = "group"
    make_grouped = "make_grouped"
    make_indexed_pairs = "make_indexed_pairs"
    pick_relevant = "pick_relevant"
    # recursive
    integrate_til_zero = "integrate_til_zero"


@dataclass(frozen=True)
class VertexUnary(Generic[T, V]):
    t: type[T]
    v: type[V]
    operator_kind: OperatorKind
    path: Path
    f: Callable[[T], V]

    @property
    def as_graph(self) -> Graph[A1[T], A1[V]]:
        return Graph(
            vertices={self.path: self},
            input=[(self.path, 0)],
            internal=set(),
            output=[self.path],
            run_no_output=[],
        )

    def __hash__(self) -> int:
        return _hash_vertex(self)

    def __repr__(self) -> str:
        return _repr_vertex(self)


@dataclass(frozen=True)
class VertexUnaryDelay(VertexUnary[T, V]):
    indexes: tuple[types.Index[T, Any], ...]

    def __hash__(self) -> int:
        return _hash_vertex(self)

    def __repr__(self) -> str:
        return _repr_vertex(self)


@dataclass(frozen=True)
class VertexUnaryIntegrateTilZero(VertexUnary[T, V]):
    graph: Graph[A1[T], A1[V]]

    def __hash__(self) -> int:
        return _hash_vertex(self)

    def __repr__(self) -> str:
        return _repr_vertex(self)


@dataclass(frozen=True)
class VertexBinary(Generic[T, U, V]):
    t: type[T]
    u: type[U]
    v: type[V]
    operator_kind: OperatorKind
    path: Path
    f: Callable[[T, U], V]

    @property
    def as_graph(self) -> Graph[A2[T, U], A1[V]]:
        return Graph(
            vertices={self.path: self},
            input=[(self.path, 0), (self.path, 1)],
            internal=set(),
            output=[self.path],
            run_no_output=[],
        )

    def __hash__(self) -> int:
        return _hash_vertex(self)

    def __repr__(self) -> str:
        return _repr_vertex(self)


Vertex = VertexUnary | VertexBinary
Port = tuple[Path, int]


@dataclass
class Graph(Generic[T, V]):
    vertices: dict[Path, Vertex]
    input: list[Port]  # in -> a
    internal: set[tuple[Path, Port]]  # a -> b
    output: list[Path]  # b -> out
    run_no_output: list[Path]  # b -> nowhere

    def __post_init__(self) -> None:
        # validate unique vertex identifiers
        vertex_identifiers: dict[tuple[OperatorKind, Path], int] = defaultdict(int)
        for vertex in self.vertices.values():
            vertex_identifiers[(vertex.operator_kind, vertex.path)] += 1
        for identifier, count in vertex_identifiers.items():
            if count != 1:
                raise RuntimeError(
                    f"Saw vertices with duplicate identifier: {identifier}"
                )
        # validate runtime types match
        for start_p, [end_p, i] in self.internal:
            start, end = self.vertices[start_p], self.vertices[end_p]
            start_type = start.v
            end_type = end.t
            if i == 1:
                assert isinstance(end, VertexBinary)
                end_type = end.u
            if start_type != end_type:
                raise RuntimeError(
                    f"start.v {start_p}:\n{start_type}\ndoesn't match\n"
                    f"end.{'u' if i == 1 else 't'} {end_p}:\n{end_type}"
                )

    @property
    def delay_vertices(self) -> list[VertexUnaryDelay[Any, Any]]:
        return [v for v in self.vertices.values() if isinstance(v, VertexUnaryDelay)]


def _hash_vertex(self: Vertex) -> int:
    return hash((self.operator_kind, self.path))


def _repr_vertex(self: Vertex) -> str:
    if isinstance(self, VertexUnary):
        return (
            f"<Vertex {self.operator_kind.name} "
            f"({munge_type_name(self.t)}) -> "
            f"{munge_type_name(self.v)}>"
        )
    return (
        f"<Vertex {self.operator_kind.name} "
        f"({munge_type_name(self.t)}, {munge_type_name(self.u)}) -> "
        f"{munge_type_name(self.v)}>"
    )


# .png helpers


def _dot_identity(vertex: Vertex) -> str:
    return "_".join(vertex.path.inner)


def _level(vertex: Vertex, n: int) -> str | None:
    if len(vertex.path.inner) < n:
        return None
    return "__".join(vertex.path.inner[:n])


def munge_type_name(t: type) -> str:
    if not get_args(t):
        return t.__name__
    return re.sub(r"\w+\.", r"", repr(t))


def _hash(v: Vertex | OperatorKind) -> int:
    if isinstance(v, OperatorKind):
        s = v.value.encode()
    else:
        s = "\t".join(v.path.inner).encode()
    md5 = hashlib.md5()
    md5.update(s)
    return int.from_bytes(md5.digest())


def write_png(
    graph: Graph[Any, Any],
    path: str,
    simplify_labels: bool = True,
    level: int = 2,
) -> None:
    dir = pathlib.Path(path).parent
    dir.mkdir(exist_ok=True, parents=True)

    import pydot

    g = pydot.Dot(
        graph_type="digraph",
        # rankdir="LR",
        texmode="math",
    )

    def to_color(v: Vertex) -> str:
        colors = [
            "coral",
            "deepskyblue",
            "gold",
            "cyan",
            "cornflowerblue",
            "silver",
            "darkorange",
            "darkgoldenrod",
            "aqua",
            "burlywood",
            "lavender",
            "khaki",
            "plum",
        ]
        return colors[_hash(v.operator_kind) % len(colors)]

    level_1s = {
        l: pydot.Cluster(
            f"subgraph1_{l}",
            label="/".join(str(n) for n in vertex.path.inner[:level]),
            fillcolor="beige",
            style="filled",
        )
        for vertex in graph.vertices.values()
        if (l := _level(vertex, level)) is not None
    }

    for vertex in graph.vertices.values():
        label = str(vertex)
        if simplify_labels:
            label = vertex.operator_kind.value
        node = pydot.Node(
            _dot_identity(vertex),
            fillcolor=to_color(vertex),
            style="filled",
            label=label,
        )
        g.add_node(node)
        subgraph = level_1s.get(_level(vertex, level) or "")
        if subgraph is not None:
            subgraph.add_node(node)

    for subgraph in level_1s.values():
        g.add_subgraph(subgraph)

    for i, [p, _] in enumerate(graph.input):
        vertex = graph.vertices[p]
        g.add_node(pydot.Node(f"input__{i}", fillcolor="red", style="filled"))
        g.add_edge(
            pydot.Edge(
                f"input__{i}",
                _dot_identity(vertex),
                label=munge_type_name(vertex.t),
            )
        )
    for i, p in enumerate(graph.output):
        vertex = graph.vertices[p]
        g.add_node(pydot.Node(f"output__{i}", fillcolor="red", style="filled"))
        g.add_edge(
            pydot.Edge(
                _dot_identity(vertex),
                f"output__{i}",
                label=munge_type_name(vertex.v),
            )
        )
    for start_p, [end_p, i] in graph.internal:
        start = graph.vertices[start_p]
        end = graph.vertices[end_p]
        g.add_edge(
            pydot.Edge(
                _dot_identity(start),
                _dot_identity(end),
                label=(f"[{i}] " if i > 0 else "") + munge_type_name(start.v),
            )
        )

    g.write_png(path)
