from typing import Any, Callable

import stepping as st
import stepping.store
from stepping import run
from stepping.graph import (
    Graph,
    OperatorKind,
    Path,
    VertexBinary,
    VertexUnary,
    write_png,
)
from stepping.operators import builder
from stepping.operators.transform import lift_grouped, replace_vertex
from stepping.types import Grouped, Signature, ZSet
from stepping.zset.python import ZSetPython


def add(a: ZSet[int], b: ZSet[int]) -> ZSet[int]:
    return a + b


def identity(n: ZSet[int]) -> ZSet[int]:
    return n


zset_type = ZSet[int]


def p(s: str) -> Path:
    return Path((s,))


def connect(a: Graph[Any, Any], b: Graph[Any, Any]) -> Graph[Any, Any]:
    if len(a.output) == 1:
        internal = {(a.output[0], b.input[i]) for i in range(len(b.input))}
    else:
        internal = {(a.output[i], b.input[i]) for i in range(len(a.output))}
    return Graph(
        vertices=a.vertices + b.vertices,
        input=a.input,
        internal=internal | a.internal | b.internal,
        output=b.output,
        run_no_output=b.run_no_output,
    )


def test_replace_vertex() -> None:
    a1 = VertexBinary(zset_type, zset_type, zset_type, OperatorKind.add, p("a1"), f=add)  # type: ignore[type-abstract]
    d1 = VertexUnary(zset_type, zset_type, OperatorKind.add, p("d1"), f=identity)  # type: ignore[type-abstract]
    e1 = VertexUnary(zset_type, zset_type, OperatorKind.add, p("e1"), f=identity)  # type: ignore[type-abstract]
    f1 = VertexUnary(zset_type, zset_type, OperatorKind.add, p("f1"), f=identity)  # type: ignore[type-abstract]
    g1 = VertexUnary(zset_type, zset_type, OperatorKind.add, p("g1"), f=identity)  # type: ignore[type-abstract]
    graph = connect(connect(a1.as_graph, d1.as_graph), e1.as_graph)
    actual = replace_vertex(graph, d1, connect(f1.as_graph, g1.as_graph))

    expected = Graph[Any, Any](
        vertices=[a1, e1, f1, g1],
        input=[(a1, 0), (a1, 1)],
        internal={
            (a1, (f1, 0)),
            (f1, (g1, 0)),
            (g1, (e1, 0)),
        },
        output=[e1],
        run_no_output=[],
    )
    assert actual == expected


def test_crazy_group_transform(request: Any) -> None:
    integrate = builder.compile_generic(
        st.integrate,
        {},
        Signature([("a", str)], {}, str),
        Path(),
    )
    g = lift_grouped(str, integrate)

    if request.config.getoption("--write-graphs"):
        write_png(g, "graphs/test_crazy_group_transform.png", simplify_labels=False)

    def group(d: dict[str, int]) -> Grouped[int, str]:
        out = Grouped[int, str]()
        for k, v in d.items():
            out.set(k, v)
        return out

    store = stepping.store.StorePython.from_graph(g)
    (out,) = run.iteration(store, g, (group({"k1": 2, "k2": 4}),))
    assert out == group({"k1": 2, "k2": 4})

    (out,) = run.iteration(store, g, (group({"k1": 3, "k3": 8}),))
    assert out == group({"k1": 5, "k3": 8})

    (out,) = run.iteration(store, g, (group({"k2": 2}),))
    assert out == group({"k2": 6})


def test_transform_grouped_reduce(request: Any) -> None:
    reduce2 = builder.compile_generic(
        st.reduce,
        {"zero": int, "pick_value": lambda f: f},
        Signature(
            [("a", ZSet[int])],
            {"zero": Callable[[], int], "pick_value": Callable[[int], int]},  # type: ignore
            str,
        ),
        Path(),
    )
    g = lift_grouped(str, reduce2)
    if request.config.getoption("--write-graphs"):
        write_png(g, "graphs/test_transform_grouped_reduce.png")

    def group(d: dict[str, dict[int, int]]) -> Grouped[ZSet[int], str]:
        out = Grouped[ZSet[int], str]()
        for k, v in d.items():
            out.set(k, ZSetPython(v))
        return out

    store = stepping.store.StorePython.from_graph(g)

    (out,) = run.iteration(store, g, (group({"k1": {2: 1, 3: 1}, "k2": {4: 1}}),))
    assert out == group({"k1": {5: 1}, "k2": {4: 1}})

    (out,) = run.iteration(store, g, (group({"k1": {5: 1}}),))
    assert out == group({"k1": {10: 1, 5: -1}})
