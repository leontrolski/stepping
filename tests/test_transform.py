from typing import Any

import stepping.store
from stepping import operators, run
from stepping.graph import Graph, VertexBinary, VertexUnary, write_png
from stepping.operators.transform import lift_grouped, replace_vertex
from stepping.types import Grouped
from stepping.types import RuntimeComposite as R
from stepping.types import ZSet
from stepping.zset.python import ZSetPython


def add(a: ZSet[int], b: ZSet[int]) -> ZSet[int]:
    return a + b


def identity(n: ZSet[int]) -> ZSet[int]:
    return n


zset_type = R[ZSet[int]].sub()


def test_replace_vertex() -> None:
    a1 = VertexBinary(zset_type, zset_type, zset_type, "a1", f=add)
    d1 = VertexUnary(zset_type, zset_type, "d1", f=identity)
    e1 = VertexUnary(zset_type, zset_type, "e1", f=identity)
    f1 = VertexUnary(zset_type, zset_type, "f1", f=identity)
    g1 = VertexUnary(zset_type, zset_type, "g1", f=identity)
    graph = a1.g.connect(d1.g).connect(e1.g)
    actual = replace_vertex(graph, d1, f1.g.connect(g1.g))

    expected = Graph[Any, Any](
        vertices=[a1, e1, f1, g1],
        input=[(a1, 0), (a1, 1)],
        internal={
            (a1, (f1, 0)),
            (f1, (g1, 0)),
            (g1, (e1, 0)),
        },
        output=[e1],
    )
    assert actual == expected


def test_crazy_group_transform(request: Any) -> None:
    integrate = operators.integrate(int)
    g = lift_grouped(str, integrate)
    if request.config.getoption("--write-graphs"):
        write_png(
            g, "graphs/transform/test_crazy_group_transform.png", simplify_labels=False
        )

    def group(d: dict[str, int]) -> Grouped[int, str]:
        out = Grouped(int, str)
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
    reduce1 = operators.reduce(int, int, pick_reducable=lambda f: f)
    g = lift_grouped(str, reduce1)
    if request.config.getoption("--write-graphs"):
        write_png(g, "graphs/transform/test_transform_grouped_reduce.png")

    def group(d: dict[str, dict[int, int]]) -> Grouped[ZSet[int], str]:
        out = Grouped(R[ZSet[int]].sub(), str)
        for k, v in d.items():
            out.set(k, ZSetPython(v))
        return out

    store = stepping.store.StorePython.from_graph(g)

    (out,) = run.iteration(store, g, (group({"k1": {2: 1, 3: 1}, "k2": {4: 1}}),))
    assert out == group({"k1": {5: 1}, "k2": {4: 1}})

    (out,) = run.iteration(store, g, (group({"k1": {5: 1}}),))
    assert out == group({"k1": {10: 1, 5: -1}})
