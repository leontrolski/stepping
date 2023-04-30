from typing import Any

from stepping.graph import Graph, VertexBinary, VertexUnary, stack
from stepping.types import RuntimeComposite as R
from stepping.types import ZSet


def add(a: ZSet[int], b: ZSet[int]) -> ZSet[int]:
    return a + b


def identity(n: ZSet[int]) -> ZSet[int]:
    return n


zset_type = R[ZSet[int]].sub()


def test_graph_syntax() -> None:
    a1 = VertexBinary(zset_type, zset_type, zset_type, "a1", f=add)
    d1 = VertexUnary(zset_type, zset_type, "d1", f=identity)
    actual = a1.g.connect(d1.g)
    expected = Graph[Any, Any](
        vertices=[a1, d1],
        input=[(a1, 0), (a1, 1)],
        internal={(a1, (d1, 0))},
        output=[d1],
    )
    assert actual == expected

    d1 = VertexUnary(zset_type, zset_type, "d1", f=identity)
    d2 = VertexUnary(zset_type, zset_type, "d2", f=identity)
    d3 = VertexUnary(zset_type, zset_type, "d3", f=identity)
    a1 = VertexBinary(zset_type, zset_type, zset_type, "a1", f=add)
    d4 = VertexUnary(zset_type, zset_type, "d4", f=identity)
    actual = stack(d1.g, d2.g.connect(d3.g)).connect(a1.g).connect(d4.g)
    expected = Graph(
        vertices=[d1, d2, d3, a1, d4],
        input=[(d1, 0), (d2, 0)],
        internal={
            (d1, (a1, 0)),
            (d2, (d3, 0)),
            (d3, (a1, 1)),
            (a1, (d4, 0)),
        },
        output=[d4],
    )
    assert actual == expected


def test_typing() -> None:
    x = VertexUnary(zset_type, zset_type, "x", identity).g
    y = VertexUnary(zset_type, zset_type, "y", identity).g
    # try plugging in z not a and you get an error
    z = VertexUnary(zset_type, zset_type, "z", identity).g
    a = VertexBinary(zset_type, zset_type, zset_type, "z", lambda a, b: a + b).g
    g = stack(x, y).connect(a)
