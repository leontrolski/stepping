from dataclasses import dataclass
from typing import Any

import stepping.store
from stepping import operators, run
from stepping.graph import write_png
from stepping.types import Data, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import ZSet, pick_identity, pick_index
from stepping.zset import iter_by_index, postgres
from stepping.zset.python import ZSetPython


@dataclass(frozen=True)
class Left(Data):
    kind: str
    name: str
    sound_id: int


@dataclass(frozen=True)
class Right(Data):
    sound_id: int
    sound: str


def test_integrate(conn: postgres.Conn, request: Any) -> None:
    integrate = operators.integrate(R[ZSet[Left]].sub())
    if request.config.getoption("--write-graphs"):
        write_png(integrate, "graphs/sql/test_integrate.png")
    graph = integrate

    store = stepping.store.StorePostgres.from_graph(conn, graph, "test_integrate")

    def insert_left(n: Left) -> ZSet[Left]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}),))
        return ZSetPython(out.iter())

    actual = insert_left(Left("dog", "fido", 2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 1},
    )
    assert actual == expected

    actual = insert_left(Left("dog", "fido", 2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 2},
    )
    assert actual == expected

    actual = insert_left(Left("dog", "fido", 2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 3},
    )
    assert actual == expected


def test_join(conn: postgres.Conn) -> None:
    join = operators.join(
        Left,
        Right,
        pick_index(Left, lambda l: l.sound_id),
        pick_index(Right, lambda r: r.sound_id),
    )
    graph = operators.finalize(join)
    store = stepping.store.StorePostgres.from_graph(conn, graph, "test_join")

    def insert_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython[Left](), ZSetPython({n: 1})))
        return out

    insert_left(Left("cat", "felix", 1))
    insert_left(Left("cat", "felix", 1))

    actual = insert_right(Right(2, "woof"))
    expected = ZSetPython[Pair[Left, Right]]()
    assert actual == expected

    actual = insert_right(Right(1, "miaow"))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): 2
        },
    )
    assert actual == expected

    actual = insert_left(Left("dog", "fido", 2))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="dog", name="fido", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1
        },
    )
    assert actual == expected

    actual = remove_left(Left("cat", "felix", 1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): -1
        },
    )
    assert actual == expected


@dataclass(frozen=True)
class WithLenAndWeight:
    value: str
    length: int
    weight: int = 1


def test_group_by(conn: postgres.Conn, request: Any) -> None:
    index = pick_index(WithLenAndWeight, lambda w: w.length)
    group1 = operators.group(WithLenAndWeight, index)
    reduce1 = operators.reduce(WithLenAndWeight, int, pick_reducable=lambda w: w.weight)
    reduce1_lifted = operators.lift_grouped(index.k, reduce1)
    flatten1 = operators.flatten(int, index.k)

    q = operators.finalize(
        operators.map(
            str,
            WithLenAndWeight,
            lambda s: WithLenAndWeight(s, len(s)),
        )
        .connect(group1.connect(reduce1_lifted).connect(flatten1))
        .connect(
            operators.integrate_zset(
                Pair[int, int],
            )
        )
    )
    if request.config.getoption("--write-graphs"):
        write_png(q, "graphs/sql/test_group_by.png")
    store = stepping.store.StorePostgres.from_graph(conn, q, "test_group_by")

    def insert(n: str) -> ZSet[Pair[int, int]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[int, int]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: -1}),))
        return out

    expected: ZSet[Pair[int, int]]

    actual = insert("cat")
    expected = ZSetPython({Pair(3, 1): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(3, 1): 1, Pair(2, 1): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(3, 2): 1, Pair(2, 1): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(3, 3): 1, Pair(2, 1): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(3, 2): 1, Pair(2, 1): 1})
    assert actual == expected


def test_first_n(conn: postgres.Conn) -> None:
    index = pick_identity(int)
    integrate = operators.integrate_zset_indexed(int, index)
    query = operators.first_n(int, index, 3).connect(integrate)
    store = stepping.store.StorePostgres.from_graph(conn, query, "test_first_n")
    (action,) = run.actions(store, query)

    def insert(n: int) -> list[int]:
        (z,) = action.insert([n])
        return [
            v for _, value, count in iter_by_index(z, index) for v in [value] * count
        ]

    def remove(n: int) -> list[int]:
        (z,) = action.remove([n])
        return [
            v for _, value, count in iter_by_index(z, index) for v in [value] * count
        ]

    actual = insert(1)
    assert actual == [1]

    actual = insert(2)
    assert actual == [1, 2]

    actual = insert(5)
    assert actual == [1, 2, 5]

    actual = insert(4)
    assert actual == [1, 2, 4]

    actual = insert(1)
    assert actual == [1, 1, 2]

    actual = insert(-1)
    assert actual == [-1, 1, 1]

    actual = remove(1)
    assert actual == [-1, 1, 2]


@dataclass(frozen=True)
class WithLenAndZSet:
    value: str
    length: int
    zset: ZSetPython[str]


def test_group_by_zset(conn: postgres.Conn) -> None:
    index = pick_index(WithLenAndZSet, lambda w: w.length)
    group1 = operators.group(WithLenAndZSet, index)
    reduce1 = operators.reduce(
        WithLenAndZSet, ZSetPython[str], pick_reducable=lambda w: w.zset
    )
    reduce1_lifted = operators.lift_grouped(index.k, reduce1)
    flatten1 = operators.flatten(ZSetPython[str], index.k)

    q = operators.finalize(
        operators.map(
            str,
            WithLenAndZSet,
            lambda s: WithLenAndZSet(s, len(s), ZSetPython({s: 1})),
        )
        .connect(group1.connect(reduce1_lifted).connect(flatten1))
        .connect(operators.integrate_zset(Pair[int, ZSetPython[str]]))
    )
    store = stepping.store.StorePostgres.from_graph(conn, q, "test_group_by")

    def insert(n: str) -> ZSet[Pair[int, ZSetPython[str]]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[int, ZSetPython[str]]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: -1}),))
        return out

    expected: ZSet[Pair[int, ZSetPython[str]]]

    def z(*vs: str) -> ZSetPython[str]:
        out = ZSetPython[str]()
        for v in vs:
            out += ZSetPython({v: 1})
        return out

    actual = insert("cat")
    expected = ZSetPython({Pair(3, z("cat")): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(3, z("cat")): 1, Pair(2, z("ca")): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(3, z("cat", "dog")): 1, Pair(2, z("ca")): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(3, z("cat", "dog", "cat")): 1, Pair(2, z("ca")): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(3, z("cat", "dog")): 1, Pair(2, z("ca")): 1})
    assert actual == expected
