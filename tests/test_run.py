from dataclasses import dataclass
from typing import Any, Callable

import stepping.store
import stepping.types
from stepping import operators, run
from stepping.graph import A1, Graph, stack, write_png
from stepping.types import EMPTY, Empty, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import ZSet, pick_identity, pick_index
from stepping.zset import iter_by_index
from stepping.zset.python import ZSetPython


def make_insert(
    store: stepping.types.Store, graph: Graph[A1[ZSet[int]], A1[ZSet[int]]]
) -> Callable[[int], ZSet[int]]:
    def inner(n: int) -> ZSet[int]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    return inner


def make_remove(
    store: stepping.types.Store, graph: Graph[A1[ZSet[int]], A1[ZSet[int]]]
) -> Callable[[int], ZSet[int]]:
    def inner(n: int) -> ZSet[int]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    return inner


def test_definition_3_27() -> None:
    integrate = operators.integrate_zset(int)
    store = stepping.store.StorePython.from_graph(integrate)
    insert = make_insert(store, integrate)

    actual = insert(8)
    assert actual == ZSetPython({8: 1})
    actual = insert(6)
    assert actual == ZSetPython({8: 1, 6: 1})
    actual = insert(4)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 1})
    actual = insert(5)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 1, 5: 1})
    actual = insert(4)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 2, 5: 1})


def test_definition_3_25() -> None:
    differentiate = operators.differentiate_zset(int)
    store = stepping.store.StorePython.from_graph(differentiate)
    insert = make_insert(store, differentiate)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({3: -1, 4: 1})
    actual = insert(5)
    assert actual == ZSetPython({4: -1, 5: 1})


def test_theorem_3_30() -> None:
    integrate = operators.integrate_zset(int)
    differentiate = operators.differentiate_zset(int)

    for graph in [
        integrate.connect(differentiate),
        differentiate.connect(integrate),
    ]:
        store = stepping.store.StorePython.from_graph(graph)
        insert = make_insert(store, graph)

        actual = insert(3)
        assert actual == ZSetPython({3: 1})
        actual = insert(4)
        assert actual == ZSetPython({4: 1})
        actual = insert(5)
        assert actual == ZSetPython({5: 1})


def test_integrate_and_delay() -> None:
    i = operators.integrate_zset(int)
    z = operators.delay(R[ZSet[int]].sub())
    graph = i.connect(z)
    store = stepping.store.StorePython.from_graph(graph)
    insert = make_insert(store, graph)

    actual = insert(8)
    assert actual == ZSetPython[int]()
    actual = insert(6)
    assert actual == ZSetPython({8: 1})
    actual = insert(6)
    assert actual == ZSetPython({8: 1, 6: 1})


def test_prop_6_3() -> None:
    distinct = operators.distinct(int)
    store = stepping.store.StorePython.from_graph(distinct)
    insert = make_insert(store, distinct)
    remove = make_remove(store, distinct)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(3)
    assert actual == ZSetPython[int]()
    actual = insert(4)
    assert actual == ZSetPython({4: 1})
    actual = remove(3)
    assert actual == ZSetPython[int]()
    actual = remove(3)
    assert actual == ZSetPython({3: -1})


def test_prop_6_3_integrate() -> None:
    graph = operators.distinct(int).connect(operators.integrate_zset(int))
    store = stepping.store.StorePython.from_graph(graph)
    insert = make_insert(store, graph)
    remove = make_remove(store, graph)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({3: 1, 4: 1})
    actual = remove(3)
    assert actual == ZSetPython({3: 1, 4: 1})
    actual = remove(3)
    assert actual == ZSetPython({4: 1})


@dataclass(frozen=True)
class Left:
    kind: str
    name: str
    sound_id: int


@dataclass(frozen=True)
class Right:
    sound_id: int
    sound: str


def test_join(request: Any) -> None:
    join = operators.finalize(
        operators.join(
            Left,
            Right,
            pick_index(Left, lambda l: l.sound_id),
            pick_index(Right, lambda r: r.sound_id),
        )
    )
    if request.config.getoption("--write-graphs"):
        write_png(join, "graphs/test_join.png")
    store = stepping.store.StorePython.from_graph(join)

    def insert_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, join, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, join, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, join, (ZSetPython[Left](), ZSetPython({n: 1})))
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


def test_outer_join(request: Any) -> None:
    join = operators.finalize(
        operators.outer_join(
            Left,
            Right,
            pick_index(Left, lambda l: l.sound_id),
            pick_index(Right, lambda r: r.sound_id),
        )
    )
    if request.config.getoption("--write-graphs"):
        write_png(join, "graphs/test_outer_join.png")
    store = stepping.store.StorePython.from_graph(join)

    def insert_left(n: Left) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = run.iteration(store, join, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = run.iteration(store, join, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = run.iteration(store, join, (ZSetPython[Left](), ZSetPython({n: 1})))
        return out

    def remove_right(n: Right) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = run.iteration(store, join, (ZSetPython[Left](), ZSetPython({n: -1})))
        return out

    expected: ZSetPython[Pair[Left, Right | Empty]]

    actual = insert_left(Left("cat", "felix", 1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): 1
        },
    )
    assert actual == expected

    actual = insert_left(Left("cat", "felix", 1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): 1
        },
    )
    assert actual == expected

    actual = insert_right(Right(2, "woof"))
    expected = ZSetPython[Pair[Left, Right | Empty]]()
    assert actual == expected

    actual = insert_right(Right(1, "miaow"))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): 2,
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): -2,
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

    actual = remove_right(Right(1, "miaow"))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): -2,
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): 2,
        },
    )
    assert actual == expected

    actual = remove_left(Left("cat", "felix", 1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): -1
        },
    )
    assert actual == expected


@dataclass(frozen=True)
class Product:
    name: str
    price: int


@dataclass(frozen=True)
class WithOne:
    value: int
    join_on: int = 1


def test_filter() -> None:
    query = operators.filter(int, lambda i: i > 3)
    store = stepping.store.StorePython.from_graph(query)
    (action,) = run.actions(store, query)

    (actual,) = action.insert([1])
    assert actual == ZSetPython[int]()

    (actual,) = action.insert([3, 4])
    assert actual == ZSetPython({4: 1})


def test_first_n(request: Any) -> None:
    index = pick_identity(int)
    integrate = operators.integrate_zset_indexed(int, index)
    query = operators.first_n(int, index, 3).connect(integrate)
    query = operators.finalize(query)
    if request.config.getoption("--write-graphs"):
        write_png(query, "graphs/test_first_n.png")
    store = stepping.store.StorePython.from_graph(query)
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


def test_sum(request: Any) -> None:
    sum = operators.sum(Product, int, lambda p: p.price)
    count = operators.count(Product)

    map1 = operators.map(int, WithOne, WithOne)
    map2 = operators.map(int, WithOne, WithOne)

    join = operators.join(
        WithOne,
        WithOne,
        pick_index(WithOne, lambda w: w.join_on),
        pick_index(WithOne, lambda w: w.join_on),
    )
    map3 = operators.map(
        Pair[WithOne, WithOne],
        tuple,
        lambda p: ("total", p.left.value, "count", p.right.value),
    )
    q = operators.finalize(
        operators.identity_zset(Product)
        .connect(stack(sum.connect(map1), count.connect(map2)))
        .connect(join)
        .connect(map3)
    )
    if request.config.getoption("--write-graphs"):
        write_png(q, "graphs/test_sum.png")
    store = stepping.store.StorePython.from_graph(q)

    def insert(n: Product) -> ZSet[Any]:
        (out,) = run.iteration(store, q, (ZSetPython({n: 1}),))
        return out

    def remove(n: Product) -> ZSet[Any]:
        (out,) = run.iteration(store, q, (ZSetPython({n: -1}),))
        return out

    actual = insert(Product("tv", 3))
    expected = ZSetPython[tuple[Any, ...]](
        {("total", 3, "count", 1): 1},
    )
    assert actual == expected

    actual = insert(Product("radio", 5))
    expected = ZSetPython(
        {
            ("total", 3, "count", 1): -1,
            ("total", 8, "count", 2): 1,
        },
    )
    assert actual == expected

    actual = insert(Product("radio", 5))
    expected = ZSetPython(
        {
            ("total", 8, "count", 2): -1,
            ("total", 13, "count", 3): 1,
        },
    )
    assert actual == expected

    actual = remove(Product("tv", 3))
    expected = ZSetPython(
        {
            ("total", 13, "count", 3): -1,
            ("total", 10, "count", 2): 1,
        },
    )
    assert actual == expected


@dataclass(frozen=True)
class WithLen:
    value: str
    length: int


def test_per(request: Any) -> None:
    index = pick_index(WithLen, lambda w: w.length)
    map1 = operators.map(str, WithLen, lambda s: WithLen(s, len(s)))
    group1 = operators.group(WithLen, index)
    map2 = operators.map(WithLen, str, lambda n: n.value.upper())
    map2_lifted = operators.lift_grouped(index.k, map2)
    flatten1 = operators.flatten(str, index.k)
    q = operators.finalize(map1.connect(group1).connect(map2_lifted).connect(flatten1))

    if request.config.getoption("--write-graphs"):
        write_png(q, "graphs/test_per.png")
    store = stepping.store.StorePython.from_graph(q)

    def insert(n: str) -> ZSet[Pair[int, str]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[int, str]]:
        (out,) = run.iteration(store, q, (ZSetPython({n: -1}),))
        return out

    actual = insert("cat")
    expected = ZSetPython({Pair(3, "CAT"): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(2, "CA"): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(3, "DOG"): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(3, "CAT"): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(3, "CAT"): -1})
    assert actual == expected


@dataclass(frozen=True)
class WithLenAndFirst:
    value: str
    length: int
    first: str


def test_nested_group(request: Any) -> None:
    add_keys = operators.map(
        str,
        WithLenAndFirst,
        lambda s: WithLenAndFirst(s, len(s), s[0]),
    )
    by_length = pick_index(WithLenAndFirst, lambda w: w.length)

    per_length = operators.group(WithLenAndFirst, by_length).connect(
        operators.lift_grouped(
            by_length.k,
            operators.map(WithLenAndFirst, str, lambda n: n.value.upper()),
        )
    )

    flatten = operators.flatten(str, by_length.k)
    integrate = operators.integrate_zset(Pair[int, str])
    q = operators.finalize(
        add_keys.connect(per_length).connect(flatten).connect(integrate)
    )

    if request.config.getoption("--write-graphs"):
        write_png(q, "graphs/test_nested_group.png")
    store = stepping.store.StorePython.from_graph(q)
    (action,) = run.actions(store, q)

    (actual,) = action.insert(["cat"])
    expected = ZSetPython({Pair(3, "CAT"): 1})
    assert actual == expected

    (actual,) = action.insert(["dog"])
    expected = ZSetPython(
        {
            Pair(3, "CAT"): 1,
            Pair(3, "DOG"): 1,
        }
    )
    assert actual == expected


@dataclass(frozen=True)
class WithLenAndZSet:
    value: str
    length: int
    zset: ZSetPython[str]


def test_group_by(request: Any) -> None:
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
    if request.config.getoption("--write-graphs"):
        write_png(q, "graphs/test_group_by.png", simplify_labels=False)
    store = stepping.store.StorePython.from_graph(q)

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
