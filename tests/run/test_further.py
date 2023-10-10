from dataclasses import dataclass
from typing import Annotated, Any

import pytest

import stepping as st
from stepping import run
from stepping.graph import write_png
from stepping.types import EMPTY, Empty, Index, Pair, ZSet
from stepping.zset.python import ZSetPython
from tests.conftest import Conns
from tests.helpers import StoreMaker, store_ids, store_makers


class Left(st.Data):
    kind: str
    name: str
    sound_id: int


class Right(st.Data):
    sound_id: int
    sound: str


def _f_test_join(l: ZSet[Left], r: ZSet[Right]) -> ZSet[Pair[Left, Right]]:
    joined = st.join(
        l,
        r,
        on_left=Index.pick(Left, lambda l: l.sound_id),
        on_right=Index.pick(Right, lambda r: r.sound_id),
    )
    return joined


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_join(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_join)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_join.png")

    def insert_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right]]:
        (out,) = st.iteration(store, graph, (ZSetPython[Left](), ZSetPython({n: 1})))
        return out

    insert_left(Left(kind="cat", name="felix", sound_id=1))
    insert_left(Left(kind="cat", name="felix", sound_id=1))

    actual = insert_right(Right(sound_id=2, sound="woof"))
    expected = ZSetPython[Pair[Left, Right]]()
    assert actual == expected

    actual = insert_right(Right(sound_id=1, sound="miaow"))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): 2
        },
    )
    assert actual == expected

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="dog", name="fido", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1
        },
    )
    assert actual == expected

    actual = remove_left(Left(kind="cat", name="felix", sound_id=1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): -1
        },
    )
    assert actual == expected


def _f_test_outer_join(
    l: ZSet[Left], r: ZSet[Right]
) -> ZSet[Pair[Left, Right | Empty]]:
    joined = st.outer_join(
        l,
        r,
        on_left=Index.pick(Left, lambda l: l.sound_id),
        on_right=Index.pick(Right, lambda r: r.sound_id),
    )
    return joined


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_outer_join(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_outer_join)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_outer_join.png")

    def insert_left(n: Left) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = st.iteration(store, graph, (ZSetPython[Left](), ZSetPython({n: 1})))
        return out

    def remove_right(n: Right) -> ZSet[Pair[Left, Right | Empty]]:
        (out,) = st.iteration(store, graph, (ZSetPython[Left](), ZSetPython({n: -1})))
        return out

    expected: ZSetPython[Pair[Left, Right | Empty]]

    actual = insert_left(Left(kind="cat", name="felix", sound_id=1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): 1
        },
    )
    assert actual == expected

    actual = insert_left(Left(kind="cat", name="felix", sound_id=1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): 1
        },
    )
    assert actual == expected

    actual = insert_right(Right(sound_id=2, sound="woof"))
    expected = ZSetPython[Pair[Left, Right | Empty]]()
    assert actual == expected

    actual = insert_right(Right(sound_id=1, sound="miaow"))
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

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="dog", name="fido", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1
        },
    )
    assert actual == expected

    actual = remove_right(Right(sound_id=1, sound="miaow"))
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

    actual = remove_left(Left(kind="cat", name="felix", sound_id=1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=EMPTY,
            ): -1
        },
    )
    assert actual == expected


def _gt_3(i: int) -> bool:
    return i > 3


def _f_test_filter(a: ZSet[int]) -> ZSet[int]:
    filtered = st.filter(a, f=_gt_3)
    return filtered


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_filter(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_filter)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_filter.png")

    (action,) = run.actions(store, graph)

    (actual,) = action.insert(1)
    assert actual == ZSetPython[int]()

    (actual,) = action.insert(3, 4)
    assert actual == ZSetPython({4: 1})


def _f_test_first_n(a: ZSet[int]) -> ZSet[int]:
    first_n_ed = st.first_n(a, index=Index.identity(int), n=3)
    integrated = st.integrate_indexed(first_n_ed, indexes=(Index.identity(int),))
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_first_n(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    index = Index.identity(int)
    graph, store = store_maker(conns, _f_test_first_n)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_first_n.png")
    (action,) = run.actions(store, graph)

    def insert(n: int) -> list[int]:
        (z,) = action.insert(n)
        return [
            v for _, value, count in z.iter_by_index(index) for v in [value] * count
        ]

    def remove(n: int) -> list[int]:
        (z,) = action.remove(n)
        return [
            v for _, value, count in z.iter_by_index(index) for v in [value] * count
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


class Product(st.Data):
    name: str
    price: int


def _flatten(p: Pair[int, int]) -> tuple[str, int, str, int]:
    return "total", p.left, "count", p.right


def _pick_price(p: Product) -> int:
    return p.price


index_joined = st.Index.atom("one", Pair[int, int], int, lambda p: 1)
cache_joined = st.Cache[Pair[int, int]]()


def _f_test_sum(a: ZSet[Product]) -> ZSet[tuple[str, int, str, int]]:
    summed = st.reduce(a, zero=int, pick_value=_pick_price)
    counted = st.count(a)
    joined = st.join(
        summed,
        counted,
        on_left=Index.atom("left", int, int, lambda n: 1),
        on_right=Index.atom("right", int, int, lambda n: 1),
    )
    _ = cache_joined[joined](lambda j: st.integrate_indexed(j, indexes=(index_joined,)))
    flattened = st.map(joined, f=_flatten)
    return flattened


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_sum(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_sum)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_sum.png")

    def insert(n: Product) -> ZSet[Any]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    def remove(n: Product) -> ZSet[Any]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    actual = insert(Product(name="tv", price=3))
    expected = ZSetPython[tuple[Any, ...]](
        {("total", 3, "count", 1): 1},
    )
    assert actual == expected

    actual = insert(Product(name="radio", price=5))
    expected = ZSetPython(
        {
            ("total", 3, "count", 1): -1,
            ("total", 8, "count", 2): 1,
        },
    )
    assert actual == expected

    actual = insert(Product(name="radio", price=5))
    expected = ZSetPython(
        {
            ("total", 8, "count", 2): -1,
            ("total", 13, "count", 3): 1,
        },
    )
    assert actual == expected

    actual = remove(Product(name="tv", price=3))
    expected = ZSetPython(
        {
            ("total", 13, "count", 3): -1,
            ("total", 10, "count", 2): 1,
        },
    )
    assert actual == expected

    actual_cached = list(
        cache_joined.zset(store).iter_by_index(index_joined, frozenset((1,)))
    )
    assert actual_cached == [
        (
            1,
            Pair(left=10, right=2),
            1,
        )
    ]


def _upper(n: str) -> str:
    return n.upper()


def _f_test_group(a: ZSet[str]) -> ZSet[Pair[str, int]]:
    grouped = st.group(a, by=st.Index.atom("len", str, int, lambda n: len(n)))
    mapped_2 = st.per_group[grouped](lambda g: st.map(g, f=_upper))
    flattened = st.flatten(mapped_2)
    return flattened


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_group_reduce_flatten(
    request: Any, conns: Conns, store_maker: StoreMaker
) -> None:
    graph, store = store_maker(conns, _f_test_group)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_group_reduce_flatten.png")

    def insert(n: str) -> ZSet[Pair[str, int]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[str, int]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    actual = insert("cat")
    expected = ZSetPython({Pair("CAT", 3): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair("CA", 2): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair("DOG", 3): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair("CAT", 3): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair("CAT", 3): -1})
    assert actual == expected


class WithLenAndFirst(st.Data):
    value: str
    length: int
    first: str


def _with_len_and_first(s: str) -> WithLenAndFirst:
    return WithLenAndFirst(value=s, length=len(s), first=s[0])


def _upper2(w: WithLenAndFirst) -> str:
    return w.value.upper()


def _f_test_nested_group(a: ZSet[str]) -> ZSet[Pair[str, int]]:
    keys_added = st.map(a, f=_with_len_and_first)
    grouped = st.group(keys_added, by=Index.pick(WithLenAndFirst, lambda w: w.length))
    uppered = st.per_group[grouped](lambda g: st.map(g, f=_upper2))
    flattened = st.flatten(uppered)
    integrated = st.integrate(flattened)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_nested_group(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_nested_group)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_nested_group.png")
    (action,) = run.actions(store, graph)

    (actual,) = action.insert("cat")
    expected = ZSetPython({Pair("CAT", 3): 1})
    assert actual == expected

    (actual,) = action.insert("dog")
    expected = ZSetPython(
        {
            Pair("CAT", 3): 1,
            Pair("DOG", 3): 1,
        }
    )
    assert actual == expected


class WithLenAndZSet(st.Data):
    value: str
    length: int
    zset: ZSetPython[str]


def _with_len_and_zset(s: str) -> WithLenAndZSet:
    return WithLenAndZSet(value=s, length=len(s), zset=ZSetPython({s: 1}))


def zset_zero() -> ZSetPython[str]:
    return ZSetPython[str]()


def pick_zset(w: WithLenAndZSet) -> ZSetPython[str]:
    return w.zset


def _f_test_group_by(a: ZSet[str]) -> ZSet[Pair[ZSetPython[str], int]]:
    keys_added = st.map(a, f=_with_len_and_zset)
    grouped = st.group(keys_added, by=Index.pick(WithLenAndZSet, lambda w: w.length))
    reduced = st.per_group[grouped](
        lambda g: st.reduce(g, zero=zset_zero, pick_value=pick_zset)
    )
    flattened = st.flatten(reduced)
    integrated = st.integrate(flattened)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_group_by(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_group_by)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_group_by.png", simplify_labels=True)

    def insert(n: str) -> ZSet[Pair[ZSetPython[str], int]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[ZSetPython[str], int]]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    expected: ZSet[Pair[ZSetPython[str], int]]

    def z(*vs: str) -> ZSetPython[str]:
        out = ZSetPython[str]()
        for v in vs:
            out += ZSetPython({v: 1})
        return out

    actual = insert("cat")
    expected = ZSetPython({Pair(z("cat"), 3): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(z("cat"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(z("cat", "dog"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(z("cat", "dog", "cat"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(z("cat", "dog"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected


def _f_test_integrate_2(a: ZSet[Left]) -> ZSet[Left]:
    integrated = st.integrate(a)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_integrate_2(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_integrate_2)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_integrate_2.png")

    def insert_left(n: Left) -> ZSet[Left]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}),))
        return ZSetPython(out.iter())

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 1},
    )
    assert actual == expected

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 2},
    )
    assert actual == expected

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {Left(kind="dog", name="fido", sound_id=2): 3},
    )
    assert actual == expected


def _f_test_join_2(l: ZSet[Left], r: ZSet[Right]) -> ZSet[Pair[Left, Right]]:
    joined = st.join(
        l,
        r,
        on_left=Index.pick(Left, lambda l: l.sound_id),
        on_right=Index.pick(Right, lambda r: r.sound_id),
    )
    return joined


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_join_2(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_join_2)

    def insert_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}), ZSetPython[Right]()))
        return out

    def remove_left(n: Left) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: -1}), ZSetPython[Right]()))
        return out

    def insert_right(n: Right) -> ZSet[Pair[Left, Right]]:
        (out,) = run.iteration(store, graph, (ZSetPython[Left](), ZSetPython({n: 1})))
        return out

    insert_left(Left(kind="cat", name="felix", sound_id=1))
    insert_left(Left(kind="cat", name="felix", sound_id=1))

    actual = insert_right(Right(sound_id=2, sound="woof"))
    expected = ZSetPython[Pair[Left, Right]]()
    assert actual == expected

    actual = insert_right(Right(sound_id=1, sound="miaow"))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): 2
        },
    )
    assert actual == expected

    actual = insert_left(Left(kind="dog", name="fido", sound_id=2))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="dog", name="fido", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1
        },
    )
    assert actual == expected

    actual = remove_left(Left(kind="cat", name="felix", sound_id=1))
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="miaow"),
            ): -1
        },
    )
    assert actual == expected


class WithLenAndWeight(st.Data):
    value: str
    length: int
    weight: int = 1


def _with_len_and_weight(s: str) -> WithLenAndWeight:
    return WithLenAndWeight(value=s, length=len(s))


def _pick_weight(w: WithLenAndWeight) -> int:
    return w.weight


def _f_test_group_2(a: ZSet[str]) -> ZSet[Pair[int, int]]:
    mapped = st.map(a, f=_with_len_and_weight)
    grouped = st.group(mapped, by=Index.pick(WithLenAndWeight, lambda w: w.length))
    mapped_2 = st.per_group[grouped](
        lambda g: st.reduce(g, zero=int, pick_value=_pick_weight)
    )
    flattened = st.flatten(mapped_2)
    integrated = st.integrate(flattened)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_group_by_2(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_group_2)
    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_group_by_2.png")

    def insert(n: str) -> ZSet[Pair[int, int]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[int, int]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    expected: ZSet[Pair[int, int]]

    actual = insert("cat")
    expected = ZSetPython({Pair(1, 3): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(1, 3): 1, Pair(1, 2): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(2, 3): 1, Pair(1, 2): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(3, 3): 1, Pair(1, 2): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(2, 3): 1, Pair(1, 2): 1})
    assert actual == expected


def _f_test_first_n_2(a: ZSet[int]) -> ZSet[int]:
    first_n_ed = st.first_n(a, index=Index.identity(int), n=3)
    integrated = st.integrate_indexed(first_n_ed, indexes=(Index.identity(int),))
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_first_n_2(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_first_n_2)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_first_n_2.png")

    (action,) = run.actions(store, graph)

    index = Index.identity(int)

    def insert(n: int) -> list[int]:
        (z,) = action.insert(n)
        return [
            v for _, value, count in z.iter_by_index(index) for v in [value] * count
        ]

    def remove(n: int) -> list[int]:
        (z,) = action.remove(n)
        return [
            v for _, value, count in z.iter_by_index(index) for v in [value] * count
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


def _f_test_group_by_zset_2(a: ZSet[str]) -> ZSet[Pair[ZSetPython[str], int]]:
    keys_added = st.map(a, f=_with_len_and_zset)
    grouped = st.group(keys_added, by=Index.pick(WithLenAndZSet, lambda w: w.length))
    reduced = st.per_group[grouped](
        lambda g: st.reduce(g, zero=zset_zero, pick_value=pick_zset)
    )
    flattened = st.flatten(reduced)
    integrated = st.integrate(flattened)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_group_by_zset_2(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_group_by_zset_2)

    if request.config.getoption("--write-graphs"):
        write_png(graph, "graphs/test_group_by_zset_2.png")

    def insert(n: str) -> ZSet[Pair[ZSetPython[str], int]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    def remove(n: str) -> ZSet[Pair[ZSetPython[str], int]]:
        (out,) = run.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    expected: ZSet[Pair[ZSetPython[str], int]]

    def z(*vs: str) -> ZSetPython[str]:
        out = ZSetPython[str]()
        for v in vs:
            out += ZSetPython({v: 1})
        return out

    actual = insert("cat")
    expected = ZSetPython({Pair(z("cat"), 3): 1})
    assert actual == expected

    actual = insert("ca")
    expected = ZSetPython({Pair(z("cat"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = insert("dog")
    expected = ZSetPython({Pair(z("cat", "dog"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = insert("cat")
    expected = ZSetPython({Pair(z("cat", "dog", "cat"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected

    actual = remove("cat")
    expected = ZSetPython({Pair(z("cat", "dog"), 3): 1, Pair(z("ca"), 2): 1})
    assert actual == expected
