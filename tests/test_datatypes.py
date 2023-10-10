import cProfile
from dataclasses import dataclass
from datetime import date
from random import randint

from stepping import steppingpack, types
from stepping.datatypes import _btree, sorted_set


def test_btree_basic_remove() -> None:
    index = types.Index.identity(int)
    s = sorted_set.SortedSet(index)

    s = s.add(1)
    s = s.remove(1)
    s = s.add(1)
    assert list(s) == [1]


def test_btree_basic() -> None:
    index = types.Index.identity(int)
    s = sorted_set.SortedSet(index)

    s = s.add(1)
    assert list(s) == [1]

    s = s.add(2)
    s = s.add(1)
    s = s.add(4)
    s = s.add(3)
    s = s.add(6)
    s = s.add(5)
    s = s.add(1)

    assert list(s) == [1, 2, 3, 4, 5, 6]
    assert list(s.iter_matching(types.MATCH_ALL)) == [1, 2, 3, 4, 5, 6]


def test_btree_basic_reversed() -> None:
    index = types.Index.identity(int, ascending=False)
    s = sorted_set.SortedSet(index)

    s = s.add(1)
    assert list(s) == [1]

    s = s.add(2)
    s = s.add(1)
    s = s.add(4)
    s = s.add(3)
    s = s.add(6)
    s = s.add(5)
    s = s.add(1)

    assert list(s) == [6, 5, 4, 3, 2, 1]
    assert list(s.iter_matching(types.MATCH_ALL)) == [6, 5, 4, 3, 2, 1]


def test_btree_basic_matching() -> None:
    index = types.Index.identity(int)
    s = sorted_set.SortedSet(index)

    for n in range(1000):
        s = s.add(n)

    assert list(s.iter_matching(frozenset((0,)))) == [0]
    assert list(s.iter_matching(frozenset((500,)))) == [500]
    assert list(s.iter_matching(frozenset((999,)))) == [999]
    assert list(s.iter_matching(frozenset((3,)))) == [3]
    assert list(s.iter_matching(frozenset((800,)))) == [800]
    assert list(s.iter_matching(frozenset((1001,)))) == []

    assert list(s.iter_matching(frozenset((500, 600)))) == [500, 600]
    assert list(s.iter_matching(frozenset((600, 500)))) == [500, 600]
    assert list(s.iter_matching(frozenset((600, 200, 1001)))) == [200, 600]
    assert list(s.iter_matching(frozenset(()))) == []


def test_btree_tuple_matching() -> None:
    index = types.Index.pick(tuple[int, int], lambda t: t[1])  # type: ignore
    s = sorted_set.SortedSet(index)  # type: ignore

    for n in range(40):
        s = s.add((n, n % 10))  # type: ignore

    assert list(s.iter_matching((4, 3))) == [  # type: ignore
        (3, 3),
        (13, 3),
        (23, 3),
        (33, 3),
        (4, 4),
        (14, 4),
        (24, 4),
        (34, 4),
    ]


class Cat(steppingpack.Data):
    age: date | None
    name: str


def test_btree_complex() -> None:
    index = types.Index.pick(Cat, lambda c: (c.age, c.name))
    s = sorted_set.SortedSet(index)

    _0 = Cat(age=None, name="a")
    _1 = Cat(age=date(2000, 1, 1), name="a")
    _2 = Cat(age=date(2000, 1, 2), name="a")
    _3 = Cat(age=date(2000, 1, 3), name="a")
    _4 = Cat(age=date(2000, 1, 3), name="b")

    s = s.add(_1)
    assert list(s) == [_1]

    s = s.add(_2)
    s = s.add(_1)
    s = s.add(_0)
    s = s.add(_4)
    s = s.add(_3)
    s = s.add(_1)

    assert list(s) == [_0, _1, _2, _3, _4]

    s = s.remove(_1)
    assert list(s) == [_0, _2, _3, _4]


def test_btree_complex_reverse() -> None:
    assert not _btree.lt((2, 8), (1, 8), (True, False))
    assert _btree.lt((1, 3), (2, 4), (True, True))
    assert _btree.lt((1, 3), (2, 4), (True, False))
    assert _btree.lt((1, 3), (1, 4), (True, True))
    assert _btree.lt((1, 4), (1, 3), (True, False))
    assert not _btree.lt((1, 3), (1, 4), (True, False))
    assert _btree.lt((date(2000, 1, 3), "b"), (date(2000, 1, 3), "a"), (True, False))
    assert _btree.lt((date(2000, 1, 1), "a"), (date(2000, 1, 2), "a"), (True, False))
    assert not _btree.lt(
        (date(2000, 1, 2), "a"), (date(2000, 1, 1), "a"), (True, False)
    )

    index = types.Index.pick(Cat, lambda c: (c.age, c.name), ascending=(True, False))
    s = sorted_set.SortedSet(index)

    _0 = Cat(age=None, name="a")
    _1 = Cat(age=date(2000, 1, 1), name="a")
    _2 = Cat(age=date(2000, 1, 2), name="a")
    _3 = Cat(age=date(2000, 1, 3), name="b")
    _4 = Cat(age=date(2000, 1, 3), name="a")

    s = s.add(_1)
    assert list(s) == [_1]

    s = s.add(_2)
    s = s.add(_1)
    s = s.add(_0)
    s = s.add(_4)
    s = s.add(_3)
    s = s.add(_1)

    assert list(s) == [_0, _1, _2, _3, _4]

    s = s.remove(_1)
    assert list(s) == [_0, _2, _3, _4]


def test_btree_profile() -> None:
    index = types.Index.identity(int)
    s = sorted_set.SortedSet(index)

    rs = [randint(1, 1_000_000) for _ in range(100_000)]
    for r in rs:
        s = s.add(r)
    rs = [randint(1, 1_000_000) for _ in range(100)]
    with cProfile.Profile() as pr:
        for r in rs:
            s = s.add(r)
    pr.dump_stats("test_btree_profile.prof")
    print(s)
