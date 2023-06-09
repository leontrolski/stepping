import cProfile
from dataclasses import dataclass
from datetime import date
from random import randint

from stepping import types
from stepping.datatypes import sorted_set


def test_btree_basic_remove() -> None:
    index = types.pick_identity(int)
    s = sorted_set.SortedSet(index)

    s.add(1)
    s.remove(1)
    s.add(1)
    assert list(s) == [1]


def test_btree_basic() -> None:
    index = types.pick_identity(int)
    s = sorted_set.SortedSet(index)

    s.add(1)
    assert list(s) == [1]

    s.add(2)
    s.add(1)
    s.add(4)
    s.add(3)
    s.add(6)
    s.add(5)
    s.add(1)

    assert list(s) == [1, 2, 3, 4, 5, 6]
    assert list(s.iter_matching(types.MATCH_ALL)) == [1, 2, 3, 4, 5, 6]


def test_btree_basic_reversed() -> None:
    index = types.pick_identity(int, ascending=False)
    s = sorted_set.SortedSet(index)

    s.add(1)
    assert list(s) == [1]

    s.add(2)
    s.add(1)
    s.add(4)
    s.add(3)
    s.add(6)
    s.add(5)
    s.add(1)

    assert list(s) == [6, 5, 4, 3, 2, 1]
    assert list(s.iter_matching(types.MATCH_ALL)) == [6, 5, 4, 3, 2, 1]


def test_btree_basic_matching() -> None:
    index = types.pick_identity(int)
    s = sorted_set.SortedSet(index)

    for n in range(1000):
        s.add(n)

    assert list(s.iter_matching((0,))) == [0]
    assert list(s.iter_matching((500,))) == [500]
    assert list(s.iter_matching((999,))) == [999]
    assert list(s.iter_matching((3,))) == [3]
    assert list(s.iter_matching((800,))) == [800]
    assert list(s.iter_matching((1001,))) == []

    assert list(s.iter_matching((500, 600))) == [500, 600]
    assert list(s.iter_matching((600, 500))) == [500, 600]
    assert list(s.iter_matching((600, 200, 1001))) == [200, 600]
    assert list(s.iter_matching(())) == []


def test_btree_tuple_matching() -> None:
    index = types.pick_index(
        types.RuntimeComposite[tuple[int, int]].sub(), lambda t: t[1]
    )
    s = sorted_set.SortedSet(index)

    for n in range(40):
        s.add((n, n % 10))

    assert list(s.iter_matching((4, 3))) == [
        (3, 3),
        (13, 3),
        (23, 3),
        (33, 3),
        (4, 4),
        (14, 4),
        (24, 4),
        (34, 4),
    ]


@dataclass(frozen=True)
class Cat(types.Data):
    age: date | None
    name: str


def test_btree_complex() -> None:
    index = types.pick_index(Cat, lambda c: (c.age, c.name))
    s = sorted_set.SortedSet(index)

    _0 = Cat(None, "a")
    _1 = Cat(date(2000, 1, 1), "a")
    _2 = Cat(date(2000, 1, 2), "a")
    _3 = Cat(date(2000, 1, 3), "a")
    _4 = Cat(date(2000, 1, 3), "b")

    s.add(_1)
    assert list(s) == [_1]

    s.add(_2)
    s.add(_1)
    s.add(_0)
    s.add(_4)
    s.add(_3)
    s.add(_1)

    assert list(s) == [_0, _1, _2, _3, _4]

    s.remove(_1)
    assert list(s) == [_0, _2, _3, _4]


def test_btree_complex_reverse() -> None:
    assert not sorted_set.lt((2, 8), (1, 8), (True, False))
    assert sorted_set.lt((1, 3), (2, 4), (True, True))
    assert sorted_set.lt((1, 3), (2, 4), (True, False))
    assert sorted_set.lt((1, 3), (1, 4), (True, True))
    assert sorted_set.lt((1, 4), (1, 3), (True, False))
    assert not sorted_set.lt((1, 3), (1, 4), (True, False))
    assert sorted_set.lt(
        (date(2000, 1, 3), "b"), (date(2000, 1, 3), "a"), (True, False)
    )
    assert sorted_set.lt(
        (date(2000, 1, 1), "a"), (date(2000, 1, 2), "a"), (True, False)
    )
    assert not sorted_set.lt(
        (date(2000, 1, 2), "a"), (date(2000, 1, 1), "a"), (True, False)
    )

    index = types.pick_index(Cat, lambda c: (c.age, c.name), ascending=(True, False))
    s = sorted_set.SortedSet(index)

    _0 = Cat(None, "a")
    _1 = Cat(date(2000, 1, 1), "a")
    _2 = Cat(date(2000, 1, 2), "a")
    _3 = Cat(date(2000, 1, 3), "b")
    _4 = Cat(date(2000, 1, 3), "a")

    s.add(_1)
    assert list(s) == [_1]

    s.add(_2)
    s.add(_1)
    s.add(_0)
    s.add(_4)
    s.add(_3)
    s.add(_1)

    assert list(s) == [_0, _1, _2, _3, _4]

    s.remove(_1)
    assert list(s) == [_0, _2, _3, _4]


def test_btree_profile() -> None:
    index = types.pick_identity(int)
    s = sorted_set.SortedSet(index)

    rs = [randint(1, 1_000_000) for _ in range(100_000)]
    for r in rs:
        s.add(r)
    rs = [randint(1, 1_000_000) for _ in range(100)]
    with cProfile.Profile() as pr:
        for r in rs:
            s.add(r)
    pr.dump_stats("btree.prof")
    print(s)
