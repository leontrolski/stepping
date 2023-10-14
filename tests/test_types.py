from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import ANY

from stepping import types


@dataclass
class Cat:
    name: str
    age: int
    child: Cat


def test_proxy() -> None:
    p = types.Proxy(Cat)
    assert p.t is Cat
    assert p._path == ()

    q = p.age
    assert q.t is int
    assert q._path == ("age",)

    r = p.child.name
    assert r.t is str
    assert r._path == ("child", "name")


def test_pick_1() -> None:
    # fmt: off
    actual = types.Index.pick(Cat, lambda c: (c.age, c.name, c.child.name))
    # fmt: on

    expected = types.Index[Cat, tuple[int, str, str]](
        ("age", "name", "child.name"), (True, True, True), ANY, Cat, tuple[int, str, str], True  # type: ignore
    )
    assert actual == expected


def test_pick_2() -> None:
    # fmt: off
    actual = types.Index.pick(Cat,
                            lambda cat: (cat.age, cat.name, cat.child.name),
                ascending=(True, False, True)
                            )
    # fmt: on
    expected = types.Index[Cat, tuple[int, str, str]](
        ("age", "name", "child.name"), (True, False, True), ANY, Cat, tuple[int, str, str], True  # type: ignore
    )
    assert actual == expected


def test_pick_3() -> None:
    actual = types.Index.identity(int, ascending=False)
    expected = types.Index[int, int](("identity",), (False,), ANY, int, int, False)
    assert actual == expected


def test_pick_4() -> None:
    actual = types.Index.pick(Cat, lambda cat: cat.age)
    expected = types.Index[Cat, int](("age",), (True,), ANY, Cat, int, False)
    assert actual == expected


def test_pick_5() -> None:
    # fmt: off
    actual = types.Index.pick(Cat,

                        lambda c: c.age
                        )
    # fmt: on
    expected = types.Index[Cat, int](("age",), (True,), ANY, Cat, int, False)
    assert actual == expected


def test_pick_6() -> None:
    actual = types.Index.pick(types.Pair[Cat, tuple[int]], lambda p: (p.right[0],))
    expected = types.Index[types.Pair[Cat, tuple[int]], tuple[int]](
        ("right.0",), (True,), ANY, types.Pair[Cat, tuple[int]], tuple[int], True  # type: ignore
    )
    assert actual == expected


def test_pick_7() -> None:
    actual = types.Index.pick(types.Pair[int, tuple[str, float]], lambda p: p.right)
    expected = types.Index[types.Pair[int, tuple[str, float]], tuple[str, float]](
        ("right.0", "right.1"), (True, True), ANY, types.Pair[int, tuple[str, float]], tuple[str, float], True  # type: ignore
    )
    assert actual == expected
