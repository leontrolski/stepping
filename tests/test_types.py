from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import ANY

from stepping import types
from stepping.zset.sql import generic, postgres


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
    actual = types.pick_index(Cat, lambda c: (c.age, c.name, c.child.name))
    # fmt: on

    expected = types.Index[Cat, tuple[int, str, str]](
        ("age", "name", "child.name"), (True, True, True), ANY, Cat, tuple[int, str, str]  # type: ignore
    )
    assert actual == expected

    # TODO: add SQLite versions of these assertions
    actual_sql = postgres.to_expressions(actual)
    expected_sql = [
        "((data #>> '{age}')::int)",
        "((data #>> '{name}')::text)",
        "((data #>> '{child,name}')::text)",
    ]
    assert actual_sql == expected_sql


def test_pick_2() -> None:
    # fmt: off
    actual = types.pick_index(Cat,
                            lambda cat: (cat.age, cat.name, cat.child.name),
                ascending=(True, False, True)
                            )
    # fmt: on
    expected = types.Index[Cat, tuple[int, str, str]](
        ("age", "name", "child.name"), (True, False, True), ANY, Cat, tuple[int, str, str]  # type: ignore
    )
    assert actual == expected

    actual_sql = postgres.to_expressions(actual, include_asc=True)
    expected_sql = [
        "((data #>> '{age}')::int)",
        "((data #>> '{name}')::text) DESC",
        "((data #>> '{child,name}')::text)",
    ]
    assert actual_sql == expected_sql


def test_pick_3() -> None:
    actual = types.pick_identity(int, ascending=False)
    expected = types.Index[int, int]("", False, ANY, int, int)
    assert actual == expected

    l = list(generic.split_index_tuple_types(actual.fields, actual.ascending, actual.k))
    e = [("", int, False)]
    assert l == e

    actual_sql = postgres.to_expressions(actual)
    expected_sql = ["(data::int)"]
    assert actual_sql == expected_sql


def test_pick_4() -> None:
    actual = types.pick_index(Cat, lambda cat: cat.age)
    expected = types.Index[Cat, int]("age", True, ANY, Cat, int)
    assert actual == expected

    l = list(generic.split_index_tuple_types(actual.fields, actual.ascending, actual.k))
    e = [("age", int, True)]
    assert l == e


def test_pick_5() -> None:
    # fmt: off
    actual = types.pick_index(Cat,

                        lambda c: c.age
                        )
    # fmt: on
    expected = types.Index[Cat, int]("age", True, ANY, Cat, int)
    assert actual == expected


def test_pick_6() -> None:
    actual = types.pick_index(types.Pair[Cat, tuple[int]], lambda p: (p.right[0],))
    expected = types.Index[types.Pair[Cat, tuple[int]], tuple[int]](
        ("right.0",), (True,), ANY, types.Pair[Cat, tuple[int]], tuple[int]  # type: ignore
    )
    assert actual == expected

    l = list(generic.split_index_tuple_types(actual.fields, actual.ascending, actual.k))
    e = [("right.0", int, True)]
    assert l == e

    actual_sql = postgres.to_expressions(actual)
    expected_sql = [
        "((data #>> '{right,0}')::int)",
    ]
    assert actual_sql == expected_sql


def test_pick_7() -> None:
    actual = types.pick_index(types.Pair[int, tuple[str, float]], lambda p: p.right)
    expected = types.Index[types.Pair[int, tuple[str, float]], tuple[str, float]](
        "right", (True, True), ANY, types.Pair[int, tuple[str, float]], tuple[str, float]  # type: ignore
    )
    assert actual == expected

    l = list(generic.split_index_tuple_types(actual.fields, actual.ascending, actual.k))
    e = [("right.0", str, True), ("right.1", float, True)]
    assert l == e

    actual_sql = postgres.to_expressions(actual)
    expected_sql = [
        "((data #>> '{right,0}')::text)",
        "((data #>> '{right,1}')::double)",
    ]
    assert actual_sql == expected_sql
