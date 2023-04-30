from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from stepping import types
from stepping.types import T
from stepping.zset import postgres, python


@dataclass
class Cat:
    name: str
    age: int
    child: Cat


def test_pick_1() -> None:
    # fmt: off
    actual = types.pick_index(Cat, lambda c: (c.age, c.name, c.child.name))
    # fmt: on

    expected = types.Index[Cat, tuple[int, str, str]](
        ("age", "name", "child.name"), (True, True, True), tuple[int, str, str]  # type: ignore
    )
    assert actual == expected

    actual_sql = postgres.to_postgres_expressions(actual)  # type: ignore
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
        ("age", "name", "child.name"), (True, False, True), tuple[int, str, str]  # type: ignore
    )
    assert actual == expected

    actual_sql = postgres.to_postgres_expressions(actual, include_asc=True)  # type: ignore
    expected_sql = [
        "((data #>> '{age}')::int)",
        "((data #>> '{name}')::text) DESC",
        "((data #>> '{child,name}')::text)",
    ]
    assert actual_sql == expected_sql


def test_pick_3() -> None:
    actual = types.pick_identity(int, ascending=False)
    expected = types.Index[int, int]("", False, int)
    assert actual == expected

    l = list(
        postgres.split_index_tuple_types(actual.fields, actual.ascending, actual.k)
    )
    e = [("", int, False)]
    assert l == e

    actual_sql = postgres.to_postgres_expressions(actual)  # type: ignore
    expected_sql = ["(data::int)"]
    assert actual_sql == expected_sql


def test_pick_4() -> None:
    actual = types.pick_index(Cat, lambda cat: cat.age)
    expected = types.Index[Cat, int]("age", True, int)
    assert actual == expected

    l = list(
        postgres.split_index_tuple_types(actual.fields, actual.ascending, actual.k)
    )
    e = [("age", int, True)]
    assert l == e


def test_pick_5() -> None:
    # fmt: off
    actual = types.pick_index(Cat, 
                                 
                        lambda c: c.age
                        )
    # fmt: on
    expected = types.Index[Cat, int]("age", True, int)
    assert actual == expected


def test_pick_6() -> None:
    actual = types.pick_index(types.Pair[Cat, tuple[int]], lambda p: (p.right[0],))
    expected = types.Index[types.Pair[Cat, tuple[int]], tuple[int]](
        ("right.0",), (True,), tuple[int]  # type: ignore
    )
    assert actual == expected

    l = list(
        postgres.split_index_tuple_types(actual.fields, actual.ascending, actual.k)
    )
    e = [("right.0", int, True)]
    assert l == e

    actual_sql = postgres.to_postgres_expressions(actual)  # type: ignore
    expected_sql = [
        "((data #>> '{right,0}')::int)",
    ]
    assert actual_sql == expected_sql


def test_pick_7() -> None:
    actual = types.pick_index(types.Pair[int, tuple[str, float]], lambda p: p.right)
    expected = types.Index[types.Pair[int, tuple[str, float]], tuple[str, float]](
        "right", (True, True), tuple[str, float]  # type: ignore
    )
    assert actual == expected

    l = list(
        postgres.split_index_tuple_types(actual.fields, actual.ascending, actual.k)
    )
    e = [("right.0", str, True), ("right.1", float, True)]
    assert l == e

    actual_sql = postgres.to_postgres_expressions(actual)  # type: ignore
    expected_sql = [
        "((data #>> '{right,0}')::text)",
        "((data #>> '{right,1}')::double)",
    ]
    assert actual_sql == expected_sql


def test_runtime_composite() -> None:
    def bar(t: type[T]) -> type[tuple[int, T]]:
        return types.RuntimeComposite[tuple[int, T]].sub(T=t)

    t = bar(str)
    actual = cast(Any, t)
    assert actual == tuple[int, str]

    def qux(t: type[T]) -> type[tuple[T, types.Addable] | int]:
        return types.RuntimeComposite[tuple[T, types.Addable] | int].sub(T=t)

    t2 = qux(str)
    actual2 = cast(Any, t2)
    assert actual2 == tuple[str, types.Addable] | int

    def qux2(t: type[T]) -> type[list[T]]:
        return types.RuntimeComposite[list[T]].sub(T=t)

    t3 = qux2(str)
    actual = cast(Any, t3)
    assert actual == list[str]
