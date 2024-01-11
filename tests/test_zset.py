from typing import Annotated as A
from typing import TypeVar

# REVISIT
import steppingpack

from stepping.types import Index, Reducable, ZSet
from stepping.zset import functions
from stepping.zset.python import ZSetPython

T = TypeVar("T", bound=steppingpack.Value)


def test_typing() -> None:
    bar: ZSet[int] = ZSetPython(int)
    qux: Reducable = ZSetPython(int)  # type: ignore[arg-type]


class Left(steppingpack.Data):
    kind: A[str, 1]
    name: A[str, 2]
    sound_id: A[int, 3]


class Right(steppingpack.Data):
    sound_id: A[int, 1]
    sound: A[str, 2]


left_table = [
    Left(kind="cat", name="felix", sound_id=1),
    Left(kind="cat", name="felix", sound_id=1),
    Left(kind="dog", name="fido", sound_id=2),
    Left(kind="dog", name="rex", sound_id=2),
    Left(kind="ant", name="teeny", sound_id=3),
    Left(kind="cow", name="spot", sound_id=4),
]

right_table = [
    Right(sound_id=1, sound="meow"),
    Right(sound_id=2, sound="woof"),
    Right(sound_id=4, sound="moo"),
]


def to_zset(t: type[T], table: list[T]) -> ZSet[T]:
    zset = ZSetPython[T](t)
    for row in table:
        zset += ZSetPython(t, [(row, 1)])
    return zset


def test_index() -> None:
    ix = Index.identity(int)
    zset = ZSetPython[int](int, indexes=(ix,))
    zset += ZSetPython(int, [(3, 2)])


def test_join() -> None:
    actual = functions.join(
        to_zset(Left, left_table),
        to_zset(Right, right_table),
        on_left=Index.pick(Left, lambda l: l.sound_id),
        on_right=Index.pick(Right, lambda r: r.sound_id),
    )
    expected = ZSetPython(
        tuple[Left, Right],  # type: ignore[type-var]
        [
            (
                (
                    Left(kind="cow", name="spot", sound_id=4),
                    Right(sound_id=4, sound="moo"),
                ),
                1,
            ),
            (
                (
                    Left(kind="cat", name="felix", sound_id=1),
                    Right(sound_id=1, sound="meow"),
                ),
                2,
            ),
            (
                (
                    Left(kind="dog", name="fido", sound_id=2),
                    Right(sound_id=2, sound="woof"),
                ),
                1,
            ),
            (
                (
                    Left(kind="dog", name="rex", sound_id=2),
                    Right(sound_id=2, sound="woof"),
                ),
                1,
            ),
        ],
    )
    assert actual == expected


def test_first_n() -> None:
    actual = list(functions._first_n(iter([(1, 1), (2, 1), (3, 1), (4, 1)]), 3))
    expected = [(1, 1), (2, 1), (3, 1)]
    assert actual == expected

    actual = list(functions._first_n(iter([(1, 1), (2, 4), (3, 1), (4, 1)]), 3))
    expected = [(1, 1), (2, 2)]
    assert actual == expected
