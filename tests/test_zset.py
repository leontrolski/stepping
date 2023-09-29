from dataclasses import dataclass

from stepping.types import Pair, Reducable, T, ZSet, pick_identity, pick_index
from stepping.zset import functions
from stepping.zset.python import ZSetPython


def test_typing() -> None:
    bar: ZSet[int] = ZSetPython[int]()
    qux: Reducable = ZSetPython[int]()


@dataclass(frozen=True)
class Left:
    kind: str
    name: str
    sound_id: int


@dataclass(frozen=True)
class Right:
    sound_id: int
    sound: str


left_table = [
    Left("cat", "felix", 1),
    Left("cat", "felix", 1),
    Left("dog", "fido", 2),
    Left("dog", "rex", 2),
    Left("ant", "teeny", 3),
    Left("cow", "spot", 4),
]

right_table = [
    Right(1, "meow"),
    Right(2, "woof"),
    Right(4, "moo"),
]


def to_zset(table: list[T]) -> ZSet[T]:
    zset = ZSetPython[T]()
    for row in table:
        zset += ZSetPython({row: 1})
    return zset


def test_index() -> None:
    ix = pick_identity(int)
    zset = ZSetPython[int](indexes=(ix,))
    zset += ZSetPython({3: 2})


def test_join() -> None:
    actual = functions.join(
        to_zset(left_table),
        to_zset(right_table),
        on_left=pick_index(Left, lambda l: l.sound_id),
        on_right=pick_index(Right, lambda r: r.sound_id),
    )
    expected = ZSetPython(
        {
            Pair(
                left=Left(kind="cow", name="spot", sound_id=4),
                right=Right(sound_id=4, sound="moo"),
            ): 1,
            Pair(
                left=Left(kind="cat", name="felix", sound_id=1),
                right=Right(sound_id=1, sound="meow"),
            ): 2,
            Pair(
                left=Left(kind="dog", name="fido", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1,
            Pair(
                left=Left(kind="dog", name="rex", sound_id=2),
                right=Right(sound_id=2, sound="woof"),
            ): 1,
        },
    )
    assert actual == expected


def test_first_n() -> None:
    actual = list(functions._first_n(iter([(1, 1), (2, 1), (3, 1), (4, 1)]), 3))
    expected = [(1, 1), (2, 1), (3, 1)]
    assert actual == expected

    actual = list(functions._first_n(iter([(1, 1), (2, 4), (3, 1), (4, 1)]), 3))
    expected = [(1, 1), (2, 2)]
    assert actual == expected
