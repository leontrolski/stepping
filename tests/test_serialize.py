from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Annotated, Any, Callable
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest

from stepping import serialize, types

la = ZoneInfo("America/Los_Angeles")


@dataclass(frozen=True)
class Bar(types.Data):
    x: int
    y: datetime | None
    zs: list[bool]


@dataclass
class Weird:
    p: int
    q: int

    def identity(self) -> str:
        return f"{self.p}-{self.q}"

    def serialize(self) -> types.Serialized:
        return f"{self.p}-{self.q}"

    @classmethod
    def make_deserialize(cls) -> Callable[[types.Serialized], Weird]:
        def inner(json: types.Serialized) -> Weird:
            p, q = json.split("-")  # type: ignore
            return Weird(int(p), int(q))

        return inner


weird: types.Serializable = Weird(5, 6)


@dataclass(frozen=True)
class Foo(types.Data):
    a: date
    b: Annotated[datetime, "oi"]
    bar: Bar
    bars: list[Bar]
    xs: tuple[tuple[str, ...], ...]
    coord: tuple[float, float]
    id: UUID
    c: float | int


foo = Foo(
    a=date(2023, 1, 2),
    b=datetime(1989, 12, 30, 1, 2, 3),
    bar=Bar(5, datetime(1989, 12, 30, tzinfo=la), [True, False]),
    bars=[Bar(6, None, [True]), Bar(7, datetime(1989, 12, 30), [False])],
    xs=(("str-1", "str-2"), ("str-3",)),
    coord=(1.3, 1.8),
    id=uuid4(),
    c=5.0,
)


@pytest.mark.parametrize(  # type: ignore[misc]
    ["t", "n"],
    [
        (int, 3),
        (Foo, foo),
        (Weird, weird),
        # (set[int], {4, 5}),
    ],
)
def test_serialize(t: Any, n: Any) -> None:
    serialized = serialize.serialize(n)
    deserialized = serialize.deserialize(t, serialized)  # type: ignore
    assert deserialized == n
