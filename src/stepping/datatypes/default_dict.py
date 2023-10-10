from __future__ import annotations

from typing import Any, Callable, Generic, Iterator, TypeVar

import immutables

K = TypeVar("K")  # , bound=Hashable)
T = TypeVar("T")


class DefaultDict(Generic[K, T]):
    __slots__ = ("default_factory", "d")

    default_factory: Callable[[], T]
    d: immutables.Map[K, T]

    def __init__(self, f: Callable[[], T]) -> None:
        self.default_factory = f
        self.d = immutables.Map()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DefaultDict):
            return False
        return self.d == other.d

    def __repr__(self) -> str:
        return repr(dict(self.d))

    def __getitem__(self, k: K) -> T:
        if k not in self.d:
            self.d = self.d.set(k, self.default_factory())
        return self.d[k]

    def __bool__(self) -> bool:
        return bool(self.d)

    def __contains__(self, other: object) -> bool:
        return other in self.d

    def keys(self) -> set[K]:
        return set(self.d.keys())

    def items(self) -> Iterator[tuple[K, T]]:
        for k, v in self.d.items():
            yield k, v

    def get(self, k: K, default: T) -> T:
        return self.d.get(k, default)

    def pop(self, k: K) -> DefaultDict[K, T]:
        out = DefaultDict[K, T](self.default_factory)
        out.d = self.d.delete(k)
        return out

    def set(self, k: K, v: T) -> DefaultDict[K, T]:
        out = DefaultDict[K, T](self.default_factory)
        out.d = self.d.set(k, v)
        return out

    def update(self, d: dict[K, T]) -> DefaultDict[K, T]:
        out = DefaultDict[K, T](self.default_factory)
        out.d = self.d.update(d)
        return out
