from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Iterator

from tabulate import tabulate

from stepping import steppingpack
from stepping.datatypes import default_dict, sorted_set
from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    K,
    MatchAll,
    Pair,
    T,
    ZSet,
    ZSetBodge,
)


@dataclass
class ZSetPython(ZSetBodge[T]):
    indexes: tuple[Index[T, Indexable], ...]
    _data: default_dict.DefaultDict[T, int]
    _data_indexes: tuple[sorted_set.SortedSet[T, Indexable], ...]  # type: ignore[type-var]

    def __repr__(self) -> str:
        indexes_str = (" " + repr(self.indexes)) if self.indexes else ""
        repr_header = f"<ZSetPython{indexes_str}>\n"
        headers = ["_count_", "_value_"]
        table: list[tuple[Any, ...]] = [(count, value) for value, count in self.iter()]
        if self._data:
            _table = list[tuple[Any, ...]]()
            first_value, *_ = self._data.keys()
            if isinstance(first_value, (Pair, steppingpack.Data)):
                if isinstance(first_value, Pair):
                    _headers = ["_count_", "left", "right"]
                else:
                    _headers = ["_count_"] + list(first_value.st_field_names)
                try:
                    for value, count in self.iter():
                        row = (count,) + tuple(
                            getattr(value, f.name)
                            for f in fields(value)  # type: ignore
                        )
                        _table.append(row)
                    headers = _headers
                    table = _table
                except Exception:
                    pass
        return repr_header + tabulate(table, headers, tablefmt="fancy_grid")  # type: ignore

    def __init__(
        self,
        data: dict[T, int] | Iterator[tuple[T, int]] | None = None,
        indexes: tuple[Index[T, Indexable], ...] = (),
    ) -> None:
        self._data = default_dict.DefaultDict(int)
        if data is not None:
            # Warning: data from __init__ is not indexed
            # Not sure if this is too confusing.
            if isinstance(data, dict):
                self._data = self._data.update(data)
            else:
                for v, count in data:
                    self._data = self._data.set(v, self._data[v] + count)
        self.indexes = indexes
        # This ignore is kinda OK - if we've manage to define the index, it should be Serializable
        self._data_indexes = tuple(sorted_set.SortedSet(i) for i in indexes)  # type: ignore[type-var]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ZSetPython):
            return False
        return self._data == other._data

    def copy(self) -> ZSetPython[T]:
        out = ZSetPython[T]()
        out.indexes = self.indexes
        out._data = self._data
        out._data_indexes = self._data_indexes
        return out

    def get_count(self, v: T) -> int:
        return self._data.get(v, 0)

    def empty(self) -> bool:
        return not bool(self._data)

    def __mul__(self, other: int) -> ZSetPython[T]:
        if other == 0:
            return ZSetPython[T]()
        out = self.copy()
        out._data = out._data.update({k: v * other for k, v in self.iter()})
        return out

    def __hash__(self) -> int:
        return hash((tuple(self.iter())))

    # ZSet methods

    def __neg__(self) -> ZSetPython[T]:
        out = self.copy()
        out._data = out._data.update({k: -v for k, v in self.iter()})
        return out

    def __add__(self, other: ZSet[T]) -> ZSetPython[T]:
        return _add(self, other)

    def __isub__(self, other: ZSet[T]) -> ZSetPython[T]:
        # Performance improvement - could do self = self + (-other)
        return _add(self, other, neg=True)

    def iter(
        self, match: frozenset[T] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[T, int]]:
        if isinstance(match, MatchAll):
            for v, count in self._data.items():
                yield v, count
        else:
            for m in match:
                if m in self._data:
                    yield m, self._data[m]

    def _iter_by_index(
        self,
        index: Index[T, K],
        match_keys: frozenset[K] | MatchAll = MATCH_ALL,
    ) -> Iterator[tuple[K, T, int]]:
        if index not in self.indexes:
            raise RuntimeError(f"ZSet does not have index: {index}")
        if not match_keys:
            return iter([])
        data_index = next(d for d in self._data_indexes if d.index == index)
        return (
            (index.f(v), v, self.get_count(v))
            for v in data_index.iter_matching(match_keys)
        )


def _add(a: ZSetPython[T], b: ZSet[T], neg: bool = False) -> ZSetPython[T]:
    out = a.copy()

    for v, count in b.iter():
        if neg:
            count = -count
        if v in out._data:
            new_count = out._data[v] + count
            if new_count == 0:
                out._data = out._data.pop(v)
                for i, data_index in enumerate(out._data_indexes):
                    out._data_indexes = (
                        out._data_indexes[: i - 1] +
                        (data_index.remove(v),) +
                        out._data_indexes[i + 1 :]
                    )
            else:
                out._data = out._data.set(v, new_count)
        else:
            new_count = count
            out._data = out._data.set(v, count)
            for i, data_index in enumerate(out._data_indexes):
                out._data_indexes = (
                    out._data_indexes[: i - 1] +
                    (data_index.add(v),) +
                    out._data_indexes[i + 1 :]
                )

    return out
