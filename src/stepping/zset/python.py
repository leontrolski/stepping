from __future__ import annotations

import json
from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Callable, Generic, Iterator, get_args

from tabulate import tabulate

from stepping import serialize
from stepping.datatypes.default_dict import DefaultDict
from stepping.datatypes.sorted_set import SortedSet
from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    MatchAll,
    Serialized,
    T,
    ZSet,
    choose,
)


@dataclass
class ZSetPython(Generic[T]):
    indexes: tuple[Index[T, Indexable], ...]
    _data: DefaultDict[T, int]
    _data_indexes: list[SortedSet[T, Indexable]]  # type: ignore

    def __repr__(self) -> str:
        indexes_str = ", ".join(
            "index on: " + (str(i.fields) if i.fields else "_value_")
            for i in self.indexes
        )
        repr_header = f"<ZSetPython{' ' if self.indexes else ''}{indexes_str}>\n"
        headers = ["_count_", "_value_"]
        table: list[tuple[Any, ...]] = [(count, value) for value, count in self.iter()]
        if self._data:
            _table = list[tuple[Any, ...]]()
            first_value, *_ = self._data.keys()
            if is_dataclass(first_value):
                _headers = ["_count_"] + [f.name for f in fields(first_value)]
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
        return repr_header + tabulate(table, headers, tablefmt="rounded_outline")  # type: ignore

    def __init__(
        self,
        data: dict[T, int]
        | DefaultDict[T, int]
        | Iterator[tuple[T, int]]
        | None = None,
        indexes: tuple[Index[T, Indexable], ...] = (),
    ) -> None:
        self._data = DefaultDict(int)
        if data is not None:
            # Warning: data from __init__ is not indexed
            # Not sure if this is too confusing.
            if isinstance(data, DefaultDict):
                self._data = data
            elif isinstance(data, dict):
                self._data.update(data)
            else:
                for v, count in data:
                    self._data[v] += count
        self.indexes = indexes
        # TODO: fix typing here - s/T/TSerializable
        self._data_indexes = [SortedSet(i) for i in indexes]  # type: ignore[type-var]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ZSetPython):
            return False
        return self._data == other._data

    def copy(self) -> ZSetPython[T]:
        out = ZSetPython[T]()
        out.indexes = self.indexes
        out._data = self._data.copy()
        out._data_indexes = [n.copy() for n in self._data_indexes]
        return out

    def get_count(self, v: T) -> int:
        return self._data.get(v, 0)

    def empty(self) -> bool:
        return len(self._data) == 0

    def __mul__(self, other: int) -> ZSetPython[T]:
        if other == 0:
            return ZSetPython[T]()
        out = self.copy()
        out._data.update({k: v * other for k, v in self.iter()})
        return out

    def __hash__(self) -> int:
        return hash((tuple(self.iter())))

    # ZSet methods

    def __neg__(self) -> ZSetPython[T]:
        out = self.copy()
        out._data.update({k: -v for k, v in self.iter()})
        return out

    def __add__(self, other: ZSet[T]) -> ZSetPython[T]:
        out = self.copy()

        for v, count in other.iter():
            if v in out._data:
                new_count = out._data[v] + count
                if new_count == 0:
                    out._data.pop(v)
                    for data_index in out._data_indexes:
                        data_index.remove(v)
                else:
                    out._data[v] = new_count
            else:
                new_count = count
                out._data[v] = count
                for data_index in out._data_indexes:
                    data_index.add(v)

        return out

    def iter(self) -> Iterator[tuple[T, int]]:
        for v, count in self._data.items():
            yield v, count

    def iter_by_index_generic(
        self,
        index: Index[T, Indexable],
        match_keys: tuple[Indexable, ...] | MatchAll = MATCH_ALL,
    ) -> Iterator[tuple[Indexable, T, int]]:
        if index not in self.indexes:
            raise RuntimeError(f"ZSet does not have index: {index}")
        data_index = next(d for d in self._data_indexes if d.index == index)
        return (
            (choose(index, v), v, self.get_count(v))
            for v in data_index.iter_matching(match_keys)
        )

    # SerializableObject methods

    def identity(self) -> str:
        return ",".join(
            sorted(
                json.dumps(
                    [serialize.serialize(value), count],  # type: ignore[arg-type]
                    separators=(",", ":"),
                    sort_keys=True,
                )
                for value, count in self.iter()
            )
        )

    def serialize(self) -> Serialized:
        return [
            [serialize.serialize(value), count]  # type: ignore[arg-type]
            for value, count in self.iter()
        ]

    @classmethod
    def make_deserialize(cls) -> Callable[[Serialized], ZSetPython[T]]:
        (inner_t,) = get_args(cls)

        def inner(n: Serialized) -> ZSetPython[T]:
            assert isinstance(n, list)

            value_counts = list[tuple[Any, int]]()
            for value_count in n:
                assert isinstance(value_count, list)
                assert len(value_count) == 2
                value_serialized, count = value_count
                assert isinstance(count, int)
                value = serialize.deserialize(inner_t, value_serialized)  # type: ignore[var-annotated]
                value_counts.append((value, count))

            return ZSetPython[T](iter(value_counts))

        return inner
