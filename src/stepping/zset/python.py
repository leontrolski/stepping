from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, fields
from typing import Annotated as A
from typing import Any, ClassVar, Iterable, Iterator, TypeVar

import steppingpack
from tabulate import tabulate

from stepping.types import MATCH_ALL, Index, Indexable, K, MatchAll, T, ZSet, ZSetBodge


@dataclass
class ZSetPython(ZSetBodge[T]):
    t: type[T]
    indexes: tuple[Index[T, Indexable], ...]
    _data: steppingpack.OrderedDict[T, int]
    _data_indexes: tuple[
        tuple[Index[T, Indexable], steppingpack.OrderedDict[Indexable, T]], ...
    ]

    def __repr__(self) -> str:
        indexes_str = (" " + repr(self.indexes)) if self.indexes else ""
        repr_header = f"<ZSetPython{indexes_str}>\n"
        headers = ["_count_", "_value_"]
        table: list[tuple[Any, ...]] = [(count, value) for value, count in self.iter()]
        if len(self._data):
            _table = list[tuple[Any, ...]]()
            first_value, *_ = self._data.keys()
            if issubclass(self.t, steppingpack.Data):
                _headers = ["_count_"] + [
                    item.key for item in self.t.model_schema.items
                ]
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
        t: type[T],
        data: dict[T, int] | Iterable[tuple[T, int]] | None = None,
        indexes: tuple[Index[T, Indexable], ...] = (),
    ) -> None:
        self.t = t
        self._data = steppingpack.OrderedDict(t, int, [])
        if data is not None:
            # Warning: data from __init__ is not indexed
            # Not sure if this is too confusing.
            if isinstance(data, dict):
                dict_ = data
            if not isinstance(data, dict):
                dict_ = defaultdict(int)
                for value, count in data:
                    dict_[value] += count

            self._data = steppingpack.OrderedDict(
                t, int, ((k, v) for k, v in dict_.items() if v != 0)
            )
        self.indexes = indexes
        # This ignore is kinda OK - if we've manage to define the index, it should be Serializable
        self._data_indexes = tuple((i, steppingpack.OrderedDict(i.k, t, [])) for i in indexes)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ZSetPython):
            return False
        return self._data == other._data

    def copy(self) -> ZSetPython[T]:
        out = ZSetPython[T](self.t)
        out.indexes = self.indexes
        out._data = self._data
        out._data_indexes = self._data_indexes
        return out

    def get_count(self, v: T) -> int:
        count = self._data.get(v)
        return 0 if count is None else count

    def empty(self) -> bool:
        return not bool(len(self._data))

    def __mul__(self, other: int) -> ZSetPython[T]:
        if other == 0:
            return ZSetPython[T](self.t)
        out = self.copy()
        out._data = steppingpack.OrderedDict(
            self.t, int, [(k, v * other) for k, v in self.iter()]
        )
        return out

    def __hash__(self) -> int:
        return hash((tuple(self.iter())))

    # ZSet methods

    def __neg__(self) -> ZSetPython[T]:
        out = self.copy()
        out._data = steppingpack.OrderedDict(
            self.t, int, [(k, -v) for k, v in self.iter()]
        )
        return out

    def __add__(self, other: ZSet[T]) -> ZSetPython[T]:
        return _add(self, other)

    def __isub__(self, other: ZSet[T]) -> ZSetPython[T]:
        # Performance improvement - could do self = self + (-other)
        return _add(self, other, neg=True)

    def iter(
        self, match: tuple[T, ...] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[T, int]]:
        if isinstance(match, MatchAll):
            for v, count in self._data.items():
                yield v, count
        else:
            for v, count in self._data.items_matching(match):
                yield v, count

    def _iter_by_index(
        self,
        index: Index[T, K],
        match_keys: tuple[K, ...] | MatchAll = MATCH_ALL,
    ) -> Iterator[tuple[K, T, int]]:
        if index not in self.indexes:
            raise RuntimeError(f"ZSet does not have index: {index}")
        if not match_keys:
            return iter([])
        data_index: steppingpack.OrderedDict[K, T] = next(
            d for i, d in self._data_indexes if i == index  # type: ignore[misc]
        )
        if isinstance(match_keys, MatchAll):
            return ((k, v, self.get_count(v)) for k, v in data_index.items())
        return (
            (k, v, self.get_count(v)) for k, v in data_index.items_matching(match_keys)
        )


def _add(a: ZSetPython[T], b: ZSet[T], neg: bool = False) -> ZSetPython[T]:
    out = a.copy()

    for v, count in b.iter():
        if neg:
            count = -count
        if v in out._data.keys():
            new_count = out._data[v] + count
            if new_count == 0:
                out._data = out._data.pop_new(v)
                out._data_indexes = tuple(
                    (i, d.pop_new(i.f(v))) for i, d in out._data_indexes
                )
            else:
                out._data = out._data.set_new(v, new_count)
        else:
            new_count = count
            out._data = out._data.set_new(v, count)
            out._data_indexes = tuple(
                (i, d.set_new(i.f(v), v)) for i, d in out._data_indexes
            )

    return out
