from __future__ import annotations

import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field, replace
from functools import cache
from typing import Any, Callable, Iterator, Self, get_args

import psycopg

from stepping import steppingpack
from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    IndexableAtom,
    K,
    MatchAll,
    Time,
    TSerializable,
    ZSet,
    ZSetBodge,
    is_type,
)
from stepping.zset.python import ZSetPython

ConnPostgres = psycopg.Connection[tuple[Any, ...]]
ConnSQLite = sqlite3.Connection
Conn = ConnPostgres | ConnSQLite

CurPostgres = psycopg.Cursor[Any]
CurSQLite = sqlite3.Cursor
Cur = CurPostgres | CurSQLite

MAX_SLEEP_SECS = 5.0
# 1.3 means this grows exponentially, but fairly slowly
SLEEP_SECS: list[float] = [0.00001 * 1.3**n for n in range(100)]
SLEEP_SECS = [sleep_secs for sleep_secs in SLEEP_SECS if sleep_secs <= MAX_SLEEP_SECS]


@dataclass(eq=False)
class ZSetSQL(ZSetBodge[TSerializable]):
    cur: Cur
    t: type[TSerializable]
    table_name: str
    indexes: tuple[Index[TSerializable, Any], ...]
    changes: tuple[ZSetPython[TSerializable], ...] = ()
    is_negative: bool = False
    register: Callable[[Self], None] = lambda _: None

    def __post_init__(self) -> None:
        self.register(self)

    def __eq__(self, other: object) -> bool:
        return self.to_python() == other

    def to_python(self) -> ZSetPython[TSerializable]:
        return ZSetPython(self.iter())

    @property
    def identity_is_data(self) -> bool:
        return self.t in steppingpack.IDENTITYLESS

    def consolidate_changes(self) -> ZSetPython[TSerializable]:
        if not self.changes:
            return ZSetPython[TSerializable]()
        changes, *rest = self.changes
        for other in rest:
            changes += other
        self.changes = (changes,)
        return changes

    def wait_til_time(self, frontier: int) -> None:
        qry = f"SELECT t = {frontier} FROM last_update WHERE table_name = '{self.table_name}'"
        for sleep_secs in SLEEP_SECS:
            [(reached_time,)] = self.cur.execute(qry)
            if reached_time:
                return
            time.sleep(sleep_secs)
        else:
            raise RuntimeError(f"No changes committed from frontier: {frontier}")

    def set_last_update_time(self, t: int) -> None:
        qry = f"UPDATE last_update SET t = {t} WHERE table_name = '{self.table_name}'"
        self.cur.execute(qry)

    # ZSet methods

    def __neg__(self) -> Self:
        return replace(self, is_negative=not self.is_negative)

    def __add__(self, other: ZSet[TSerializable]) -> Self:
        other_python = ZSetPython[TSerializable]() + other
        other_python = (-other_python) if self.is_negative else other_python
        changes = self.changes + (other_python,)
        return replace(self, changes=changes)

    def iter(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        if not isinstance(match, MatchAll) and not match:
            return

        neg = -1 if self.is_negative else 1

        changes = self.consolidate_changes()
        seen_from_changes = set[tuple[TSerializable, int]]()
        for v, count in self.get_all(match):
            change_count = changes.get_count(v)
            if change_count != 0:
                seen_from_changes.add((v, change_count))
            count = count + change_count
            if count:
                yield v, count * neg

        for v, count in changes.iter(match):
            if (v, count) not in seen_from_changes:
                yield v, count * neg

    def _iter_by_index(
        self,
        index: Index[TSerializable, K],
        match_keys: frozenset[K] | MatchAll = MATCH_ALL,
    ) -> Iterator[tuple[K, TSerializable, int]]:
        if index not in self.indexes:
            raise RuntimeError(f"ZSet does not have index: {index}")

        if not match_keys:
            return iter([])

        neg = -1 if self.is_negative else 1
        rows = self.get_by_key(index, match_keys)

        changes = self.consolidate_changes()
        for key, value, count in interleave_changes(rows, changes, index):
            yield (key, value, count * neg)

    # Subclass methods

    def create_data_table(self) -> None:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def upsert(self) -> None:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def get_by_key(
        self, index: Index[TSerializable, K], match_keys: frozenset[K] | MatchAll
    ) -> Iterator[tuple[K, TSerializable, int]]:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def get_all(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        raise NotImplementedError("ZSetSQL must be subclassed")


@dataclass
class IndexInfo:
    name: str
    columns: list[str]
    columns_asc: list[str]
    columns_types: list[str]
    ks: tuple[type[IndexableAtom], ...]


@dataclass(frozen=True)
class TypeDBTypeMap:
    default: str
    map: tuple[tuple[type[IndexableAtom], str], ...]

    def get(self, k: type[IndexableAtom]) -> str:
        return dict(self.map).get(k, self.default)


@cache
def index_info(type_map: TypeDBTypeMap, index: Index[Any, Any]) -> IndexInfo:
    index_name = "_".join(index.names).replace(".", "_")
    info = IndexInfo(index_name, [], [], [], ())

    ks: tuple[type[IndexableAtom], ...]
    if index.is_composite:
        ks = get_args(index.k)
    else:
        ks = (index.k,)
    assert len(ks) == len(index.ascending)

    for field, k, ascending in zip(index.names, ks, index.ascending):
        t = type_map.get(k)
        asc = ""
        if not ascending:
            asc = " DESC"
        column_name = f"ixd__{index_name}__{field.replace('.', '_')}"
        info.columns.append(column_name)
        info.columns_types.append(f"{column_name} {t} NOT NULL")
        info.columns_asc.append(f"{column_name}{asc}")

    info.ks = ks
    return info


def interleave_changes(
    a_iterator: Iterator[tuple[K, TSerializable, int]],
    changes: ZSetPython[TSerializable],
    index: Index[TSerializable, K],
) -> Iterator[tuple[K, TSerializable, int]]:
    # TODO: just add `indexes=` to `changes` and use the sorted rows from that
    b_rows = [(index.f(v), v, c) for v, c in changes.iter()]
    b_counts: dict[K, dict[TSerializable, int]] = defaultdict(dict)
    for k, v, c in b_rows:
        b_counts[k][v] = c
    b_rows = sorted(b_rows, key=lambda kvc: kvc[0])  # type: ignore

    b_iterator: Iterator[tuple[K, TSerializable, int]] = iter(b_rows)
    a = next(a_iterator, None)
    b = next(b_iterator, None)

    while not (a is None and b is None):
        if (a is not None) and (b is None or a[0] <= b[0]):  # type: ignore[operator]
            a_key, a_value, a_count = a
            # we yield all the `a`s in a group first
            if a_key in b_counts and a_value in b_counts[a_key]:
                a_count = b_counts[a_key].pop(a_value) + a_count
            if a_count != 0:
                yield a_key, a_value, a_count
            a = next(a_iterator, None)
        else:
            assert not b is None
            b_key, b_value, b_count = b
            if b_value in b_counts[b_key]:
                yield b_key, b_value, b_count
            b = next(b_iterator, None)


def dump_key(
    index: Index[Any, Any], key: Indexable
) -> tuple[steppingpack.ValueJSON, ...]:
    if index.is_composite:
        return tuple(steppingpack.dump_indexable(k) for k in key)  # type: ignore
    else:
        return (steppingpack.dump_indexable(key),)
