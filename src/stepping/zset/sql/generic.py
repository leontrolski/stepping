from __future__ import annotations

import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterator, Self, get_args

import psycopg

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

MAX_SLEEP_SECS = 5.0  # TODO: change me!
# 1.3 means this grows exponentially, but fairly slowly
SLEEP_SECS: list[float] = [0.00001 * 1.3**n for n in range(100)]
SLEEP_SECS = [sleep_secs for sleep_secs in SLEEP_SECS if sleep_secs <= MAX_SLEEP_SECS]


@dataclass(eq=False)
class ZSetSQL(ZSetBodge[TSerializable]):
    cur: Cur
    t: type[TSerializable]
    table_name: str
    indexes: tuple[Index[TSerializable, Any], ...]
    changes: ZSetPython[TSerializable] = field(default_factory=ZSetPython)
    is_negative: bool = False
    register: Callable[[Self], None] = lambda _: None

    def __post_init__(self) -> None:
        self.register(self)

    def __eq__(self, other: object) -> bool:
        return self.to_python() == other

    def to_python(self) -> ZSetPython[TSerializable]:
        return ZSetPython(self.iter())

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
        changes = self.changes + ((-other) if self.is_negative else other)
        return replace(self, changes=changes)

    def iter(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        if not isinstance(match, MatchAll) and not match:
            return

        neg = -1 if self.is_negative else 1

        seen_from_changes = set[tuple[TSerializable, int]]()
        for v, count in self.get_all(match):
            change_count = self.changes.get_count(v)
            if change_count != 0:
                seen_from_changes.add((v, change_count))
            count = count + change_count
            if count:
                yield v, count * neg

        for v, count in self.changes.iter(match):
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

        for key, value, count in interleave_changes(rows, self.changes, index):
            yield (key, value, count * neg)

    # Subclass methods

    def create_data_table(self) -> None:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def upsert(self, z: ZSet[TSerializable]) -> None:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def get_by_key(
        self, index: Index[TSerializable, K], match_keys: frozenset[K] | MatchAll
    ) -> Iterator[tuple[K, TSerializable, int]]:
        raise NotImplementedError("ZSetSQL must be subclassed")

    def get_all(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        raise NotImplementedError("ZSetSQL must be subclassed")


def split_index_tuple_types(
    fields: str | tuple[str, ...],
    ascending: bool | tuple[bool, ...],
    t: type[Indexable],
) -> Iterator[tuple[str, type[IndexableAtom], bool]]:
    if isinstance(fields, tuple):
        assert isinstance(ascending, tuple)
        assert is_type(t, tuple)
        inner_ts = get_args(t)
        assert len(inner_ts) == len(fields)
        assert len(ascending) == len(fields)
        for inner_fields, inner_t, asc in zip(fields, inner_ts, ascending):
            yield from split_index_tuple_types(inner_fields, asc, inner_t)
    elif is_type(t, tuple):
        inner_ts = get_args(t)
        assert isinstance(ascending, tuple)
        assert len(ascending) == len(inner_ts)
        for i, [inner_t, asc] in enumerate(zip(inner_ts, ascending)):
            yield f"{i}" if fields == "" else f"{fields}.{i}", inner_t, asc
    else:
        assert isinstance(ascending, bool)
        yield fields, t, ascending  # type: ignore


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