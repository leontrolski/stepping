from __future__ import annotations

import json
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import islice
from types import UnionType
from typing import Any, Generic, Iterator, get_args

import psycopg
from psycopg_pool import ConnectionPool

from stepping import config
from stepping.serialize import deserialize, make_identity, serialize
from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    IndexableAtom,
    K,
    MatchAll,
    Serialized,
    T,
    TSerializable,
    ZSet,
    is_type,
    strip_annotated,
)
from stepping.zset.python import ZSetPython

Conn = psycopg.Connection[tuple[Any, ...]]
_pool: ConnectionPool | None = None
MAKE_TEST_ASSERTIONS = False


@dataclass(frozen=True)
class Table:
    name: str


@dataclass
class ZSetPostgres(Generic[TSerializable]):
    conn: Conn
    t: type[TSerializable]
    table: Table
    indexes: tuple[Index[TSerializable, Any], ...]
    changes: ZSetPython[TSerializable] = field(default_factory=ZSetPython)
    is_negative: bool = False

    def __post_init__(self) -> None:
        _GLOBAL_CONN_TABLE_ZSET_MAP[(self.conn, self.table)].append(self)

    def __repr__(self) -> str:
        i = self.conn.info
        indexes_str = ", ".join(
            "index on: " + (str(i.fields) if i.fields else "_value_")
            for i in self.indexes
        )
        return (
            f"<ZSetPostgres(postgres://{i.user}@{i.host}:{i.port}/{i.dbname}:{self.table.name}{' ' if self.indexes else ''}{indexes_str} (changes below)>\n"
            + "\n".join(repr(self.changes).splitlines()[1:])
        )

    def flush_changes(self) -> None:
        if self.changes.empty():
            return

        upsert(self, self.changes)

        k = (self.conn, self.table)
        to_remove = -self.changes

        if id(self) not in {id(z_sql) for z_sql in _GLOBAL_CONN_TABLE_ZSET_MAP[k]}:
            raise RuntimeError("Have already flushed this (Conn, Table)")

        for z_sql in _GLOBAL_CONN_TABLE_ZSET_MAP[k]:
            z_sql.changes = z_sql.changes + to_remove

    def __eq__(self, other: object) -> bool:
        return self.to_python() == other

    def to_python(self) -> ZSetPython[TSerializable]:
        return ZSetPython(self.iter())

    # ZSet methods

    def __neg__(self) -> ZSet[TSerializable]:
        return ZSetPostgres(
            conn=self.conn,
            t=self.t,
            table=self.table,
            indexes=self.indexes,
            changes=self.changes,
            is_negative=not self.is_negative,
        )

    def __add__(self, other: ZSet[TSerializable]) -> ZSetPostgres[TSerializable]:
        return ZSetPostgres(
            conn=self.conn,
            t=self.t,
            table=self.table,
            indexes=self.indexes,
            changes=self.changes + other,
        )

    def iter(self) -> Iterator[tuple[TSerializable, int]]:
        neg = -1 if self.is_negative else 1

        seen_from_changes = set[tuple[TSerializable, int]]()
        for v, count in get_all(self):
            change_count = self.changes.get_count(v)
            if change_count != 0:
                seen_from_changes.add((v, change_count))
            count = count + change_count
            if count:
                yield v, count * neg

        for v, count in self.changes.iter():
            if (v, count) not in seen_from_changes:
                yield v, count * neg

    def iter_by_index_generic(
        self,
        index: Index[T, Indexable],
        match_keys: tuple[Indexable, ...] | MatchAll = MATCH_ALL,
    ) -> Iterator[tuple[Indexable, T, int]]:
        if index not in self.indexes:
            raise RuntimeError(f"ZSet does not have index: {index}")

        # We should do this like __iter__, but the ordering is hard
        self.flush_changes()

        neg = -1 if self.is_negative else 1
        rows = get_by_key(
            self, index, () if isinstance(match_keys, MatchAll) else match_keys
        )
        return ((key, value, count * neg) for key, value, count in rows)


@contextmanager
def connection() -> Iterator[Conn]:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(config.get_config().DB_URL)
    with _pool.connection() as conn:
        yield conn


def explain(conn: Conn, qry: str) -> str:
    return "\n".join([row[0] for row in conn.execute("EXPLAIN " + qry)])


def normalize_upserts(
    z_sql: ZSet[TSerializable],
) -> Iterator[tuple[TSerializable, int]]:
    """Turn (insert, delete) with the same identity into an upsert."""
    by_identity: dict[str, set[tuple[TSerializable, int]]] = defaultdict(set)
    for v, count in z_sql.iter():
        by_identity[make_identity(v)].add((v, count))

    for tuples in by_identity.values():
        if len(tuples) == 1:
            ((v, count),) = tuples
            yield v, count
        elif len(tuples) == 2:
            (_, from_count), (v, count) = sorted(tuples, key=lambda t: t[1])
            if from_count != -1 or count != 1:
                raise RuntimeError("Can only UPSERT duplicate-looking values")
            yield v, 0
        else:
            raise RuntimeError("Can only UPSERT duplicate-looking values")


def type_to_postgres(t: type[IndexableAtom] | UnionType) -> str:
    t = strip_annotated(t)
    if isinstance(t, UnionType):
        return "text"
    if t is int:
        return "int"
    if t is float:
        return "double"
    if t is bool:
        return "boolean"
    return "text"


def to_postgres_expressions(
    index: Index[Any, Indexable], include_asc: bool = False
) -> list[str]:
    field_expressions = list[str]()
    for field, inner_type, ascending in split_index_tuple_types(
        index.fields, index.ascending, index.k
    ):
        if field:
            parts = field.split(".")
            field_expression = "(data #>> '{" + ",".join(part for part in parts) + "}')"
        else:
            field_expression = "data"
        t = type_to_postgres(inner_type)
        asc = ""
        if include_asc and not ascending:
            asc = " DESC"
        field_expressions.append(f"({field_expression}::{t}){asc}")
    return field_expressions


def index_name(index: Index[Any, Any]) -> str:
    index_name = "identity"
    if isinstance(index.fields, str) and index.fields:
        index_name = index.fields
    if isinstance(index.fields, tuple):
        index_name = "__".join(index.fields)
    return index_name.replace(".", "_")


def create_data_table(
    z_sql: ZSetPostgres[Any],
) -> None:
    table_name = z_sql.table.name
    z_sql.conn.execute(
        f"""
        CREATE TABLE {table_name} (
            identity TEXT PRIMARY KEY,
            data JSONB NOT NULL,
            c INT NOT NULL
        )
        """
    )

    for i in z_sql.indexes:
        prefix = f"CREATE INDEX ix__{table_name}__{index_name(i)} ON {table_name}"
        qry = (
            prefix + "(" + ", ".join(to_postgres_expressions(i, include_asc=True)) + ")"
        )
        z_sql.conn.execute(qry)


# https://docs.python.org/3/library/itertools.html#itertools-recipes
def batched(iterable: list[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


def upsert(z_sql: ZSetPostgres[TSerializable], z: ZSet[TSerializable]) -> None:
    table_name = z_sql.table.name
    columns = ["identity", "data", "c"]
    values: list[tuple[str, Serialized, int]] = [
        (make_identity(v), json.dumps(serialize(v)), count)
        for v, count in normalize_upserts(z)
    ]
    if not values:
        return

    cur = psycopg.ClientCursor(z_sql.conn)
    placeholders = ", ".join("%s" for _ in columns)

    for vs in batched(values, n=1000):
        values_str = ", ".join(cur.mogrify(f"({placeholders})", i) for i in vs)
        qry = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES {values_str}
            ON CONFLICT (identity)
            DO UPDATE SET 
                data = EXCLUDED.data,
                c = {table_name}.c + EXCLUDED.c
        """
        cur.execute(qry)

        to_check = [identity for identity, _, _ in vs]
        to_check_str = ", ".join(cur.mogrify("%s", [i]) for i in to_check)
        qry = f"""
            DELETE FROM {table_name}
            WHERE identity IN ({to_check_str})
            AND c = 0
        """
        cur.execute(qry)

    cur.close()


def get_all(z_sql: ZSetPostgres[TSerializable]) -> Iterator[tuple[TSerializable, int]]:
    table_name = z_sql.table.name
    qry = f"SELECT data, c FROM {table_name}"
    for data, c in z_sql.conn.execute(qry):
        yield deserialize(z_sql.t, data), c


def get_by_key(
    z_sql: ZSetPostgres[TSerializable],
    index: Index[TSerializable, K],
    match_keys: tuple[K, ...],
) -> Iterator[tuple[K, TSerializable, int]]:
    is_tuple = is_type(index.k, tuple)
    table_name = z_sql.table.name
    cur = psycopg.ClientCursor(z_sql.conn)

    fields_expressions = to_postgres_expressions(index.generic)
    key_expression = ", ".join(fields_expressions)
    order_by_expression = ", ".join(
        to_postgres_expressions(index.generic, include_asc=True)
    )

    where_expressions = list[str]()
    for key in match_keys:
        values: tuple[Serialized, ...]
        if is_tuple:
            values = tuple(serialize(k) for k in key)  # type: ignore
        else:
            values = (serialize(key),)
        eq = " AND ".join(f"{expr} = %s" for expr in fields_expressions)
        where_expressions.append("(" + cur.mogrify(eq, values) + ")")

    where_expression = ""
    if where_expressions:
        where_expression = "WHERE " + " OR ".join(where_expressions)

    qry = f"""
        SELECT json_build_array({key_expression}) AS key, data, c 
        FROM {table_name}
        {where_expression}
        ORDER BY {order_by_expression}
    """

    cur.execute("SET enable_seqscan=off")

    if MAKE_TEST_ASSERTIONS:
        assert "Index Scan" in explain(z_sql.conn, qry)
    for row in cur.execute(qry):
        key_data, data, count = row
        if not is_tuple:
            key_data = key_data[0]
        yield (
            deserialize(index.k, key_data),
            deserialize(z_sql.t, data),
            count,
        )

    cur.execute("SET enable_seqscan=on")
    cur.close()


def split_index_tuple_types(
    fields: str | tuple[str, ...],
    ascending: bool | tuple[bool, ...],
    t: type[Indexable],
) -> Iterator[tuple[str, type[IndexableAtom], bool]]:
    t = strip_annotated(t)
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


_GLOBAL_CONN_TABLE_ZSET_MAP: dict[
    tuple[Conn, Table], list[ZSetPostgres[Any]]
] = defaultdict(list)


def clear_global_conn_map() -> None:
    _GLOBAL_CONN_TABLE_ZSET_MAP.clear()
