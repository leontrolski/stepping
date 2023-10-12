from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from psycopg_pool import ConnectionPool

from stepping import steppingpack
from stepping.types import (
    MATCH_ALL,
    Index,
    Indexable,
    K,
    MatchAll,
    TSerializable,
    ZSet,
    batched,
)
from stepping.zset.sql import generic

_pool: ConnectionPool | None = None
MAKE_TEST_ASSERTIONS = False
TYPE_MAP = generic.TypeDBTypeMap(
    default="TEXT",
    map=(
        (int, "INT"),
        (float, "DOUBLE"),
        (bool, "BOOLEAN"),
    ),
)


@dataclass(eq=False)
class ZSetPostgres(generic.ZSetSQL[TSerializable]):
    cur: generic.CurPostgres

    def create_data_table(self) -> None:
        return _create_data_table(self)

    def upsert(self) -> None:
        return _upsert(self, self.consolidate_changes())

    def get_by_key(
        self, index: Index[TSerializable, K], match_keys: frozenset[K] | MatchAll
    ) -> Iterator[tuple[K, TSerializable, int]]:
        return _get_by_key(self, index, match_keys)

    def get_all(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        return _get_all(self, match)


@contextmanager
def connection(db_url: str) -> Iterator[generic.ConnPostgres]:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(db_url)
    with _pool.connection() as conn:
        yield conn


def explain(cur: generic.CurPostgres, qry: str, params: tuple[Any, ...] = ()) -> str:
    return "\n".join([row[0] for row in cur.execute("EXPLAIN " + qry, params)])


@contextmanager
def force_index_usage(cur: generic.CurPostgres) -> Iterator[None]:
    cur.execute("SET enable_seqscan=off")
    yield
    cur.execute("SET enable_seqscan=on")


def _create_data_table(z_sql: ZSetPostgres[Any]) -> None:
    table_name = z_sql.table_name
    # Do outside of a TRANSACTION
    index_columns = "\n".join(
        column + ","
        for index in z_sql.indexes
        for column in generic.index_info(TYPE_MAP, index).columns_types
    )
    data_column = "" if z_sql.identity_is_data else "data BYTEA NOT NULL,"
    qry = f"""
        CREATE TABLE {table_name} (
            identity BYTEA PRIMARY KEY,
            {data_column}
            {index_columns}
            c INT NOT NULL
        )
    """
    z_sql.cur.connection.execute(qry)
    for index in z_sql.indexes:
        info = generic.index_info(TYPE_MAP, index)
        prefix = f"CREATE INDEX ix__{table_name}__{info.name} ON {table_name}"
        qry = prefix + "(" + ", ".join(info.columns_asc) + ")"
        z_sql.cur.connection.execute(qry)

    qry = f"""
        CREATE TABLE IF NOT EXISTS last_update (
            table_name TEXT PRIMARY KEY UNIQUE,
            t BIGINT NOT NULL
        )
    """
    z_sql.cur.connection.execute(qry)
    z_sql.cur.connection.execute(f"INSERT INTO last_update VALUES ('{table_name}', 0)")


def _upsert(z_sql: ZSetPostgres[TSerializable], z: ZSet[TSerializable]) -> None:
    table_name = z_sql.table_name

    values = list[tuple[Any, ...]]()
    for v, count in z.iter():
        value = tuple[Any, ...]()
        if not z_sql.identity_is_data:
            value += (steppingpack.make_identity(v),)
        value += (steppingpack.dump(v),)
        for index in z_sql.indexes:
            value += generic.dump_key(index, index.f(v))
        value += (count,)
        values.append(value)

    if not values:
        return

    qs = ", ".join("%s" for _ in range(len(values[0])))
    for vs in batched(values, n=1000):
        qry = f"""
            INSERT INTO {table_name} VALUES ({qs})
            ON CONFLICT (identity)
            DO UPDATE SET
                c = {table_name}.c + EXCLUDED.c
        """
        z_sql.cur.executemany(qry, vs)

        qry = f"""
            DELETE FROM {table_name}
            WHERE identity IN (%s)
            AND c = 0
        """
        with force_index_usage(z_sql.cur):
            z_sql.cur.executemany(qry, [(v[0],) for v in vs])


def _get_all(
    z_sql: ZSetPostgres[TSerializable],
    match: frozenset[TSerializable] | MatchAll = MATCH_ALL,
) -> Iterator[tuple[TSerializable, int]]:
    table_name = z_sql.table_name
    data_column = "identity" if z_sql.identity_is_data else "data"

    if not isinstance(match, MatchAll):
        if z_sql.identity_is_data:
            hex_strings = (r"\x" + steppingpack.dump(m).hex() for m in match)
        else:
            hex_strings = (r"\x" + steppingpack.make_identity(m).hex() for m in match)
        identity_literals = ", ".join(f"'{h}'::bytea" for h in hex_strings)
        qry = f"SELECT {data_column}, c FROM {table_name} WHERE identity IN ({identity_literals})"
        for data, c in z_sql.cur.execute(qry):
            yield steppingpack.load(z_sql.t, data), c
    else:
        qry = f"SELECT {data_column}, c FROM {table_name}"
        for data, c in z_sql.cur.execute(qry):
            yield steppingpack.load(z_sql.t, data), c


def _get_by_key(
    z_sql: ZSetPostgres[TSerializable],
    index: Index[TSerializable, K],
    match_keys: frozenset[K] | MatchAll,
) -> Iterator[tuple[K, TSerializable, int]]:
    table_name = z_sql.table_name

    info = generic.index_info(TYPE_MAP, index)
    key_expression = ", ".join(info.columns)
    order_by_expression = ", ".join(info.columns_asc)

    params: tuple[str, ...] = ()
    join_expression = ""
    if not isinstance(match_keys, MatchAll):
        select_expression = ", ".join(to_each_value(index))
        on_expression = " AND ".join(f"{e} = __{i}" for i, e in enumerate(info.columns))
        join_on = list[steppingpack.ValueJSON]()
        for key in match_keys:
            join_on.append(list(generic.dump_key(index, key)))
        join_expression = f"JOIN (SELECT {select_expression} FROM json_array_elements(%s)) AS _ ON {on_expression}"
        params = (json.dumps(join_on),)

    data_column = "identity" if z_sql.identity_is_data else "data"
    qry = f"""
        SELECT json_build_array({key_expression}) AS key, {data_column}, c
        FROM {table_name}
        {join_expression}
        ORDER BY {order_by_expression}
    """

    with force_index_usage(z_sql.cur):
        if MAKE_TEST_ASSERTIONS:
            assert "Index Scan" in explain(z_sql.cur, qry, params)

        for row in z_sql.cur.execute(qry, params):
            key_data, data, count = row
            if not index.is_composite:
                key_data = key_data[0]
            yield (
                steppingpack.load(index.k, key_data),
                steppingpack.load(z_sql.t, data),
                count,
            )


def to_each_value(index: Index[Any, Indexable]) -> list[str]:
    field_expressions = list[str]()
    for i, inner_type in enumerate(generic.index_info(TYPE_MAP, index).ks):
        t = TYPE_MAP.get(inner_type)
        field_expression = "(value #>> '{" + str(i) + "}')"
        field_expressions.append(f"{field_expression}::{t} AS __{i}")
    return field_expressions
