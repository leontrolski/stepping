from __future__ import annotations

import json
import pathlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from types import UnionType
from typing import Any, Iterator

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
    batched,
    is_type,
)
from stepping.zset.sql import generic


@dataclass(eq=False)
class ZSetSQLite(generic.ZSetSQL[TSerializable]):
    cur: generic.CurSQLite

    def create_data_table(self) -> None:
        return _create_data_table(self)

    def upsert(self, z: ZSet[TSerializable]) -> None:
        return _upsert(self, z)

    def get_by_key(
        self, index: Index[TSerializable, K], match_keys: frozenset[K] | MatchAll
    ) -> Iterator[tuple[K, TSerializable, int]]:
        return _get_by_key(self, index, match_keys)

    def get_all(
        self, match: frozenset[TSerializable] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[TSerializable, int]]:
        return _get_all(self, match)


@contextmanager
def connection(db_url: pathlib.Path) -> Iterator[generic.ConnSQLite]:
    conn = sqlite3.connect(str(db_url.absolute()), isolation_level="IMMEDIATE")
    try:
        yield conn
    except Exception as e:
        raise e
    finally:
        conn.close()


def _create_data_table(z_sql: ZSetSQLite[Any]) -> None:
    table_name = z_sql.table.name
    # Do outside of a TRANSACTION
    z_sql.cur.connection.execute(
        f"""
        CREATE TABLE {table_name} (
            identity TEXT PRIMARY KEY,
            data TEXT NOT NULL,  -- JSON
            c INTEGER NOT NULL
        )
        """
    )
    for i in z_sql.indexes:
        prefix = f"CREATE INDEX ix__{table_name}__{index_name(i)} ON {table_name}"
        qry = prefix + "(" + ", ".join(to_expressions(i, include_asc=True)) + ")"
        z_sql.cur.connection.execute(qry)


def _upsert(z_sql: ZSetSQLite[TSerializable], z: ZSet[TSerializable]) -> None:
    table_name = z_sql.table.name
    values: list[dict[Any, Any]] = [
        dict(
            identity=make_identity(v),
            data=serialize(v),
            c=count,
        )
        for v, count in z.iter()
    ]
    if not values:
        return
    for vs in batched(values, n=1000):
        qry = f"""
            INSERT INTO {table_name} (identity, data, c)
            SELECT
                value ->> '$.identity',
                value ->  '$.data',
                value ->> '$.c'
            FROM json_each(?) WHERE TRUE
            ON CONFLICT (identity)
            DO UPDATE SET
                data = excluded.data,
                c = {table_name}.c + excluded.c
        """
        z_sql.cur.execute(qry, (json.dumps(vs),))

        identities = [v["identity"] for v in vs]
        qry = f"""
            DELETE FROM {table_name}
            WHERE identity IN (SELECT value FROM json_each(?))
            AND c = 0
        """
        z_sql.cur.execute(qry, (json.dumps(identities),))


def _get_all(
    z_sql: ZSetSQLite[TSerializable],
    match: frozenset[TSerializable] | MatchAll = MATCH_ALL,
) -> Iterator[tuple[TSerializable, int]]:
    table_name = z_sql.table.name

    if not isinstance(match, MatchAll):
        match_identities = [make_identity(m) for m in match]
        qry = f"SELECT data, c FROM {table_name} WHERE identity IN (SELECT value FROM json_each(?))"
        for data, c in z_sql.cur.execute(qry, (json.dumps(match_identities),)):
            yield deserialize(z_sql.t, json.loads(data)), c
    else:
        qry = f"SELECT data, c FROM {table_name}"
        for data, c in z_sql.cur.execute(qry):
            yield deserialize(z_sql.t, json.loads(data)), c


def _get_by_key(
    z_sql: ZSetSQLite[TSerializable],
    index: Index[TSerializable, K],
    match_keys: frozenset[K] | MatchAll,
) -> Iterator[tuple[K, TSerializable, int]]:
    is_tuple = is_type(index.k, tuple)
    table_name = z_sql.table.name

    fields_expressions = to_expressions(index)
    key_expression = ", ".join(fields_expressions)
    order_by_expression = ", ".join(to_expressions(index, include_asc=True))

    params: tuple[str, ...] = ()
    join_expression = ""
    if not isinstance(match_keys, MatchAll):
        select_expression = ", ".join(to_each_value(index))
        on_expression = " AND ".join(
            f"{e} = __{i}" for i, e in enumerate(fields_expressions)
        )
        join_on = list[Serialized]()
        for key in match_keys:
            if is_tuple:
                join_on.append([serialize(k) for k in key])  # type: ignore
            else:
                join_on.append([serialize(key)])
        join_expression = (
            f"JOIN (SELECT {select_expression} FROM json_each(?)) ON {on_expression}"
        )
        params = (json.dumps(join_on),)

    qry = f"""
        SELECT json_array({key_expression}) AS key, data, c
        FROM {table_name}
        {join_expression}
        ORDER BY {order_by_expression}
    """

    for row in z_sql.cur.execute(qry, params):
        key_data, data, count = row
        key_data = json.loads(key_data)
        data = json.loads(data)
        if not is_tuple:
            key_data = key_data[0]
        yield (
            deserialize(index.k, key_data),
            deserialize(z_sql.t, data),
            count,
        )


def type_to_db(t: type[IndexableAtom] | UnionType) -> str:
    if isinstance(t, UnionType):
        return "TEXT"
    if t is int:
        return "INTEGER"
    if t is float:
        return "REAL"
    if t is bool:
        return "INTEGER"
    return "TEXT"


def to_expressions(
    index: Index[Any, Indexable], include_asc: bool = False
) -> list[str]:
    field_expressions = list[str]()
    for field, inner_type, ascending in generic.split_index_tuple_types(
        index.fields, index.ascending, index.k
    ):
        if field:
            parts_str = "$"
            for part in field.split("."):
                if part.isdigit():
                    parts_str += f"[{part}]"
                else:
                    parts_str += f".{part}"
            field_expression = f"(data ->> '{parts_str}')"
        else:
            field_expression = "data"
        t = type_to_db(inner_type)
        asc = ""
        if include_asc and not ascending:
            asc = " DESC"
        field_expressions.append(f"CAST({field_expression} AS {t}){asc}")
    return field_expressions


def to_each_value(index: Index[Any, Indexable]) -> list[str]:
    field_expressions = list[str]()
    for i, [_, inner_type, __] in enumerate(
        generic.split_index_tuple_types(index.fields, index.ascending, index.k)
    ):
        t = type_to_db(inner_type)
        field_expression = f"(value ->> '$[{i}]')"
        field_expressions.append(f"CAST({field_expression} AS {t}) AS __{i}")
    return field_expressions


def index_name(index: Index[Any, Any]) -> str:
    index_name = "identity"
    if isinstance(index.fields, str) and index.fields:
        index_name = index.fields
    if isinstance(index.fields, tuple):
        index_name = "__".join(index.fields)
    return index_name.replace(".", "_")
