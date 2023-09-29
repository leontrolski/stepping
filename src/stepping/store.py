from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, get_args

from stepping.graph import Graph, VertexUnaryDelay
from stepping.types import Store, ZSet, is_type
from stepping.zset.python import ZSetPython
from stepping.zset.sql import generic, postgres, sqlite


@dataclass
class StorePython:
    _current: dict[VertexUnaryDelay[Any, Any], ZSet[Any]]
    _changes: list[tuple[VertexUnaryDelay[Any, Any], Any]]

    @classmethod
    def from_graph(cls, graph: Graph[Any, Any]) -> StorePython:
        store = StorePython({}, [])
        for vertex in graph.vertices:
            if not isinstance(vertex, VertexUnaryDelay):
                continue
            z = ZSetPython[Any](indexes=vertex.indexes)
            store._current[vertex] = z
        return store

    def get(self, vertex: VertexUnaryDelay[Any, Any]) -> Any:
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        return self._current[vertex]

    def set(self, vertex: VertexUnaryDelay[Any, Any], value: Any) -> None:
        assert isinstance(value, (ZSetPython, generic.ZSetSQL))
        self._changes.append((vertex, value))

    def inc(self, flush: bool) -> None:
        for vertex, value in self._changes:
            self._current[vertex] = value
        self._changes = []


def _make_cursor(conn: generic.Conn) -> generic.Cur:
    cur = conn.cursor()
    return cur


@dataclass
class StoreSQL:
    _zset_cls: type[generic.ZSetSQL[Any]]
    _current: dict[VertexUnaryDelay[Any, Any], ZSet[Any]]
    _changes: list[tuple[VertexUnaryDelay[Any, Any], ZSet[Any]]]
    _conns: dict[generic.Conn, generic.Cur] = field(default_factory=dict)

    # Called by subclasses
    @staticmethod
    def _from_graph(
        zset_cls: type[postgres.ZSetPostgres[Any]] | type[sqlite.ZSetSQLite[Any]],
        conn: generic.Conn,
        graph: Graph[Any, Any],
        create_tables: bool = True,
    ) -> StoreSQL:
        store = StoreSQL(zset_cls, {}, [])
        if conn not in store._conns:
            store._conns[conn] = _make_cursor(conn)
        cur = store._conns[conn]

        for vertex in graph.vertices:
            if not isinstance(vertex, VertexUnaryDelay):
                continue
            table = generic.Table(table_name(vertex))
            assert is_type(vertex.t, ZSet)
            (t,) = get_args(vertex.t)
            z = zset_cls(
                cur,  # type: ignore[arg-type]
                t,
                table,
                vertex.indexes,
            )
            if create_tables:
                z.create_data_table()
            store._current[vertex] = z

        return store

    def get(self, vertex: VertexUnaryDelay[Any, Any]) -> generic.ZSetSQL[Any]:
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        return self._current[vertex]  # type: ignore[return-value]

    def set(self, vertex: VertexUnaryDelay[Any, Any], value: Any) -> None:
        original = self.get(vertex)

        # If the incoming value is ZSetPython, clear the table and write it fresh,
        # this happens a lot when storing the output of `make_set`.
        if isinstance(value, ZSetPython):
            value = original + (-original) + value
        if not isinstance(value, generic.ZSetSQL):
            raise NotImplementedError(f"Not sure how to store value: {type(value)}")

        self._changes.append((vertex, value))

    def inc(self, flush: bool) -> None:
        for vertex, value in self._changes:
            self._current[vertex] = value
        self._changes = []

        if flush:
            for value in self._current.values():
                assert isinstance(value, generic.ZSetSQL)
                value.flush_changes()

            for conn in list(self._conns):
                conn.commit()
                self._conns[conn] = _make_cursor(conn)

            for value in self._current.values():
                assert isinstance(value, generic.ZSetSQL)
                value.cur = self._conns[value.cur.connection]


class StorePostgres(StoreSQL):
    _zset_cls: type[postgres.ZSetPostgres[Any]]

    @staticmethod
    def from_graph(
        conn: generic.ConnPostgres,
        graph: Graph[Any, Any],
        create_tables: bool,
    ) -> StorePostgres:
        return StoreSQL._from_graph(  # type: ignore[return-value]
            postgres.ZSetPostgres,
            conn,
            graph,
            create_tables,
        )


class StoreSQLite(StoreSQL):
    _zset_cls: type[sqlite.ZSetSQLite[Any]]

    @staticmethod
    def from_graph(
        conn: generic.ConnSQLite,
        graph: Graph[Any, Any],
        create_tables: bool,
    ) -> StoreSQLite:
        return StoreSQL._from_graph(  # type: ignore[return-value]
            sqlite.ZSetSQLite,
            conn,
            graph,
            create_tables,
        )


def _hash(s: str | bytes, length: int = 32) -> str:
    if isinstance(s, str):
        s = s.encode()

    md5 = hashlib.md5()
    md5.update(s)
    return md5.hexdigest()[:length]


def table_name(vertex: VertexUnaryDelay[Any, Any]) -> str:
    middle = list(vertex.path.inner)
    if len(middle) % 2 == 0:
        middle = [
            middle[i * 2][0] + middle[i * 2 + 1][0] for i in range(len(middle) // 2)
        ]
    else:
        middle = [n[0] for n in middle]
    return "_".join(middle) + "_" + _hash(str(vertex), 6)


def pp_store(store: Store) -> None:
    for vertex, z in sorted(store._current.items(), key=lambda vz: str(vz[0])):
        print(vertex)
        print("-----------------------------")
        for value, count in sorted(z.iter(), key=lambda vc: str(vc[0])):
            print(value, count)
        print("-----------------------------")
