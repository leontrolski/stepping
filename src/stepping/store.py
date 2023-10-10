from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, get_args

from stepping.graph import Graph, VertexUnaryDelay
from stepping.types import Store, Time, ZSet, is_type
from stepping.zset.python import ZSetPython
from stepping.zset.sql import generic, postgres, sqlite


@dataclass
class StorePython:
    _current: dict[VertexUnaryDelay[Any, Any], ZSet[Any]]
    _changes: dict[VertexUnaryDelay[Any, Any], ZSet[Any]]

    @classmethod
    def from_graph(cls, graph: Graph[Any, Any]) -> StorePython:
        store = StorePython({}, {})
        for vertex in graph.delay_vertices:
            z = ZSetPython[Any](indexes=vertex.indexes)
            store._current[vertex] = z
        return store

    def get(self, vertex: VertexUnaryDelay[Any, Any], time: Time | None) -> Any:
        if isinstance(time, Time) and time.flush_every_set is True:
            raise NotImplementedError("Internally consistency not implemented")
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        return self._current[vertex]

    def set(self, vertex: VertexUnaryDelay[Any, Any], value: Any, time: Time) -> None:
        if time.flush_every_set is True:
            raise NotImplementedError("Internally consistency not implemented")
        assert isinstance(value, (ZSetPython, generic.ZSetSQL))
        self._changes[vertex] = value

    def inc(self, time: Time) -> None:
        for vertex, value in self._changes.items():
            self._current[vertex] = value
        self._changes = {}

    def flush(self, vertices: Iterable[VertexUnaryDelay[Any, Any]], time: Time) -> None:
        raise NotImplementedError("Internally consistency not implemented")


def _make_cursor(conn: generic.Conn) -> generic.Cur:
    cur = conn.cursor()
    return cur


@dataclass
class StoreSQL:
    _zset_cls: type[generic.ZSetSQL[Any]]
    _current: dict[VertexUnaryDelay[Any, Any], generic.ZSetSQL[Any]]
    _changes: dict[VertexUnaryDelay[Any, Any], generic.ZSetSQL[Any]]
    _conn: generic.Conn
    _by_table: dict[str, list[generic.ZSetSQL[Any]]]

    def register(self, value: generic.ZSetSQL[Any]) -> None:
        self._by_table[value.table_name].append(value)

    # Called by subclasses
    @staticmethod
    def _from_graph(
        zset_cls: type[postgres.ZSetPostgres[Any]] | type[sqlite.ZSetSQLite[Any]],
        conn: generic.Conn,
        graph: Graph[Any, Any],
        create_tables: bool = True,
    ) -> StoreSQL:
        store = StoreSQL(zset_cls, {}, {}, conn, defaultdict(list))
        cur = _make_cursor(conn)
        for vertex in graph.delay_vertices:
            assert is_type(vertex.t, ZSet)
            (t,) = get_args(vertex.t)
            z = zset_cls(
                cur,  # type: ignore[arg-type]
                t,
                table_name(vertex),
                vertex.indexes,
                register=store.register,
            )
            if create_tables:
                z.create_data_table()
            store._current[vertex] = z
        conn.commit()
        return store

    def get(
        self, vertex: VertexUnaryDelay[Any, Any], time: Time | None
    ) -> generic.ZSetSQL[Any]:
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        value = self._current[vertex]
        if time is not None and time.frontier != -1:
            value.wait_til_time(time.frontier)
        return value

    def set(self, vertex: VertexUnaryDelay[Any, Any], value: Any, time: Time) -> None:
        original = self._current[vertex]

        # If the incoming value is ZSetPython, clear the table and write it fresh,
        # this happens a lot when storing the output of `make_set`.
        if isinstance(value, ZSetPython):
            value = original + (-original) + value
        if not isinstance(value, generic.ZSetSQL):
            raise NotImplementedError(f"Not sure how to store value: {type(value)}")

        self._changes[vertex] = value
        if time.flush_every_set is True:
            self.flush([vertex], time)

    def inc(self, time: Time) -> None:
        if time.flush_every_set is False:
            self.flush(self._current, time)
        self._current |= self._changes

    def flush(self, vertices: Iterable[VertexUnaryDelay[Any, Any]], time: Time) -> None:
        for vertex in vertices:
            value = self._changes[vertex]
            changes = value.changes
            value.upsert()
            if time.input_time != -1:
                value.set_last_update_time(time.input_time)
            for peer in self._by_table[value.table_name]:
                peer.changes -= changes
            self._by_table[value.table_name] = [value]

        self._conn.commit()


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
    assert isinstance(store, (StorePython, StoreSQL))
    for vertex, z in sorted(store._current.items(), key=lambda vz: str(vz[0])):
        print(vertex)
        print("-----------------------------")
        for value, count in sorted(z.iter(), key=lambda vc: str(vc[0])):
            print(value, count)
        print("-----------------------------")
