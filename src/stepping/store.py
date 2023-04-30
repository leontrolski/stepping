from dataclasses import dataclass, field
from typing import Any

from stepping.graph import Graph, Vertex, VertexKind
from stepping.types import Store, ZSet, get_annotation_indexes
from stepping.zset import postgres
from stepping.zset.python import ZSetPython


@dataclass
class StorePython:
    _current: dict[Vertex, ZSet[Any]]
    _changes: list[tuple[Vertex, Any]]

    @classmethod
    def from_graph(cls, graph: Graph[Any, Any]) -> Store:
        store = StorePython({}, [])
        for vertex in graph.vertices:
            if vertex.kind is not VertexKind.DELAY:
                continue
            assert vertex.t == vertex.v

            _, indexes = get_annotation_indexes(vertex.v)
            z = ZSetPython[Any](indexes=indexes)
            store._current[vertex] = z
        return store

    def get(self, vertex: Vertex) -> Any:
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        return self._current[vertex]

    def set(self, vertex: Vertex, value: Any) -> None:
        assert isinstance(value, (ZSetPython, postgres.ZSetPostgres))
        self._changes.append((vertex, value))

    def inc(self) -> None:
        for vertex, value in self._changes:
            self._current[vertex] = value
        self._changes = []


@dataclass
class StorePostgres:
    _current: dict[Vertex, ZSet[Any]]
    _changes: list[tuple[Vertex, Any]]
    _conns: set[postgres.Conn] = field(default_factory=set)

    @classmethod
    def from_graph(
        cls,
        conn: postgres.Conn,
        graph: Graph[Any, Any],
        table_prefix: str,
        create_tables: bool = True,
    ) -> Store:
        store = StorePostgres({}, [])
        for vertex in graph.vertices:
            if vertex.kind is not VertexKind.DELAY:
                continue
            assert vertex.t == vertex.v

            table = postgres.Table(table_prefix + "__" + vertex.identifier)
            t, indexes = get_annotation_indexes(vertex.v)
            z = postgres.ZSetPostgres(conn, t, table, indexes)
            if create_tables:
                with conn.transaction():
                    postgres.create_data_table(z)
            store._current[vertex] = z

        store._conns.add(conn)
        return store

    def get(self, vertex: Vertex) -> Any:
        if vertex not in self._current:
            raise RuntimeError(f"There is nowhere to put data for key: {vertex}")
        return self._current[vertex]

    def set(self, vertex: Vertex, value: Any) -> None:
        # If the incoming value is ZSetPython, clear the original and write the new one
        if isinstance(value, ZSetPython):
            original = self.get(vertex)
            assert isinstance(original, postgres.ZSetPostgres)
            value = original + (-original) + value
        assert isinstance(value, postgres.ZSetPostgres)
        self._changes.append((vertex, value))

    def inc(self) -> None:
        for vertex, value in self._changes:
            self._current[vertex] = value
        self._changes = []

        for value in self._current.values():
            assert isinstance(value, postgres.ZSetPostgres)
            value.flush_changes()

        for conn in self._conns:
            conn.commit()

        postgres.clear_global_conn_map()


def pp_store(store: Store) -> None:
    for vertex, z in sorted(store._current.items(), key=lambda vz: str(vz[0])):
        print(vertex.identifier)
        print("-----------------------------")
        for value, count in sorted(z.iter(), key=lambda vc: str(vc[0])):
            print(value, count)
        print("-----------------------------")
