from typing import Any, Callable, Protocol, overload

import stepping as st
from stepping.graph import A1, A2, A3, T1, T2, T3, U1, Graph
from stepping.types import Store
from tests.conftest import Conns


class StoreMaker(Protocol):
    # fmt: off
    @staticmethod
    @overload
    def __call__(conns: Conns, func: Callable[[T1], U1]) -> tuple[Graph[A1[T1], A1[U1]], Store]: ...
    @staticmethod
    @overload
    def __call__(conns: Conns, func: Callable[[T1, T2], U1]) -> tuple[Graph[A2[T1, T2], A1[U1]], Store]: ...
    @staticmethod
    @overload
    def __call__(conns: Conns, func: Callable[[T1, T2, T3], U1]) -> tuple[Graph[A3[T1, T2, T3], A1[U1]], Store]: ...
    # fmt: on
    @staticmethod
    def __call__(
        conns: Conns,
        func: Callable[..., Any],
    ) -> tuple[Graph[Any, Any], Store]:
        ...


def store_maker_python(
    _: Conns, func: Callable[..., Any]
) -> tuple[Graph[Any, Any], Store]:
    graph = st.compile(func)
    store = st.StorePython.from_graph(graph)
    return graph, store


def store_maker_postgres(
    conns: Conns, func: Callable[..., Any]
) -> tuple[st.Graph[Any, Any], Store]:
    graph = st.compile(func)
    store = st.StorePostgres.from_graph(conns.postgres, graph, create_tables=True)
    return graph, store


def store_maker_sqlite(
    conns: Conns, func: Callable[..., Any]
) -> tuple[st.Graph[Any, Any], Store]:
    graph = st.compile(func)
    store = st.StoreSQLite.from_graph(conns.sqlite, graph, create_tables=True)
    return graph, store


store_makers: list[StoreMaker] = [store_maker_python, store_maker_postgres, store_maker_sqlite]  # type: ignore[list-item]
store_ids = ["python", "postgres", "sqlite"]
