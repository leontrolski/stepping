# These first imports are not explicitly exported
from typing import Any, Callable, overload  # isort:skip
from stepping.graph import (  # isort:skip
    A1,
    A2,
    A3,
    A4,
    T1,
    T2,
    T3,
    T4,
    U1,
    U2,
    U3,
    U4,
    Path,
)
from stepping.operators.lifted import (  # isort:skip
    count_lifted,
    distinct_lifted,
    first_n_lifted,
    group_reduce_flatten_lifted,
    join_lifted,
    outer_join_lifted,
    reduce_lifted,
    transitive_closure_lifted,
)
from stepping.operators import builder, transform  # isort:skip
from stepping.operators.builder import traverse  # isort:skip

from stepping.graph import Graph as Graph
from stepping.graph import write_png as write_png
from stepping.operators.builder import at_compile_time as at_compile_time
from stepping.operators.builder import compile_typeof as compile_typeof
from stepping.operators.group import flatten as flatten
from stepping.operators.group import group as group
from stepping.operators.linear import add as add
from stepping.operators.linear import add3 as add3
from stepping.operators.linear import delay as delay
from stepping.operators.linear import delay_indexed as delay_indexed
from stepping.operators.linear import differentiate as differentiate
from stepping.operators.linear import ensure_python_zset as ensure_python_zset
from stepping.operators.linear import filter as filter
from stepping.operators.linear import haitch as haitch
from stepping.operators.linear import identity_print as identity_print
from stepping.operators.linear import integrate as integrate
from stepping.operators.linear import integrate_delay as integrate_delay
from stepping.operators.linear import integrate_indexed as integrate_indexed
from stepping.operators.linear import make_scalar as make_scalar
from stepping.operators.linear import make_set as make_set
from stepping.operators.linear import map as map
from stepping.operators.linear import map_many as map_many
from stepping.operators.linear import neg as neg
from stepping.operators.transform import Cache as Cache
from stepping.operators.transform import per_group as per_group
from stepping.run import actions as actions
from stepping.run import iteration as iteration
from stepping.store import StorePostgres as StorePostgres
from stepping.store import StorePython as StorePython
from stepping.store import StoreSQL as StoreSQL
from stepping.store import StoreSQLite as StoreSQLite
from stepping.types import Data as Data
from stepping.types import Empty as Empty
from stepping.types import Pair as Pair
from stepping.types import Store as Store
from stepping.types import ZSet as ZSet
from stepping.types import batched as batched
from stepping.types import pick_identity as pick_identity
from stepping.types import pick_index as pick_index
from stepping.zset.python import ZSetPython as ZSetPython
from stepping.zset.python import annotate_zset as annotate_zset
from stepping.zset.sql.generic import ConnPostgres as ConnPostgres
from stepping.zset.sql.generic import ConnSQLite as ConnSQLite
from stepping.zset.sql.postgres import ZSetPostgres as ZSetPostgres
from stepping.zset.sql.postgres import connection as connection_postgres_
from stepping.zset.sql.sqlite import ZSetSQLite as ZSetSQLite
from stepping.zset.sql.sqlite import connection as connection_sqlite_

distinct = distinct_lifted
count = count_lifted
first_n = first_n_lifted
group_reduce_flatten = group_reduce_flatten_lifted
join = join_lifted
outer_join = outer_join_lifted
reduce = reduce_lifted
transitive_closure = transitive_closure_lifted

connection_postgres = connection_postgres_
connection_sqlite = connection_sqlite_


# fmt: off
@overload
def compile(func: Callable[[ZSet[T1]], ZSet[U1]]) -> Graph[A1[ZSet[T1]], A1[ZSet[U1]]]: ...
@overload
def compile(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2]]]) -> Graph[A1[ZSet[T1]], A2[ZSet[U1], ZSet[U2]]]: ...
@overload
def compile(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Graph[A1[ZSet[T1]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]: ...
@overload
def compile(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Graph[A1[ZSet[T1]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2]], ZSet[U1]]) -> Graph[A2[ZSet[T1], ZSet[T2]], A1[ZSet[U1]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2]]]) -> Graph[A2[ZSet[T1], ZSet[T2]], A2[ZSet[U1], ZSet[U2]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Graph[A2[ZSet[T1], ZSet[T2]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Graph[A2[ZSet[T1], ZSet[T2]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], ZSet[U1]]) -> Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A1[ZSet[U1]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2]]]) -> Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A2[ZSet[U1], ZSet[U2]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], ZSet[U1]]) -> Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A1[ZSet[U1]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2]]]) -> Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A2[ZSet[U1], ZSet[U2]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]: ...
@overload
def compile(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]: ...
# fmt: on
def compile(
    func: Callable[..., Any],
) -> Graph[Any, Any]:
    signature = traverse.get_signature(func)
    if traverse.has_any_type_vars(signature):
        raise RuntimeError(f"function: {func} has TypeVar arguments, so can't compile")
    graph = builder.compile_generic(
        func,
        {},
        signature,
        Path(),
    )
    return transform.finalize(graph)


_LAZY_CACHE = dict[
    Callable[..., Any],
    Graph[Any, Any],
]()


# fmt: off
@overload
def compile_lazy(func: Callable[[ZSet[T1]], ZSet[U1]]) -> Callable[[], Graph[A1[ZSet[T1]], A1[ZSet[U1]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2]]]) -> Callable[[], Graph[A1[ZSet[T1]], A2[ZSet[U1], ZSet[U2]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Callable[[], Graph[A1[ZSet[T1]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Callable[[], Graph[A1[ZSet[T1]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2]], ZSet[U1]]) -> Callable[[], Graph[A2[ZSet[T1], ZSet[T2]], A1[ZSet[U1]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2]]]) -> Callable[[], Graph[A2[ZSet[T1], ZSet[T2]], A2[ZSet[U1], ZSet[U2]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Callable[[], Graph[A2[ZSet[T1], ZSet[T2]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Callable[[], Graph[A2[ZSet[T1], ZSet[T2]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], ZSet[U1]]) -> Callable[[], Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A1[ZSet[U1]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2]]]) -> Callable[[], Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A2[ZSet[U1], ZSet[U2]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Callable[[], Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Callable[[], Graph[A3[ZSet[T1], ZSet[T2], ZSet[T3]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], ZSet[U1]]) -> Callable[[], Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A1[ZSet[U1]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2]]]) -> Callable[[], Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A2[ZSet[U1], ZSet[U2]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2], ZSet[U3]]]) -> Callable[[], Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A3[ZSet[U1], ZSet[U2], ZSet[U3]]]]: ...
@overload
def compile_lazy(func: Callable[[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], tuple[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]) -> Callable[[], Graph[A4[ZSet[T1], ZSet[T2], ZSet[T3], ZSet[T4]], A4[ZSet[U1], ZSet[U2], ZSet[U3], ZSet[U4]]]]: ...
# fmt: on
def compile_lazy(
    func: Callable[..., Any],
) -> Callable[[], Graph[Any, Any]]:
    def compile_inner(
        func: Callable[..., Any] = func,
    ) -> Graph[Any, Any]:
        if func not in _LAZY_CACHE:
            _LAZY_CACHE[func] = compile(func)
        return _LAZY_CACHE[func]

    return compile_inner
