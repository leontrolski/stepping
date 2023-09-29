from typing import Any

import pytest

import stepping as st
from tests.conftest import Conns
from tests.helpers import StoreMaker, store_ids, store_makers

Rel = set[tuple[int, int]]


# left = parent_id, right = id
values = [
    (0, 1),
    (1, 2),
    (2, 3),
    (0, 4),
    (1, 5),
]


def test_raw_sql_2(postgres_conn: st.ConnPostgres) -> None:
    qry = "CREATE TABLE a_ (left_ INT, right_ INT)"
    postgres_conn.execute(qry)
    for left_, right_ in values:
        qry = f"INSERT INTO a_ VALUES ({left_}, {right_})"
        postgres_conn.execute(qry)

    qry = """
        WITH RECURSIVE delayed AS (
            SELECT left_, right_ FROM a_
            UNION
            SELECT a_.left_, delayed.right_
            FROM a_
            JOIN delayed
            ON a_.right_ = delayed.left_
        )
        SELECT * FROM delayed
    """
    actual = sorted((id, parent_id) for (id, parent_id) in postgres_conn.execute(qry))
    assert actual == [
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (0, 5),
        (1, 2),
        (1, 3),
        (1, 5),
        (2, 3),
    ]


Row = st.Pair[int, int]
on_left = st.pick_index(Row, lambda row: row.right)
on_right = st.pick_index(Row, lambda row: row.left)


def _tc_map(p: st.Pair[Row, Row]) -> Row:
    return Row(p.left.left, p.right.right)


def _f_test_python_implementation(z: st.ZSet[Row]) -> st.ZSet[Row]:
    added: st.ZSet[Row]
    delayed = st.delay(added)
    joined = st.join(z, delayed, on_left=on_left, on_right=on_right)
    mapped = st.map(joined, f=_tc_map)
    added = st.add(mapped, z)
    return added


def test_python_implementation(request: Any) -> None:
    g = st.compile(_f_test_python_implementation)

    store = st.StorePython.from_graph(g)

    zset = st.ZSetPython({Row(h, t): 1 for h, t in values})
    (actual_z,) = st.iteration(store, g, (zset,))
    (actual_z,) = st.iteration(store, g, (zset,))
    (actual_z,) = st.iteration(store, g, (zset,))
    actual = sorted((row.left, row.right) for row, _ in actual_z.iter())
    assert actual == [
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (0, 5),
        (1, 2),
        (1, 3),
        (1, 5),
        (2, 3),
    ]


def _f_test_recurse(a: st.ZSet[Row]) -> st.ZSet[Row]:
    closured = st.transitive_closure(a)
    integrated = st.integrate(closured)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_recurse(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_recurse)

    if request.config.getoption("--write-graphs"):
        st.write_png(graph, "graphs/test_recurse.png", level=6)

    # do in two passes
    zset = st.ZSetPython({Row(h, t): 1 for h, t in values[:3]})
    st.iteration(store, graph, (zset,))
    zset = st.ZSetPython({Row(h, t): 1 for h, t in values[3:]})
    st.iteration(store, graph, (zset,))
    # and again!
    (actual_z,) = st.iteration(store, graph, (zset,))
    actual = sorted((row.left, row.right) for row, _ in actual_z.iter())
    assert actual == [
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (0, 5),
        (1, 2),
        (1, 3),
        (1, 5),
        (2, 3),
    ]

    zset = st.ZSetPython({Row(1, 2): -1})
    (actual_z,) = st.iteration(store, graph, (zset,))
    actual = sorted((row.left, row.right) for row, _ in actual_z.iter())
    assert actual == [
        (0, 1),
        (0, 4),
        (0, 5),
        (1, 5),
        (2, 3),
    ]


def test_single_pass(request: Any) -> None:
    graph = st.compile(_f_test_recurse)
    store = st.StorePython.from_graph(graph)

    zset = st.ZSetPython({Row(h, t): 1 for h, t in values[:3]})
    (actual_z,) = st.iteration(store, graph, (zset,))
    actual = sorted((row.left, row.right) for row, _ in actual_z.iter())
    assert actual == [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 3),
        (2, 3),
    ]
