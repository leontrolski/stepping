from typing import Callable

import pytest

import stepping as st
from stepping import actions
from stepping.graph import A1, Graph
from stepping.types import EMPTY, ZSet
from stepping.zset.python import ZSetPython
from tests.conftest import Conns
from tests.helpers import StoreMaker, store_ids, store_makers


# TODO: replace these with `st.actions`
def make_insert(
    store: st.Store, graph: Graph[A1[ZSet[int]], A1[ZSet[int]]]
) -> Callable[[int], ZSet[int]]:
    def inner(n: int) -> ZSet[int]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: 1}),))
        return out

    return inner


def make_remove(
    store: st.Store, graph: Graph[A1[ZSet[int]], A1[ZSet[int]]]
) -> Callable[[int], ZSet[int]]:
    def inner(n: int) -> ZSet[int]:
        (out,) = st.iteration(store, graph, (ZSetPython({n: -1}),))
        return out

    return inner


def _f_test_definition_3_27(a: ZSet[int]) -> ZSet[int]:
    integrated = st.integrate(a)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_definition_3_27(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_definition_3_27)
    insert = make_insert(store, graph)

    actual = insert(8)
    assert actual == ZSetPython({8: 1})
    actual = insert(6)
    assert actual == ZSetPython({8: 1, 6: 1})
    actual = insert(4)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 1})
    actual = insert(5)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 1, 5: 1})
    actual = insert(4)
    assert actual == ZSetPython({8: 1, 6: 1, 4: 2, 5: 1})


def _f_test_definition_3_25(a: ZSet[int]) -> ZSet[int]:
    differentiated = st.differentiate(a)
    differentiated2 = st.ensure_python_zset(differentiated)
    return differentiated2


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_definition_3_25(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_definition_3_25)
    insert = make_insert(store, graph)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({3: -1, 4: 1})
    actual = insert(5)
    assert actual == ZSetPython({4: -1, 5: 1})


def _f_test_theorem_3_30_a(a: ZSet[int]) -> ZSet[int]:
    integrated = st.integrate(a)
    differentiated = st.differentiate(integrated)
    differentiated2 = st.ensure_python_zset(differentiated)
    return differentiated2


def _f_test_theorem_3_30_b(a: ZSet[int]) -> ZSet[int]:
    differentiated = st.differentiate(a)
    integrated = st.integrate(differentiated)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_theorem_3_30_a(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_theorem_3_30_a)
    insert = make_insert(store, graph)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({4: 1})
    actual = insert(5)
    assert actual == ZSetPython({5: 1})


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_theorem_3_30_b(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_theorem_3_30_b)
    insert = make_insert(store, graph)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({4: 1})
    actual = insert(5)
    assert actual == ZSetPython({5: 1})


def _f_test_integrate_and_delay(a: ZSet[int]) -> ZSet[int]:
    integrated = st.integrate_delay(a)
    integrated2 = st.ensure_python_zset(integrated)
    return integrated2


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_integrate_and_delay(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_integrate_and_delay)
    insert = make_insert(store, graph)

    actual = insert(8)
    assert actual == ZSetPython[int]()
    actual = insert(6)
    assert actual == ZSetPython({8: 1})
    actual = insert(6)
    assert actual == ZSetPython({8: 1, 6: 1})


def _f_test_prop_6_3(a: ZSet[int]) -> ZSet[int]:
    distincted = st.distinct(a)
    return distincted


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_prop_6_3(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_prop_6_3)
    insert = make_insert(store, graph)

    (action,) = actions(store, graph)

    # Remove all of these in favour of `action`
    insert = make_insert(store, graph)
    remove = make_remove(store, graph)

    (actual,) = action.insert(3, 3)
    assert actual == ZSetPython({3: 1})
    (actual,) = action.insert(3)
    assert actual == ZSetPython[int]()
    (actual,) = action.insert(4)
    assert actual == ZSetPython({4: 1})
    (actual,) = action.remove(3)
    assert actual == ZSetPython[int]()
    (actual,) = action.remove(3)
    assert actual == ZSetPython[int]()
    (actual,) = action.remove(3)
    assert actual == ZSetPython({3: -1})


def _f_test_prop_6_3_integrate(a: ZSet[int]) -> ZSet[int]:
    distincted = st.distinct(a)
    integrated = st.integrate(distincted)
    return integrated


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_prop_6_3_integrate(conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_prop_6_3_integrate)
    insert = make_insert(store, graph)
    insert = make_insert(store, graph)
    remove = make_remove(store, graph)

    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(3)
    assert actual == ZSetPython({3: 1})
    actual = insert(4)
    assert actual == ZSetPython({3: 1, 4: 1})
    actual = remove(3)
    assert actual == ZSetPython({3: 1, 4: 1})
    actual = remove(3)
    assert actual == ZSetPython({4: 1})
