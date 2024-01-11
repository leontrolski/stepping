import pathlib
from textwrap import dedent
from typing import Annotated as A

# reference: setup
import stepping as st


class Product(st.Data):
    name: A[str, 1]
    price: A[float, 2]


class LineItem(st.Data):
    basket_id: A[int, 1]
    product_name: A[str, 2]
    qty: A[int, 3]


def pick_price(p: tuple[Product, LineItem]) -> float:
    return p[0].price * p[1].qty


def to_receipt_item(p: tuple[float, int]) -> str:
    return f"Basket id: {p[1]} total: ${p[0]}"


# /reference: setup


# reference: query
cache = st.Cache[str]()


def query(
    products: st.ZSet[Product],
    line_items: st.ZSet[LineItem],
) -> st.ZSet[tuple[Product, LineItem]]:
    joined = st.join(
        products,
        line_items,
        on_left=st.Index.pick(Product, lambda p: p.name),
        on_right=st.Index.pick(LineItem, lambda l: l.product_name),
    )
    grouped = st.group_reduce_flatten(
        joined,
        by=st.Index.pick(tuple[Product, LineItem], lambda p: p[1].basket_id),
        zero=float,
        pick_value=pick_price,
    )
    receipt_items = st.map(grouped, f=to_receipt_item)
    _ = cache[receipt_items](lambda z: st.integrate(z))
    return joined


# /reference: query


def test_sum_python() -> None:
    # reference: compiling
    graph = st.compile(query)
    store = st.StorePython.from_graph(graph)
    # /reference: compiling
    # reference: inserting
    (product_action, line_item_action) = st.actions(store, graph)

    product_action.insert(Product(name="tv", price=3.0))
    product_action.insert(Product(name="radio", price=5.0))
    line_item_action.insert(
        LineItem(basket_id=1, product_name="radio", qty=4),
        LineItem(basket_id=1, product_name="tv", qty=1),
        LineItem(basket_id=2, product_name="tv", qty=2),
    )
    product_action.replace(
        Product(name="tv", price=3.0),
        Product(name="tv", price=4.0),
    )
    # /reference: inserting

    actual = str(cache.zset(store))
    expected = dedent(
        """
        <ZSetPython>
        ╒═══════════╤═══════════════════════════╕
        │   _count_ │ _value_                   │
        ╞═══════════╪═══════════════════════════╡
        │         1 │ Basket id: 2 total: $8.0  │
        ├───────────┼───────────────────────────┤
        │         1 │ Basket id: 1 total: $24.0 │
        ╘═══════════╧═══════════════════════════╛
        """
    ).strip()
    assert set(actual.splitlines()) == set(expected.splitlines())

    # reference: iteration
    iteration_output = product_action.replace(
        Product(name="tv", price=4.0),
        Product(name="tv", price=5.0),
    )
    print(iteration_output)
    # /reference: iteration
    actual = str(iteration_output[0])
    expected = dedent(
        """
        <ZSetPython>
        ╒═══════════╤═══════════════════════════════╤═════════════════════════════════════════════════╕
        │   _count_ │ left                          │ right                                           │
        ╞═══════════╪═══════════════════════════════╪═════════════════════════════════════════════════╡
        │        -1 │ Product(name='tv', price=4.0) │ LineItem(basket_id=2, product_name='tv', qty=2) │
        ├───────────┼───────────────────────────────┼─────────────────────────────────────────────────┤
        │         1 │ Product(name='tv', price=5.0) │ LineItem(basket_id=2, product_name='tv', qty=2) │
        ├───────────┼───────────────────────────────┼─────────────────────────────────────────────────┤
        │        -1 │ Product(name='tv', price=4.0) │ LineItem(basket_id=1, product_name='tv', qty=1) │
        ├───────────┼───────────────────────────────┼─────────────────────────────────────────────────┤
        │         1 │ Product(name='tv', price=5.0) │ LineItem(basket_id=1, product_name='tv', qty=1) │
        ╘═══════════╧═══════════════════════════════╧═════════════════════════════════════════════════╛
        """
    ).strip()
    assert set(actual.splitlines()) == set(expected.splitlines())

    foo = cache.zset(store)
    bar = product_action.insert
    # reveal_locals()
    # product_action.insert: (inputs: list[Product]) -> tuple[st.ZSet[tuple[Product, LineItem]]]
    # expected: str
    # foo: st.ZSet[str]
    # graph: st.Graph[st.A2[st.ZSet[Product], st.ZSet[LineItem]], st.A1[st.ZSet[tuple[Product, LineItem]]]]
    # line_item_action: stepping.run.Action[LineItem, tuple[st.ZSet[tuple[Product, LineItem]]]]
    # product_action: stepping.run.Action[Product, tuple[st.ZSet[tuple[Product, LineItem]]]]
    # store: stepping.store.StorePython
