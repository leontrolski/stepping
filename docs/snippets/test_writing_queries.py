import pathlib
from textwrap import dedent

# reference: setup
import stepping as st


class Product(st.Data):
    name: str
    price: float


class LineItem(st.Data):
    basket_id: int
    product_name: str
    qty: int


def pick_price(p: st.Pair[Product, LineItem]) -> float:
    return p.left.price * p.right.qty


def to_receipt_item(p: st.Pair[float, int]) -> str:
    return f"Basket id: {p.right} total: ${p.left}"


# /reference: setup


# reference: query
cache = st.Cache[str]()


def query(
    products: st.ZSet[Product],
    line_items: st.ZSet[LineItem],
) -> st.ZSet[st.Pair[Product, LineItem]]:
    joined = st.join(
        products,
        line_items,
        on_left=st.pick_index(Product, lambda p: p.name),
        on_right=st.pick_index(LineItem, lambda l: l.product_name),
    )
    grouped = st.group_reduce_flatten(
        joined,
        by=st.pick_index(st.Pair[Product, LineItem], lambda p: p.right.basket_id),
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

    product_action.insert(Product(name="tv", price=3))
    product_action.insert(Product(name="radio", price=5))
    line_item_action.insert(
        LineItem(basket_id=1, product_name="radio", qty=4),
        LineItem(basket_id=1, product_name="tv", qty=1),
        LineItem(basket_id=2, product_name="tv", qty=2),
    )
    product_action.replace(
        Product(name="tv", price=3),
        Product(name="tv", price=4),
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
        Product(name="tv", price=4),
        Product(name="tv", price=5),
    )
    print(iteration_output)
    # /reference: iteration
    actual = str(iteration_output[0])
    expected = dedent(
        """
        <ZSetPython>
        ╒═══════════╤═════════════════════╤═════════════════════════════════════╕
        │   _count_ │ left                │ right                               │
        ╞═══════════╪═════════════════════╪═════════════════════════════════════╡
        │         1 │ name='tv' price=5.0 │ basket_id=1 product_name='tv' qty=1 │
        ├───────────┼─────────────────────┼─────────────────────────────────────┤
        │         1 │ name='tv' price=5.0 │ basket_id=2 product_name='tv' qty=2 │
        ├───────────┼─────────────────────┼─────────────────────────────────────┤
        │        -1 │ name='tv' price=4.0 │ basket_id=2 product_name='tv' qty=2 │
        ├───────────┼─────────────────────┼─────────────────────────────────────┤
        │        -1 │ name='tv' price=4.0 │ basket_id=1 product_name='tv' qty=1 │
        ╘═══════════╧═════════════════════╧═════════════════════════════════════╛
        """
    ).strip()
    assert set(actual.splitlines()) == set(expected.splitlines())

    foo = cache.zset(store)
    bar = product_action.insert
    # reveal_locals()
    # product_action.insert: (inputs: list[Product]) -> tuple[st.ZSet[st.Pair[Product, LineItem]]]
    # expected: str
    # foo: st.ZSet[str]
    # graph: st.Graph[st.A2[st.ZSet[Product], st.ZSet[LineItem]], st.A1[st.ZSet[st.Pair[Product, LineItem]]]]
    # line_item_action: stepping.run.Action[LineItem, tuple[st.ZSet[st.Pair[Product, LineItem]]]]
    # product_action: stepping.run.Action[Product, tuple[st.ZSet[st.Pair[Product, LineItem]]]]
    # store: stepping.store.StorePython
