import pathlib
from textwrap import dedent
from typing import Annotated as A

import stepping as st


class Product(st.Data):
    name: A[str, 1]
    price: A[float, 2]


class LineItem(st.Data):
    basket_id: A[int, 1]
    product_name: A[str, 3]
    qty: A[int, 4]


def pick_price(p: tuple[Product, LineItem]) -> float:
    return p[0].price * p[1].qty


def to_receipt_item(p: tuple[float, int]) -> str:
    return f"Basket id: {p[1]} total: ${p[0]}"


cache = st.Cache[str]()


# reference: query
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


SQLITE_PATH = pathlib.Path(__file__).parent / "stepping-docs-test.db"


def test_sqlite_storage() -> None:
    SQLITE_PATH.unlink(missing_ok=True)

    graph = st.compile(query)

    # reference: sqlite_before
    with st.connection_sqlite(SQLITE_PATH) as conn:
        store = st.StoreSQLite.from_graph(conn, graph, create_tables=True)

        (product_action, line_item_action) = st.actions(store, graph)
        product_action.insert(
            Product(name="tv", price=3),
            Product(name="radio", price=5),
        )
        line_item_action.insert(
            LineItem(basket_id=1, product_name="radio", qty=4),
            LineItem(basket_id=1, product_name="tv", qty=1),
            LineItem(basket_id=2, product_name="tv", qty=2),
        )
    # /reference: sqlite_before

    # reference: sqlite_after
    with st.connection_sqlite(SQLITE_PATH) as conn:
        store = st.StoreSQLite.from_graph(conn, graph, create_tables=False)
        zset = cache.zset(store)
        # /reference: sqlite_after

        assert isinstance(zset, st.ZSetSQLite)
        table = str(zset.to_python())

        expected = dedent(
            """
            <ZSetPython>
            ╒═══════════╤═══════════════════════════╕
            │   _count_ │ _value_                   │
            ╞═══════════╪═══════════════════════════╡
            │         1 │ Basket id: 1 total: $23.0 │
            ├───────────┼───────────────────────────┤
            │         1 │ Basket id: 2 total: $6.0  │
            ╘═══════════╧═══════════════════════════╛
            """
        ).strip()
        assert set(table.splitlines()) == set(expected.splitlines())

    SQLITE_PATH.unlink(missing_ok=True)
