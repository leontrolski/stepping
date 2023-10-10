---
title: "Storing state"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 104
toc: true
---

## SQLite

Using our query from the previous page:

```python [/docs/snippets/test_storing_state.py::query]
def query(
    products: st.ZSet[Product],
    line_items: st.ZSet[LineItem],
) -> st.ZSet[st.Pair[Product, LineItem]]:
    joined = st.join(
        products,
        line_items,
        on_left=st.Index.pick(Product, lambda p: p.name),
        on_right=st.Index.pick(LineItem, lambda l: l.product_name),
    )
    grouped = st.group_reduce_flatten(
        joined,
        by=st.Index.pick(st.Pair[Product, LineItem], lambda p: p.right.basket_id),
        zero=float,
        pick_value=pick_price,
    )
    receipt_items = st.map(grouped, f=to_receipt_item)
    _ = cache[receipt_items](lambda z: st.integrate(z))
    return joined
```

It's trivial to change the store from in-memory to a SQLite db:

```python [/docs/snippets/test_storing_state.py::sqlite_before]
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
```

_`SQLITE_PATH` is a `pathlib.Path`._

Note `create_tables=True`. This argument means that the store will create tables for each of the [delay vertices]({{< ref "/docs/internals/how-it-works.md" >}}) in the `graph`. _The table names/schemas are hashes of the query, so are fragile to changes -- see [caveats]({{< ref "/docs/in-production/caveats.md" >}})_.

Weeks later, in another service, we want to query our cache, this time, we use `create_tables=False`:

```python [/docs/snippets/test_storing_state.py::sqlite_after]
with st.connection_sqlite(SQLITE_PATH) as conn:
    store = st.StoreSQLite.from_graph(conn, graph, create_tables=False)
    zset = cache.zset(store)
```

Our `zset` looks like:

```
╒═══════════╤═══════════════════════════╕
│   _count_ │ _value_                   │
╞═══════════╪═══════════════════════════╡
│         1 │ Basket id: 1 total: $23.0 │
├───────────┼───────────────────────────┤
│         1 │ Basket id: 2 total: $6.0  │
╘═══════════╧═══════════════════════════╛
```

### Under the hood

Let's connect to our SQLite db:

```bash
sqlite3 path/to/my.db
```

And look at the schema:

```sql
sqlite> .schema
CREATE TABLE tj_sl_sd_557986 (
    identity TEXT PRIMARY KEY,
    data TEXT NOT NULL,  -- JSON
    c INTEGER NOT NULL
);
CREATE INDEX ix__tj_sl_sd_557986__name ON tj_sl_sd_557986(CAST((data ->> '$.name') AS TEXT));
CREATE TABLE tj_sr_sd_8273a0 (
    identity TEXT PRIMARY KEY,
    data TEXT NOT NULL,  -- JSON
    c INTEGER NOT NULL
);
CREATE INDEX ix__tj_sr_sd_8273a0__product_name ON tj_sr_sd_8273a0(CAST((data ->> '$.product_name') AS TEXT));
...
```

And one of the tables:

```sql
sqlite> SELECT * FROM tg_sr_sl_si_sd_rn_sd_rr_sn_edca08;
identity                              data                     c
------------------------------------  -----------------------  -
afd8c168-d73c-ac05-af43-05c65f03d43a  {"left":6.0,"right":2}   1
d7ffc609-02e4-b57c-a5cf-991a2fc848b6  {"left":23.0,"right":1}  1
```

## Postgres

As well as SQLite, there is a Postgres store, with matching interface:

```python
with st.connection_postgres(DB_URL) as conn:
    store = st.StorePostgres.from_graph(conn, graph, create_tables=True)
```

The next page has advice on setting up as Postgres connection for testing.
