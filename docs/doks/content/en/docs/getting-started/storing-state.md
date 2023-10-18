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
CREATE TABLE last_update (
    table_name TEXT PRIMARY KEY UNIQUE,
    t BIGINT NOT NULL
);
CREATE TABLE tj_sl_sd_fbfccb (
    identity BLOB PRIMARY KEY,
    data BLOB NOT NULL,
    ixd__name__name TEXT NOT NULL,
    c BIGINT NOT NULL
);
CREATE INDEX ix__tj_sl_sd_fbfccb__name ON tj_sl_sd_fbfccb(ixd__name__name);
CREATE TABLE tj_sr_sd_ed3488 (
    identity BLOB PRIMARY KEY,
    data BLOB NOT NULL,
    ixd__product_name__product_name TEXT NOT NULL,
    c BIGINT NOT NULL
);
...
```

And one of the tables:

```sql
sqlite> select * from tj_sl_sd_fbfccb;
identity   data   ixd__name__name  c
---------  -----  ---------------  -
�?+�[C��TB�  ��tv     tv               1

�

           ��radio  radio            1
>E�����
-
.E�%
```

This mangled mess is bytes of [`steppingpack`]({{< ref "/docs/internals/steppingpack.md" >}}).

## Postgres

As well as SQLite, there is a Postgres store, with matching interface:

```python
with st.connection_postgres(DB_URL) as conn:
    store = st.StorePostgres.from_graph(conn, graph, create_tables=True)
```

The next page has advice on setting up as Postgres connection for testing.


## Parallelism

If you were to implement a webserver with many workers all trying to run iterations against the same store, consistency problems would arise very quickly.

To solve this, `Action.insert(...)`/`Action.remove(...)`/`Action.replace(...)` each take an optional `time` parameter:

```python
action.insert(*values, time=Time(...))
```

`Time` looks like:

```python
class Time:
    input_time: int
    frontier: int
    flush_every_set: bool | None
```

A point in time is represented as an integer, this could be Unix time in ns, or more likely, an incrementing integer.

`input_time` is the time of _this particular set of changes_.

We wait until `frontier` has been written to the database (see `last_update` above) before reading from a table -- if this value has been written, all the changes up to and including that time have been written to that particular table.

If `flush_every_set` is:

- `False`, we flush and commit data to all tables only at the end of an iteration.
- `True`, we flush and commit data within an iteration -- each time we set the value of a delay vertex. This has the potential to be fastest, but means **you no longer have an all-or-nothing transaction wrapping the whole iteration**.
- `None`, we never flush -- this is used internally.

### Example

Following is a rough example of running `stepping` in parallel against SQLite. With `flush_every_set=True` it was possible to get around a `2x` speedup.

```python
import concurrent.futures

batches = list(st.batched(input_data, 1000))
times = [
    st.Time(input_time=i, frontier=i-1, flush_every_set=True)
    for i, _ in enumerate(batches, start=1)
]

def insert_chunk(chunk: list[SomeData], time: st.Time) -> None:
    with st.connection_sqlite(SQLITE_PATH_LOADS) as conn:
        store = st.StoreSQLite.from_graph(conn, graph, create_tables=False)
        (action,) = st.actions(store, graph)
        action.insert(*chunk, time=time)

with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
    for _ in executor.map(insert_chunk, batches, times):
        pass
```

Note for each chunk, we tell it the frontier is the time of the previous chunk: `i-1`


### Notes

- When waiting for previous changes to be written, `stepping` waits for up to `stepping.zset.sql.generic.MAX_SLEEP_SECS` -- this can be set globally.
- It's necessary to provide your own global time -- this might be in the form of a Postgres `SEQUENCE`
- As it stands, if an iteration fails, the whole system will get gummed up. This needs some deep thought to overcome.
- In the future, it might be possible to do something more clever than just locking a whole table - see literature on "database phantom rows".
