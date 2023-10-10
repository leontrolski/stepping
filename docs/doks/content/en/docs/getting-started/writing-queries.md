---
title: "Writing queries"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 103
toc: true
---

_If you're more of a theory person, maybe go straight to the [concepts]({{< ref "../internals/how-it-works.md" >}} "Internals") then come back here._


## Setup

Firstly, let's set up our data types and add a couple of helper functions:

```python [/docs/snippets/test_writing_queries.py::setup]
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
```

- `st.Data` is a subclass of `pydantic.BaseModel` with some other methods attached.
- `st.Pair` is just a dataclass with a `.left` and a `.right`. These are used heavily when joining/grouping data, for example if you were to `st.join(A, B)` the return type is a `Pair[A, B]`.

## Query

```python [/docs/snippets/test_writing_queries.py::query]
cache = st.Cache[str]()


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

`st.Cache` is somewhere we persist data. In this example, it's not particularly exciting as we're doing everything in memory with Python. But on the following page, we'll wire it up to a database.

`query(...)` is a function equivalent to a `SELECT ...` query in SQL. _Due to the way this gets compiled later, only a subset of Python is supported within this lexical block._

`st.ZSet` is nearly equivalent to a table in SQL. In Python, we implement it (kinda) as a `dict[Row, int]`, where the `int` is the count of the number of values (including negative counts to represent removed rows). For more details, look at the [concepts]({{< ref "../internals/how-it-works.md" >}} "Internals") page.

`st.join(...)` is equivalent to a `LEFT INNER JOIN`, there is also `st.outer_join(...)`.

`st.Index.pick(...)` picks fields from a type. These indexes are used by the `Store` later to ensure that querying past data is efficient.

`st.group_reduce_flatten(...)` is equivalent to `SELECT sum(...) FROM ... GROUP BY basket_id`. _Unlike in SQL, the group, reduce and flatten can be decomposed, see the [definition](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+%22def+group_reduce_flatten_lifted%22&type=code)._

`st.map(...)` just maps a function over each value in the `ZSet`.

`st.integrate(...)` sums its arguments over time. In this case, we build up a big `ZSet` of all the `receipt_items` over time.


## Compile

```python [/docs/snippets/test_writing_queries.py::compiling]
graph = st.compile(query)
store = st.StorePython.from_graph(graph)
```

`st.compile(...)` is probably the most complex bit of `stepping`, it takes the code from `query` and parses it into a `Graph` that we execute later. The exact type of this graph is:

```python
st.Graph[
    st.A2[
        st.ZSet[Product],
        st.ZSet[LineItem]
    ],
    st.A1[
        st.ZSet[st.Pair[Product, LineItem]]
    ],
]
```

_`Aθ` is just a collection of arguments with length `θ`._


This corresponds to the type of the function we compiled:

```python
(st.ZSet[Product], st.ZSet[LineItem]) -> st.ZSet[st.Pair[Product, LineItem]]
```

The `store` is where we put data that we need persisting between execution iterations. Right now, this is a `st.StorePython`, but on the next page, we will wire up a `st.StorePostgres`.


## Execute

```python [/docs/snippets/test_writing_queries.py::inserting]
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
```


## Retrieve

Now let's retrieve some data from the `cache`:

```python
output = cache.zset(store)
print(output)
```

`cache.zset(...)` should return a `ZSet[str]`, in this case:

```
╭───────────┬───────────────────────────╮
│   _count_ │ _value_                   │
├───────────┼───────────────────────────┤
│         1 │ Basket id: 2 total: $8.0  │
│         1 │ Basket id: 1 total: $24.0 │
╰───────────┴───────────────────────────╯
```


It's possible to cache with indexes:

```python
_ = cache[receipt_items](
    lambda z: st.integrate_indexed(z, indexes=(index_a, ...))
)
```

Then we can iterate over those values with:

```python
for key, value, count in cache.zset(store).iter_by_index(
    index_a,
    frozenset((optional, match, values, ...)),
):
    ...
```

You may have noticed when looking in your IDE, that `product_action.insert(...)` has the type:

```(*Product) -> tuple[st.ZSet[st.Pair[Product, LineItem]]]```

Every time we call `Action.insert(...)`/`Action.remove(...)`/`Action.replace(...)`, we are returned the `ZSet` of changes from running a single iteration. This is useful in the case where we want to do something with the data other than store it in a `stepping` cache (putting it on a queue for example).

This also demonstrates how removing/updating data is implemented with `ZSet`s (notice the `-1` counts):

```python [/docs/snippets/test_writing_queries.py::iteration]
iteration_output = product_action.replace(
    Product(name="tv", price=4),
    Product(name="tv", price=5),
)
print(iteration_output)
```

```
╭───────────┬─────────────────────┬─────────────────────────────────────╮
│   _count_ │ left                │ right                               │
├───────────┼─────────────────────┼─────────────────────────────────────┤
│         1 │ name='tv' price=5.0 │ basket_id=1 product_name='tv' qty=1 │
│        -1 │ name='tv' price=4.0 │ basket_id=2 product_name='tv' qty=2 │
│        -1 │ name='tv' price=4.0 │ basket_id=1 product_name='tv' qty=1 │
│         1 │ name='tv' price=5.0 │ basket_id=2 product_name='tv' qty=2 │
╰───────────┴─────────────────────┴─────────────────────────────────────╯
```

<br>
<br>

_[More example applications]({{< ref "/docs/examples" >}} "Example applications")._
