---
title: "API"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "internals"
weight: 506
toc: true
---

# ZSets

`ZSet`s can be added `a + b`, negated `-a`, `ZSetPython`s can be multiplied `a * n`.

They also have the following methods:

```python
def iter(
    self,
    match: frozenset[T] | MatchAll
) -> Iterator[tuple[T, int]]:
    ...
```

Iterates over the value, count of the `ZSet` in no particular order. Optionally filter on a set of values.

```python
def iter_by_index(
    self,
    index: Index[T, K],
    match_keys: frozenset[K] | MatchAll
) -> Iterator[tuple[K, T, int]]:
    ...
```

Iterates over the key, value, count of the indexed `ZSet` in the order defined by the index. Optionally filter on a set of values.

# Operators

## Debugging

```python
st.identity_print(
    a: T,
) -> T
```

Prints `a`, then returns it.

```python
st.ensure_python_zset(
    a: st.ZSet[T],
) -> st.ZSet[T]
```

Converts `ZSetSQl` -> `ZSetPython`.

## Day to day

```python
st.map(
    a: st.ZSet[T],
    *,
    f: Callable[[T], V],
) -> st.ZSet[V]
```

Maps function `f` over all the values in `a`.

```python
st.map_many(
    a: st.ZSet[T],
    *,
    f: Callable[[T], frozenset[V]],
) -> st.ZSet[V]
```

Maps function `f` over all the values in `a`. `f` returns many values in a `frozenset`, these are unioned together in the returned `ZSet`.

```python
st.filter(
    a: st.ZSet[T],
    *,
    f: Callable[[T], bool],
) -> st.ZSet[T]
```

Equivalent to SQL's `WHERE`.

```python
st.join(
    l: st.ZSet[T],
    r: st.ZSet[U],
    *,
    on_left: st.Index[T, K],
    on_right: st.Index[U, K],
) -> st.ZSet[st.Pair[T, U]]
```

Equivalent to SQL's `LEFT JOIN`.

```python
st.outer_join(
    l: st.ZSet[T],
    r: st.ZSet[U],
    *,
    on_left: st.Index[T, K],
    on_right: st.Index[U, K],
) -> st.ZSet[st.Pair[T, Union[U, st.Empty]]]
```

Equivalent to SQL's `LEFT OUTER JOIN`, with `Empty()` equivalent to `NULL`.

```python
st.distinct(
    a: st.ZSet[T],
) -> st.ZSet[T]
```

Equivalent to SQL's `DISTINCT`.

```python
st.add(
    a: TAddable,
    b: TAddable,
) -> TAddable
```

Adds two values, equivalent to SQL's `UNION` when applied to `ZSet`s.

```python
st.count(
    a: st.ZSet[T],
) -> st.ZSet[int]
```

Counts the number of values (effectively `sum(count for _, count in a.iter())`).

Returns a `ZSet` containing a single `int`.

```python
st.first_n(
    a: st.ZSet[T],
    *,
    index: st.Index[T, K],
    n: int,
) -> st.ZSet[T]
```

Similar to SQL's `ORDER BY ... LIMIT n`. The output is unordered (it's still a `ZSet`), but calling:

```python
for key, value, count in z.iter_by_index(index):
    ...
```

Will yield values ordered by the index.

```python
st.transitive_closure(
    a: st.ZSet[st.Pair[TIndexable, TIndexable]],
) -> st.ZSet[st.Pair[TIndexable, TIndexable]]
```

Given a set of edges `left -> right`, returns the [transitive closure](https://en.wikipedia.org/wiki/Transitive_closure#/media/File:Transitive-closure.svg) of all the edges.

Example, given:

```
left right
 0    1
 1    2
 2    3
 0    4
 1    5
```

Output:

```
left right
 0    1
 0    2
 0    3
 0    4
 0    5
 1    2
 1    3
 1    5
 2    3
```

Read the code for ideas implementing other recursive functions.

## Group/Reduce

```python
st.reduce(
    a: st.ZSet[T],
    *,
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
) -> st.ZSet[TReducable]
```

A more general version of `st.count(...)`. In common usage `zero` and `pick_value` will be functions that either:

- Return `0` and an `int`, thereby implementing SQL's `SUM`.
- Return an empty `ZSetPython[SomeType]` and a `ZSetPython[SomeType]` respectively, thereby (kinda) implementing SQL's `ARRAY_AGG`.

_Note that to handle the removal of rows in the inputted changes, `TReducable` has to implement `__mul__` (which luckily `ZSetPython`s do)._

```python
st.group_reduce_flatten(
    a: st.ZSet[T],
    *,
    by: st.Index[T, K],
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
) -> st.ZSet[st.Pair[TReducable, K]]
```

Equivalent to SQL's `SELECT reduce(...) FROM ... GROUP BY ...`. In common usage `zero` and `pick_value` will be functions that return a `ZSetPython[SomeType]`.

The output is a `ZSet` of pairs of: the reduced value and the key they were grouped by.

```python
st.group(
    a: st.ZSet[T],
    *,
    by: st.Index[T, K],
) -> st.Grouped[st.ZSet[T], K]
```

Groups `a` by a key.

```python
st.flatten(
    a: st.Grouped[st.ZSet[T], K],
) -> st.ZSet[st.Pair[T, K]]
```

Flattens a `Group` to a more useful `ZSet`.

## Internal

```python
st.neg(
    a: TNegable,
) -> TNegable
```

Return `-a` (remember, applicable to `ZSet`s).

```python
st.make_scalar(
    a: st.ZSet[T],
    *,
    zero: Callable[[], T],
) -> T
```

Turn a `ZSet` of count = 1 to a scalar value.

```python
st.make_set(
    a: T,
) -> st.ZSet[T]
```

Turn a scalar into a `ZSet` of count = 1. SQL implicitly does this if you do `SELECT 1`.


```python
st.add3(
    a: TAddable,
    b: TAddable,
    c: TAddable,
) -> TAddable
```

Add three things.

```python
st.haitch(
    a: st.ZSet[T],
    b: st.ZSet[T],
) -> st.ZSet[T]
```

Used internally by `st.distinct(...)`, efficiently watches for change of sign in counts.

## Delay/Differentiate/Integrate

```python
st.delay(
    a: T,
) -> T
```

Returns the previous value it was called with, see [reference page]({{< ref "/docs/internals/how-it-works.md" >}}).

```python
st.delay_indexed(
    a: st.ZSet[T],
    *,
    indexes: tuple[st.Index[T, K]], ...],
) -> st.ZSet[T]
```

Returns the previous value it was called with, with indexes.

```python
st.differentiate(
    a: TAddAndNegable,
) -> TAddAndNegable
```

See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}).

```python
st.integrate(
    a: TAddable,
) -> TAddable
```

See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}).

```python
st.integrate_delay(
    a: TAddable,
) -> TAddable
```

See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}).

```python
st.integrate_indexed(
    a: st.ZSet[T],
    *,
    indexes: tuple[st.Index[T, K]], ...],
) -> st.ZSet[T]
```

See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}).

# Compile

```python
st.compile(
    func: Callable[..., Any],
) -> st.Graph[Any, Any]
```

Compile a query function to a graph.

```python
st.compile_lazy(
    func: Callable[..., Any],
) -> Callable[[], st.Graph[Any, Any]]
```

Returns a function with no arguments that compiles the graph, caches it and returns it.

```python
st.compile_typeof(
    t: T,
) -> type[T]
```

Get the resolved type of `t` at query compile time.

```python
with st.at_compile_time:
    ...
```

Run this code block at query compile time.

# Run

```python
st.iteration(
    store: st.Store,
    g: st.Graph[Any, Any],
    inputs: tuple[Any, ...],
    flush: bool,
) -> tuple[Any, ...]
```

Run a single iteration of a graph, returning resultant changes.

```python
st.actions(
    store: st.Store,
    g: st.Graph[Any, Any],
) -> Any
```

Return a tuple of helpers to insert, remove, replace. Examples dotted around the docs.

# Indexes

```python
st.pick_index(
    t: type[T],
    f: Callable[[T], K],
    ascending: bool | tuple[bool, ...] = True,
) -> st.Index[T, K]
```

Pick an index of `t`. The index key should be indexable:

```python
IndexableAtom = str | int | float | bool | None | date | datetime | UUID
Indexable = IndexableAtom | tuple[IndexableAtom, ...]
```

Optionally, a boolean for ascending can be passed in, this is equivalent to SQL's `ASC`/`DESC`. If the key is `tuple[IndexableAtom, ...]`, `ascending` must be a tuple of bools of the same length.

```python
st.pick_identity(
    t: type[KAtom],
    ascending: bool,
) -> st.Index[KAtom, KAtom]
```

Pick an index of the value itself.

# Database Connections

```python
with st.connection_postgres(db_url: str) as conn:
    ...
```

Context manager for a Postgres connection.

```python
with st.connection_sqlite(db_url: pathlib.Path) as conn:
    ...
```

Context manager for a SQLite connection.

# Helpers

```python
st.annotate_zset(
    t: type[T],
) -> tuple[pydantic.Validator, ...]
```

Use when it is required to serialize a `ZSetPython` to a store, example:

```python
class A(st.Data):
    ...
    zset: Annotated[ZSetPython[str], *st.annotate_zset(str)]
```

_At some point, the magic `pydantic` method will be added to make this redundant._

```python
st.batched(
    iterable: list[T],
    n: int,
) -> Iterator[list[T]]
```

See [itertools docs](https://docs.python.org/3/library/itertools.html#itertools-recipes).

```python
st.write_png(
    graph: st.Graph[Any, Any],
    path: str,
    simplify_labels: bool,
    level: int,
) -> NoneType
```

Write a graph to a `.png` file using `dot`.
