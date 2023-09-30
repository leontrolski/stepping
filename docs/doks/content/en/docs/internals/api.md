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

## ZSets

`ZSet`s can be added `a + b`, negated `-a`, `ZSetPython`s can be multiplied `a * n`

They also have the following methods:

<hr>

```python
def iter(
    self,
    match: frozenset[T] | MatchAll
) -> Iterator[tuple[T, int]]:
    ...
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+iter%28%22&type=code) Iterates over the value, count of the `ZSet` in no particular order. Optionally filter on a set of values.

<hr>

```python
def iter_by_index(
    self,
    index: Index[T, K],
    match_keys: frozenset[K] | MatchAll
) -> Iterator[tuple[K, T, int]]:
    ...
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+_iter_by_index%28%22&type=code) Iterates over the key, value, count of the indexed `ZSet` in the order defined by the index. Optionally filter on a set of values.

# Operators

## Debugging

```python
st.identity_print(
    a: T,
) -> T
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+identity_print%28%22&type=code) Prints `a`, then returns it.

<hr>

```python
st.ensure_python_zset(
    a: st.ZSet[T],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+ensure_python_zset%28%22&type=code) Converts `ZSetSQl` -> `ZSetPython`


## Day to day

```python
st.map(
    a: st.ZSet[T],
    *,
    f: Callable[[T], V],
) -> st.ZSet[V]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+map%28%22&type=code) Maps function `f` over all the values in `a`

<hr>

```python
st.map_many(
    a: st.ZSet[T],
    *,
    f: Callable[[T], frozenset[V]],
) -> st.ZSet[V]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+map_many%28%22&type=code) Maps function `f` over all the values in `a`. `f` returns many values in a `frozenset`, these are unioned together in the returned `ZSet`

<hr>

```python
st.filter(
    a: st.ZSet[T],
    *,
    f: Callable[[T], bool],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+filter%28%22&type=code) Equivalent to SQL's `WHERE`

<hr>

```python
st.join(
    l: st.ZSet[T],
    r: st.ZSet[U],
    *,
    on_left: st.Index[T, K],
    on_right: st.Index[U, K],
) -> st.ZSet[st.Pair[T, U]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+join_lifted%28%22&type=code) Equivalent to SQL's `JOIN`

<hr>

```python
st.outer_join(
    l: st.ZSet[T],
    r: st.ZSet[U],
    *,
    on_left: st.Index[T, K],
    on_right: st.Index[U, K],
) -> st.ZSet[st.Pair[T, Union[U, st.Empty]]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+outer_join_lifted%28%22&type=code) Equivalent to SQL's `LEFT OUTER JOIN`, with `Empty()` equivalent to `NULL`

<hr>

```python
st.distinct(
    a: st.ZSet[T],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+distinct_lifted%28%22&type=code) Equivalent to SQL's `DISTINCT`

<hr>

```python
st.add(
    a: TAddable,
    b: TAddable,
) -> TAddable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+add%28%22&type=code) Adds two values, equivalent to SQL's `UNION` when applied to `ZSet`s.

<hr>

```python
st.count(
    a: st.ZSet[T],
) -> st.ZSet[int]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+count_lifted%28%22&type=code) Counts the number of values (effectively `sum(count for _, count in a.iter())`). Returns a `ZSet` containing a single `int`

<hr>

```python
st.first_n(
    a: st.ZSet[T],
    *,
    index: st.Index[T, K],
    n: int,
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+first_n_lifted%28%22&type=code) Similar to SQL's `ORDER BY ... LIMIT n`. The output is unordered (it's still a `ZSet`), but calling:

```python
for key, value, count in z.iter_by_index(index):
    ...
```

Will yield values ordered by the index.

<hr>

```python
st.transitive_closure(
    a: st.ZSet[st.Pair[TIndexable, TIndexable]],
) -> st.ZSet[st.Pair[TIndexable, TIndexable]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+transitive_closure_lifted%28%22&type=code) Given a set of edges `left -> right`, returns the [transitive closure](https://en.wikipedia.org/wiki/Transitive_closure#/media/File:Transitive-closure.svg) of all the edges.

**Example** -- given:

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

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+reduce_lifted%28%22&type=code) A more general version of `st.count(...)`. In common usage `zero` and `pick_value` will be functions that either:

- Return `0` and an `int`, thereby implementing SQL's `SUM`.
- Return an empty `ZSetPython[SomeType]` and a `ZSetPython[SomeType]` respectively, thereby (kinda) implementing SQL's `ARRAY_AGG`

_Note that to handle the removal of rows in the inputted changes, `TReducable` has to implement `__mul__` (which luckily `ZSetPython`s do)._

<hr>

```python
st.group_reduce_flatten(
    a: st.ZSet[T],
    *,
    by: st.Index[T, K],
    zero: Callable[[], TReducable],
    pick_value: Callable[[T], TReducable],
) -> st.ZSet[st.Pair[TReducable, K]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+group_reduce_flatten_lifted%28%22&type=code) Equivalent to SQL's `SELECT reduce(...) FROM ... GROUP BY ...`. In common usage `zero` and `pick_value` will be functions that return a `ZSetPython[SomeType]`

The output is a `ZSet` of pairs of the reduced value and the key they were grouped by.

<hr>

```python
st.group(
    a: st.ZSet[T],
    *,
    by: st.Index[T, K],
) -> st.Grouped[st.ZSet[T], K]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+group%28%22&type=code) Groups `a` by a key.

<hr>

```python
st.flatten(
    a: st.Grouped[st.ZSet[T], K],
) -> st.ZSet[st.Pair[T, K]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+flatten%28%22&type=code) Flattens a `Group` to a more useful `ZSet`


## Internal

```python
st.neg(
    a: TNegable,
) -> TNegable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+neg%28%22&type=code) Returns `-a` (remember, applicable to `ZSet`s).

<hr>

```python
st.make_scalar(
    a: st.ZSet[T],
    *,
    zero: Callable[[], T],
) -> T
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+make_scalar%28%22&type=code) Turn a `ZSet` of count = 1 to a scalar value.

<hr>

```python
st.make_set(
    a: T,
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+make_set%28%22&type=code) Turn a scalar into a `ZSet` of count = 1. SQL implicitly does this if you do `SELECT 1`

<hr>

```python
st.add3(
    a: TAddable,
    b: TAddable,
    c: TAddable,
) -> TAddable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+add3%28%22&type=code) Add three things.

<hr>

```python
st.haitch(
    a: st.ZSet[T],
    b: st.ZSet[T],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+haitch%28%22&type=code) Used internally by `st.distinct(...)`, efficiently watches for change of sign in counts.


## Delay/Differentiate/Integrate

```python
st.delay(
    a: T,
) -> T
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+delay%28%22&type=code) Returns the previous value it was called with, see [reference page]({{< ref "/docs/internals/how-it-works.md" >}}#delays).

<hr>

```python
st.delay_indexed(
    a: st.ZSet[T],
    *,
    indexes: tuple[st.Index[T, K]], ...],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+delay_indexed%28%22&type=code) Returns the previous value it was called with, with indexes.

<hr>

```python
st.differentiate(
    a: TAddAndNegable,
) -> TAddAndNegable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+differentiate%28%22&type=code) Differentiates input values over time. See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}#differentiation).

<hr>

```python
st.integrate(
    a: TAddable,
) -> TAddable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+integrate%28%22&type=code) Integrates input values over time. See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}#integration).

<hr>

```python
st.integrate_indexed(
    a: st.ZSet[T],
    *,
    indexes: tuple[st.Index[T, K]], ...],
) -> st.ZSet[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+integrate_indexed%28%22&type=code) Integrates input values over time, adds indexes to the delay node. See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}#integration).

<hr>

```python
st.integrate_delay(
    a: TAddable,
) -> TAddable
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+integrate_delay%28%22&type=code) Integrates input values over time, returns the previous value. See [reference page]({{< ref "/docs/internals/how-it-works.md" >}}#integration).

## Compile

<hr>

```python
st.compile(
    func: Callable[..., Any],
) -> st.Graph[Any, Any]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+compile%28%22&type=code) Compile a query function to a graph.

<hr>

```python
st.compile_lazy(
    func: Callable[..., Any],
) -> Callable[[], st.Graph[Any, Any]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+compile_lazy%28%22&type=code) Returns a function with no arguments that compiles the graph, caches it and returns it.

<hr>

```python
st.compile_typeof(
    t: T,
) -> type[T]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22compile_typeof%22&type=code) Get the resolved type of `t` at query compile time.

<hr>

```python
with st.at_compile_time:
    ...
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22at_complie_time%22&type=code) Run this code block at query compile time.

<hr>

# Run

```python
st.iteration(
    store: st.Store,
    g: st.Graph[Any, Any],
    inputs: tuple[Any, ...],
    flush: bool,
) -> tuple[Any, ...]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+iteration%28%22&type=code) Run a single iteration of a graph, returning resultant changes.

<hr>

```python
st.actions(
    store: st.Store,
    g: st.Graph[Any, Any],
) -> Any
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+actions%28%22&type=code) Return a tuple of helpers to insert, remove, replace. Examples dotted around the docs.

<hr>

## Indexes

```python
st.pick_index(
    t: type[T],
    f: Callable[[T], K],
    ascending: bool | tuple[bool, ...] = True,
) -> st.Index[T, K]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+pick_index%28%22&type=code) Pick an index of `t`. The index key should be indexable:

<hr>

```python
IndexableAtom = str | int | float | bool | None | date | datetime | UUID
Indexable = IndexableAtom | tuple[IndexableAtom, ...]
```

Optionally, a boolean for ascending can be passed in, this is equivalent to SQL's `ASC`/`DESC`. If the key is `tuple[IndexableAtom, ...]`, `ascending` must be a tuple of bools of the same length.

<hr>

```python
st.pick_identity(
    t: type[KAtom],
    ascending: bool,
) -> st.Index[KAtom, KAtom]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+pick_identity%28%22&type=code) Pick an index of the value itself.

<hr>

## Database Connections

```python
with st.connection_postgres(db_url: str) as conn:
    ...
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+connection_postgres%28%22&type=code) Context manager for a Postgres connection.

<hr>

```python
with st.connection_sqlite(db_url: pathlib.Path) as conn:
    ...
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+connection_sqlite%28%22&type=code) Context manager for a SQLite connection.

<hr>

## Helpers

```python
st.annotate_zset(
    t: type[T],
) -> tuple[pydantic.Validator, ...]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+annotate_zset%28%22&type=code) Use when it is required to serialize a `ZSetPython` to a store, example:

<hr>

```python
class A(st.Data):
    ...
    zset: Annotated[ZSetPython[str], *st.annotate_zset(str)]
```

_At some point, the magic `pydantic` method will be added to make this redundant._

<hr>

```python
st.batched(
    iterable: list[T],
    n: int,
) -> Iterator[list[T]]
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+batched%28%22&type=code) See [itertools docs](https://docs.python.org/3/library/itertools.html#itertools-recipes).

<hr>

```python
st.write_png(
    graph: st.Graph[Any, Any],
    path: str,
    simplify_labels: bool,
    level: int,
) -> NoneType
```

[[src]](https://github.com/search?q=repo%3Aleontrolski%2Fstepping+path%3Asrc+%22def+write_png%28%22&type=code) Write a graph to a `.png` file using `dot`
