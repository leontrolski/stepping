---
title: "Type Checker"
description: ""
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "examples"
weight: 303
toc: true
---

`stepping` can be used anywhere where we receive changes incrementally - this includes larger data applications, but can also be applied to typecheckers and build systems.

## Problem

Consider the following Python files:

File `one.py`:

```python
class A:
    x: str

class B:
    y: A

class C:
    z: B
```

File `two.py`:

```python
import one

class D:
    y: one.A
    z: int

class E:
    x: float
```

We'd like to incrementally typecheck our code, so that when we update a class, we know to typecheck _only the types that could possibly have changed_ and nothing more.

## Representation

Thinking about the problem, we have a graph of dependencies:

```
one.A ─> one.B ─> one.C
    └──> two.D

two.E
```

If we represent our class types as the following:

```python [/docs/snippets/test_typechecker.py::class-class]
class Class(st.Data):
    identifier: str  # eg: "one.A"
    attrs: tuple[tuple[str, str], ...]
```

Our input data looks like:

```python [/docs/snippets/test_typechecker.py::input-data]
input_data = [
    Class(identifier="one.A", attrs=(("x", "str"),)),
    Class(identifier="one.B", attrs=(("y", "one.A"),)),
    Class(identifier="one.C", attrs=(("z", "one.B"),)),
    Class(identifier="two.D", attrs=(("y", "one.A"), ("z", "int"))),
    Class(identifier="two.E", attrs=(("x", "float"),)),
]
```

## Setup

We're going to skip over most of the setup -- full details in [test_typechecker.py](https://github.com/leontrolski/stepping/blob/main/docs/snippets/test_typechecker.py).

For context, it's hopefully enough to know that we have:

```python
# Denormalised attributes
class Attr(st.Data):
    identifier: str  # eg: "one.A"
    key: str  # eg: "x"
    value: str  # eg: "int" or "one.A"

# Denormalised nested structure of a class
class Resolved(st.Data):
    identifier: str  # eg: "one.A"
    attrs: tuple[A, ...]  # A is a nested set of kes

# Helper functions
def to_many_attrs(c: Class) -> frozenset[Attr]: ...
def to_edge(a: Attr) -> st.Pair[str, str]: ...
def zset_zero() -> st.ZSetPython[Class]: ...
def pick_zset(p: st.Pair[st.Pair[str, str], Class]) -> st.ZSetPython[Class]: ...
def resolve(p: st.Pair[Class, st.Pair[st.ZSetPython[Class], str] | st.Empty]) -> Resolved: ...
```

## Query

The query we're going to write looks like:

```python [/docs/snippets/test_typechecker.py::query]
def link_attrs(classes: st.ZSet[Class]) -> st.ZSet[Resolved]:
    attrs = st.map_many(classes, f=to_many_attrs)
    edges = st.map(attrs, f=to_edge)
    all_edges = st.transitive_closure(edges)

    from_to = st.join(
        all_edges,
        classes,
        on_left=st.pick_index(st.Pair[str, str], lambda p: p.right),
        on_right=st.pick_index(Class, lambda a: a.identifier),
    )
    grouped_by_from_identifier = st.group_reduce_flatten(
        from_to,
        by=st.pick_index(st.Pair[st.Pair[str, str], Class], lambda p: p.left.left),
        zero=zset_zero,
        pick_value=pick_zset,
    )
    from_joined_to_relevant = st.outer_join(
        classes,
        grouped_by_from_identifier,
        on_left=st.pick_index(Class, lambda a: a.identifier),
        on_right=st.pick_index(st.Pair[st.ZSetPython[Class], str], lambda p: p.right),
    )
    resolved = st.map(from_joined_to_relevant, f=resolve)
    _ = output_cache[resolved](lambda r:  st.integrate(r))
    return resolved
```

In your IDE, you'll see the types of everything, for reference:

```python
attrs: st.ZSet[Attr]
edges: st.ZSet[st.Pair[str, str]]
all_edges: st.ZSet[st.Pair[str, str]]
from_to: st.ZSet[st.Pair[st.Pair[str, str], Class]]
grouped_by_from_identifier: st.ZSet[st.Pair[st.ZSetPython[Class], str]]
from_joined_to_relevant: st.ZSet[st.Pair[Class, st.Pair[st.ZSetPython[Class], str] | st.Empty]]
resolved: st.ZSet[Resolved]
```

The steps are:

We denormalise the classes to a set of edges of a graph.

Then we recursively determine the transitive closure of the graph, meaning that we know for example that `C` depends on `A`.

We join all of the edges to the original classes, then reduce these into `ZSet`s, grouping by their left hand identifier.

Then we **outer** join the original classes on the grouped `ZSet`s.

Finally, we map over the classes and their children to produce `Resolved`s.

## Running

We run our query function against a `StoreSQLite` (many more details on setting this up elsewhere):

```python
graph = st.compile(link_attrs)
store = st.StoreSQLite.from_graph(conn, graph, create_tables=True)
(action,) = st.actions(store, graph)
(actual,) = action.insert(*input_data)
```

The result from our first iteration -- adding all the initial data -- is:

```
╒═══════════╤══════════════════════════════════════════════════════════════════════════════════════════════════╕
│   _count_ │ _value_                                                                                          │
╞═══════════╪══════════════════════════════════════════════════════════════════════════════════════════════════╡
│         1 │ identifier='one.C' attrs=(A(key='z', value=(A(key='y', value=(A(key='x', value='str'),)),)),)    │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
│         1 │ identifier='one.B' attrs=(A(key='y', value=(A(key='x', value='str'),)),)                         │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
│         1 │ identifier='one.A' attrs=(A(key='x', value='str'),)                                              │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
│         1 │ identifier='two.E' attrs=(A(key='x', value='float'),)                                            │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
│         1 │ identifier='two.D' attrs=(A(key='y', value=(A(key='x', value='str'),)), A(key='z', value='int')) │
╘═══════════╧══════════════════════════════════════════════════════════════════════════════════════════════════╛
```

The cool thing happens when we remove a `Class` with many dependants:

```python
(actual,) = action.remove(
    Class(identifier="one.B", attrs=(("y", "one.A"),))
)
```

We correctly return just the relevant changes to the output:

```
╒═══════════╤═══════════════════════════════════════════════════════════════════════════════════════════════╕
│   _count_ │ _value_                                                                                       │
├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
│        -1 │ identifier='one.B' attrs=(A(key='y', value=(A(key='x', value='str'),)),)                      │
╞═══════════╪═══════════════════════════════════════════════════════════════════════════════════════════════╡
│        -1 │ identifier='one.C' attrs=(A(key='z', value=(A(key='y', value=(A(key='x', value='str'),)),)),) │
├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
│         1 │ identifier='one.C' attrs=(A(key='z', value='one.B'),)                                         │
╘═══════════╧═══════════════════════════════════════════════════════════════════════════════════════════════╛
```

And we can read the data at any time, from any other process by doing:

```python
store = st.StoreSQLite.from_graph(conn, graph, create_tables=False)
zset = output_cache.zset(store)
```

## Other applications

Taking this kind of approach, it's possible to conceive of similar applications, for example:

- A [bazel](https://bazel.build/)-like build system that caches changes. (Implement a special `ZSet` for binary files in S3?).
- Writing a `stepping.js` that uses an in-memory store, handling efficient updates to the DOM.
