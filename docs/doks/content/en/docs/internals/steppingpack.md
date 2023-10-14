---
title: "steppingpack"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "internals"
weight: 507
toc: true
---

`steppingpack` is a space efficient wrapper around [MessagePack](https://msgpack.org/index.html).


Instead of:

<blockquote>

[Pydantic](https://docs.pydantic.dev/latest/) → JSON

</blockquote>

Think:

<blockquote>

`steppingpack` → MessagePack

</blockquote>

<br>


Space is saved by not storing the keys of values, so given:

```python
class Foo(st.Data):
    bar: int
    qux: str

foo = Foo(bar=1, qux="WOO")
```

instead of storing:

```json
{"bar": 1, "qux": "WOO"}
```

we store:

```json
[1, "WOO"]
```

## Usage

Given classes like:

```python
from stepping import steppingpack

class DataA(steppingpack.Data):
    a: str

class DataB(steppingpack.Data):
    x: int
    many: tuple[DataA, ...]
```

serialize:

```python
dumped: bytes = steppingpack.dump(DataB(...))
```

and deserialize:

```python
loaded: DataB  = steppingpack.load(DataB, dumped)
```

## Supported types

`steppingpack` can (de)serialize

```python
Atom = str | int | float | bool | None | date | datetime | UUID | Enum
Value = (
    Atom |
    tuple[Value, ...] |
    frozenset[Value] |
    st.ZSetPython[Value] |
    st.Pair[Value, Value] |
    Data
)
```

It also has support for:

- Recursive type definitions.
- Union types. Unions of `Data` must have a string `.st_discriminant` field to tell them apart, for example:

```python
class DataC(steppingpack.Data):
    st_discriminant: str = "DataC"
    ...
```

## Future

- _More types will be added, notably missing at the moment is `datetime.time`. Some kind of `FrozenDict` support would be nice too, especially if it played well with other types in `stepping.datatypes` (think hard about maintaining key order here and its applicability to serializing `ZSetPython`s too)._
- _Migrating between types in production will be added as part of [stepping manager]({{< ref "/docs/in-production/stepping-manager.md" >}} "Stepping manager")._
- _A version of [isjsonschemasubset](https://github.com/leontrolski/isjsonschemasubset) will be implemented to help with the above._
- _Nicer serialization of schemas to help with the above, using class name not the class itself as key, or maybe adding an `int` index to each of the schemas (maybe move the reference to the original types back out of the class to enable Pydanticification)._
- _Do more Rust-ification, especially for `dump_python` -- under the hood, `steppingpack` uses [ormsgpack](https://github.com/aviramha/ormsgpack), potentially even try upstream into that._
- _Move into its own separate project?_
