from __future__ import annotations

import inspect
import json
from dataclasses import dataclass, field, is_dataclass
from datetime import date, datetime
from types import NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Iterator,
    Protocol,
    Set,
    TypeVar,
    Union,
    get_args,
    get_origin,
    overload,
    runtime_checkable,
)
from uuid import UUID

from mashumaro.mixins.json import DataClassJSONMixin

from stepping import graph, serialize, types_helpers

# fmt: off

@runtime_checkable
class SerializableObject(Protocol):
    def identity(self) -> str: ...
    def serialize(self) -> Serialized: ...
    @classmethod
    def make_deserialize(cls: type[T]) -> Callable[[Serialized], T]: ...

class MatchAll:
    def __repr__(self) -> str: return "<MATCH_ALL>"
MATCH_ALL = MatchAll()
class Empty:
    def __repr__(self) -> str: return "<EMPTY>"
EMPTY = Empty()
IndexableAtom = str | int | float | bool | None | date | datetime | UUID
IndexableAtomTypes = str, int, float, bool, NoneType, date, datetime, UUID
Indexable = IndexableAtom | tuple[IndexableAtom, ...]
Serializable = IndexableAtom | SerializableObject | tuple['Serializable', ...] | list['Serializable']
Serialized   = str | int | float | bool | None | dict[str, 'Serialized'] | list['Serialized']

class Addable(Protocol):
    def __add__(self: T, other: T) -> T: ...
class Negable(Protocol):
    def __neg__(self: T) -> T: ...
class AddAndNegable(Protocol):
    def __add__(self: T, other: T) -> T: ...
    def __neg__(self: T) -> T: ...
class Reducable(Protocol):
    def __init__(self) -> None: ...
    def __add__(self: T, other: T, /) -> T: ...    # including where other is -ve
    def __mul__(self: T, other: int, /) -> T: ...  # including where other is -ve

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
X = TypeVar("X")
Y = TypeVar("Y")
K = TypeVar("K", bound=Indexable)
KAtom = TypeVar("KAtom", bound=IndexableAtom)
TSerializable = TypeVar("TSerializable", bound=Serializable)
VSerializable = TypeVar("VSerializable", bound=Serializable)
TAddable = TypeVar("TAddable", bound=Addable)
UAddable  = TypeVar("UAddable", bound=Addable)
VAddable  = TypeVar("VAddable", bound=Addable)
TNegable = TypeVar("TNegable", bound=Negable)
TAddAndNegable = TypeVar("TAddAndNegable", bound=AddAndNegable)
TReducable = TypeVar("TReducable", bound=Reducable)
Number = TypeVar("Number", int, float)

class ZSet(Protocol[T]):
    @property
    def indexes(self) -> tuple[Index[T, Indexable], ...]: ...
    def __neg__(self) -> ZSet[T]: ...
    def __add__(self, other: ZSet[T]) -> ZSet[T]: ...
    def iter(self) -> Iterator[tuple[T, int]]: ...
    def iter_by_index_generic(
        self, index: Index[T, Indexable], match_keys: tuple[Indexable, ...] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[Indexable, T, int]]: ...

class Store(Protocol):
    @property
    def _current(self) -> dict[graph.Vertex, ZSet[Any]]: ...
    def get(self, vertex: graph.Vertex) -> Any: ...
    def set(self, vertex: graph.Vertex, value: Any) -> None: ...
    def inc(self) -> None: ...

# fmt: on


@dataclass(frozen=True)
class Index(Generic[T, K]):
    fields: str | tuple[str, ...]
    ascending: bool | tuple[bool, ...]
    k: type[K]

    @property
    def generic(self) -> Index[T, Indexable]:
        return self  # type: ignore


@dataclass(frozen=True)
class Field(Generic[T, V]):
    fields: str | tuple[str, ...]
    k: type[V]


def pick_identity(t: type[KAtom], ascending: bool = True) -> Index[KAtom, KAtom]:
    return Index[KAtom, KAtom](
        "",
        ascending,
        t,
    )


def pick_index(
    t: type[T], f: Callable[[T], K], ascending: bool | tuple[bool, ...] = True
) -> Index[T, K]:
    fields = types_helpers.retrieve_fields()
    k = _type_from_fields(t, fields)
    if ascending is True:
        ascending = (True,) * len(get_args(k)) if is_type(k, tuple) else True
    if ascending is False:
        ascending = (False,) * len(get_args(k)) if is_type(k, tuple) else False
    return Index(
        fields,
        ascending,
        k,  # type: ignore
    )


def pick_field(t: type[T], f: Callable[[T], V]) -> Field[T, V]:
    fields = types_helpers.retrieve_fields()
    k = _type_from_fields(t, fields)
    return Field(
        fields,
        k,  # type: ignore
    )


# fmt: off
@overload
def choose(index: Index[T, K], v: T) -> K: ...
@overload
def choose(index: Field[T, V], v: T) -> V: ...
# fmt: on
def choose(index: Index[T, K] | Field[T, V], v: T) -> K | V:
    if isinstance(index.fields, tuple):
        return tuple(_choose(v, f) for f in index.fields)  # type: ignore
    return _choose(v, index.fields)  # type: ignore


def _choose(v: Any, field: str) -> Indexable:
    if field == "":
        return v  # type: ignore
    out = v
    for f in field.split("."):
        if f.isdigit():
            out = out[int(f)]
        else:
            out = getattr(out, f)
    return out  # type: ignore


@dataclass(frozen=True)
class Data(DataClassJSONMixin):
    def identity(self) -> str:
        return json.dumps(self.serialize(), separators=(",", ":"), sort_keys=True)

    def serialize(self) -> dict[str, Serialized]:
        return json.loads(super().to_json())  # type: ignore
        # should be: return super().to_dict() - not sure why not working...

    @classmethod
    def make_deserialize(cls: type[T]) -> Callable[[Serialized], T]:
        def inner(data: Serialized) -> T:
            assert isinstance(data, dict)
            return cls.from_dict(data)  # type: ignore[no-any-return,attr-defined]

        return inner


@dataclass(frozen=True)
class Pair(Generic[T, U], Data):
    left: T
    right: U

    def identity(self) -> str:
        return (
            serialize.make_identity(self.left)  # type:ignore[arg-type]
            + ","
            + serialize.make_identity(self.right)  # type:ignore[arg-type]
        )

    def serialize(self) -> dict[str, Serialized]:
        return dict(
            left=serialize.serialize(self.left),  # type:ignore[arg-type]
            right=serialize.serialize(self.right),  # type:ignore[arg-type]
        )

    @classmethod
    def make_deserialize(cls: type[Pair[T, U]]) -> Callable[[Serialized], Pair[T, U]]:
        left_type, right_type = get_args(cls)

        def inner(n: Serialized) -> Pair[T, U]:
            assert isinstance(n, dict)
            out: TSerializable = Pair(  # type: ignore
                left=serialize.deserialize(left_type, n["left"]),
                right=serialize.deserialize(right_type, n["right"]),
            )
            return out

        return inner


@dataclass
class Grouped(Generic[T, K]):
    t: type[T]
    k: type[K]
    _data: dict[K, T] = field(default_factory=dict)

    def set(self, k: K, v: T) -> None:
        self._data[k] = v

    def get(self, k: K) -> T | Empty:
        if k in self._data:
            return self._data[k]
        return EMPTY

    def iter(self) -> Iterator[tuple[K, T]]:
        for k, v in self._data.items():
            yield k, v

    def keys(self) -> Set[K]:
        return set(self._data.keys())


def is_type(t: type, check: type) -> bool:
    t = strip_annotated(t)
    return get_origin(t) is check


def get_annotation_grouped(t: type[Grouped[T, K]]) -> type[T]:
    assert is_type(t, Grouped)
    t_grouped, _ = get_args(t)
    return t_grouped  # type: ignore


def get_annotation_grouped_zset(
    t: type[Grouped[ZSet[T], K]]
) -> tuple[type[T], type[K]]:
    assert is_type(t, Grouped)
    zt, k = get_args(t)
    assert is_type(zt, ZSet)
    (t,) = get_args(zt)
    return t, k  # type: ignore


def get_annotation_indexes(
    t: type[ZSet[T]],
) -> tuple[type[T], tuple[Index[T, Indexable], ...]]:
    assert is_type(t, ZSet)
    if get_origin(t) is Annotated:
        t, *indexes = get_args(t)
        (inner_t,) = get_args(t)
        return inner_t, tuple(indexes)
    (inner_t,) = get_args(t)
    return inner_t, ()


def strip_annotated(t: T) -> T:
    origin = get_origin(t)
    if origin is None:
        return t
    args = get_args(t)
    if origin is Annotated:
        inner_type, *_ = args
        return strip_annotated(inner_type)  # type: ignore
    if origin is UnionType:
        origin = Union
    return origin[*(strip_annotated(inner_type) for inner_type in args)]  # type: ignore


@dataclass
class RuntimeComposite(Generic[T]):
    @classmethod
    def sub(cls, **replace: Any) -> type[T]:
        called_from = inspect.stack()[1]
        code = types_helpers.call_site_code(called_from)
        try:
            slice = code.body[0].value.func.value.slice  # type: ignore
        except SyntaxError:
            raise RuntimeError("Couldn't parse the call-site to .sub()")
        builtins = called_from.frame.f_globals["__builtins__"]
        scope = dict(called_from.frame.f_globals) | builtins
        scope.update(called_from.frame.f_locals)
        for k, v in replace.items():
            scope[k] = v
        return types_helpers.from_ast(scope, slice)


def _type_from_fields(t: type[T], fields: str | tuple[str, ...]) -> type[Indexable]:
    if isinstance(fields, tuple):
        return tuple[*(_type_from_parts(t, f.split(".")) for f in fields)]  # type: ignore
    return _type_from_parts(t, fields.split("."))


def _type_from_parts(t: type[Any] | UnionType, parts: list[str]) -> type[Indexable]:
    t = strip_annotated(t)
    original_t = get_origin(t) or t

    if is_dataclass(original_t):
        assert not isinstance(t, UnionType)
        first, *parts = parts
        inner_type = types_helpers.name_type_map_from_dataclass(t)[first]
        return _type_from_parts(inner_type, parts)

    if original_t is tuple and parts:
        first, *parts = parts
        assert first.isdigit()
        inner_type = get_args(t)[int(first)]
        return _type_from_parts(inner_type, parts)

    if get_origin(t) is Union or isinstance(t, UnionType):
        inner_types = set(get_args(t))
        if {int, float, bool} & inner_types and (
            (len(inner_types) == 2 and inner_types - {int, float, bool} != {NoneType})
            or len(inner_types) != 1
        ):
            raise RuntimeError(
                "Postgres won't index JSON on (ints | float | bool) | Other"
            )
        for inner_type in inner_types:
            assert inner_type in {NoneType, str, date, datetime, UUID}
        return t  # type: ignore

    if original_t is not tuple and t not in IndexableAtomTypes:
        raise RuntimeError(
            f"Expected to terminate on an IndeaxbleAtom type, instead saw: {t}"
        )

    if len(parts) != 0:
        raise RuntimeError(
            f"We've hit an atom type, but still have parts of the "
            f"field: '{'.'.join(parts)}' left to consume"
        )

    return t
