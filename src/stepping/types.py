from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import islice
from types import NoneType
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Protocol,
    Self,
    Set,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)
from uuid import UUID

import pydantic

from stepping import graph, serialize

# fmt: off

@runtime_checkable
class SerializableObject(Protocol):
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
Z = TypeVar("Z")
K = TypeVar("K", bound=Indexable)
T_co = TypeVar("T_co", covariant=True)
K_co = TypeVar("K_co", bound=Indexable, covariant=True)
KAtom = TypeVar("KAtom", bound=IndexableAtom)
TIndexable = TypeVar("TIndexable", bound=Indexable)
TSerializable = TypeVar("TSerializable", bound=Serializable)
TSerializable_co = TypeVar("TSerializable_co", bound=Serializable)
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
    def __neg__(self) -> Self: ...
    def __add__(self, other: ZSet[T]) -> Self: ...
    def iter(self, match: frozenset[T] | MatchAll = MATCH_ALL) -> Iterator[tuple[T, int]]: ...
    # This should be: (See bottom of file).
    # def iter_by_index(
    #     self, index: Index[T, K], match_keys: frozenset[K] | MatchAll = MATCH_ALL
    # ) -> Iterator[tuple[K, T, int]]: ...
    iter_by_index: _IterByIndex[T]

class Store(Protocol):
    @property
    def _current(self) -> dict[graph.VertexUnaryDelay[Any, Any], ZSet[Any]]: ...
    def get(self, vertex: graph.VertexUnaryDelay[Any, Any]) -> ZSet[Any]: ...
    def set(self, vertex: graph.VertexUnaryDelay[Any, Any], value: Any) -> None: ...
    def inc(self, flush: bool) -> None: ...

@runtime_checkable
class Transformer(Protocol):
    def lift(self, ret: type) -> type: ...
    def unlift(self, arg: type) -> type: ...
    def transform(self, g: graph.Graph[Any, Any]) -> graph.Graph[Any, Any]: ...
@runtime_checkable
class TransformerBuilder(Protocol):
    def from_arg_types(self, args: list[tuple[str, type]]) -> Transformer: ...
# fmt: on


@dataclass(frozen=True, eq=False)
class Index(Generic[T_co, K_co]):
    fields: str | tuple[str, ...]
    ascending: bool | tuple[bool, ...]
    f: Callable[[T_co], K_co]
    t: type[T_co]
    k: type[K_co]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Index):
            return False
        return (
            self.fields,
            self.ascending,
            self.t,
            self.k,
        ) == (
            other.fields,
            other.ascending,
            other.t,
            other.k,
        )


class Data(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)

    def serialize(self) -> dict[str, Serialized]:
        return self.model_dump(mode="json")

    @classmethod
    def make_deserialize(cls: type[T]) -> Callable[[Serialized], T]:
        def inner(data: Serialized) -> T:
            assert isinstance(data, dict)
            return cls(**data)

        return inner


@dataclass(frozen=True)
class Pair(Generic[T, U]):
    left: T
    right: U

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
    _data: dict[K, T] = field(default_factory=dict)

    def set(self, k: K, v: T) -> None:
        self._data[k] = v

    def get(self, k: K) -> T | Empty:
        if k in self._data:
            return self._data[k]
        return EMPTY

    def iter(self) -> Iterator[tuple[T, K]]:
        for k, v in self._data.items():
            yield v, k

    def keys(self) -> Set[K]:
        return set(self._data.keys())


@dataclass
class Signature:
    args: list[tuple[str, type]]
    kwargs: dict[str, type]
    ret: type
    transformer: Transformer | None = None


def pick_identity(t: type[KAtom], ascending: bool = True) -> Index[KAtom, KAtom]:
    return Index[KAtom, KAtom](
        "",
        ascending,
        lambda t: t,
        t,
        t,
    )


def pick_index(
    t: type[T], f: Callable[[T], K], ascending: bool | tuple[bool, ...] = True
) -> Index[T, K]:
    proxy = f(Proxy(t))  # type: ignore[arg-type]
    if isinstance(proxy, tuple):
        k = tuple[*(p.t for p in proxy)]  # type: ignore
        fields = tuple(p.fields for p in proxy)
    else:
        k = proxy.t  # type: ignore
        fields = proxy.fields  # type: ignore

    if ascending is True:
        ascending = (True,) * len(get_args(k)) if is_type(k, tuple) else True
    if ascending is False:
        ascending = (False,) * len(get_args(k)) if is_type(k, tuple) else False
    return Index(
        fields,
        ascending,
        f,
        t,
        k,
    )


def is_type(t: type, check: type) -> bool:
    return get_origin(t) is check


def get_annotation_zset(t: type[ZSet[T]]) -> type[T]:
    assert is_type(t, ZSet)
    (t_zsetted,) = get_args(t)
    return t_zsetted  # type: ignore


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


# https://docs.python.org/3/library/itertools.html#itertools-recipes
def batched(iterable: list[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


# Proxy


def _get_generic_args(t: type) -> tuple[type, ...]:
    """Get the arguments of the parent Generic.

    Pair -> [T, U]

    """
    parent_class_generic = next(
        b
        for a, b in zip(t.__bases__, t.__orig_bases__)  # type: ignore
        if issubclass(a, Generic)  # type: ignore
    )
    return get_args(parent_class_generic)


def _name_type_map_from_dataclass(t: type) -> dict[str, type]:
    """Inspect a dataclass and return a map of field name to type.

    Pair[User, Meter] -> {'left': User, 'right': Meter}

    """
    original_t = get_origin(t) or t
    name_type_map = get_type_hints(original_t)
    if t != original_t:
        generic_args = _get_generic_args(original_t)
        assert len(generic_args) == len(get_args(t))
        generic_specific_map = dict(zip(generic_args, get_args(t)))
        name_type_map = {k: generic_specific_map[v] for k, v in name_type_map.items()}
    return name_type_map


@dataclass(frozen=True)
class Proxy:
    t: type
    _path: tuple[str | int, ...] = ()

    @property
    def fields(self) -> str:
        return ".".join(str(n) for n in self._path)

    def __getattr__(self, key: str) -> Proxy:
        assert isinstance(key, str)
        return Proxy(_name_type_map_from_dataclass(self.t)[key], self._path + (key,))

    def __getitem__(self, key: int) -> Proxy:
        assert isinstance(key, int)
        return Proxy(get_args(self.t)[key], self._path + (key,))


# Bodge for https://stackoverflow.com/questions/77075322/


class ZSetBodgeMeta(type):
    def __new__(cls, name, bases, d):  # type: ignore
        if "_iter_by_index" in d:
            d["iter_by_index"] = d["_iter_by_index"]
        return super().__new__(cls, name, bases, d)


class ZSetBodge(Generic[T], metaclass=ZSetBodgeMeta):
    iter_by_index: _IterByIndex[T]


class _IterByIndex(Protocol[T]):
    def __call__(
        self, index: Index[T, K], match_keys: frozenset[K] | MatchAll = MATCH_ALL
    ) -> Iterator[tuple[K, T, int]]:
        ...
