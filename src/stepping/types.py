from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from functools import cache
from itertools import islice
from types import NoneType
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
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

from stepping import graph, steppingpack


# fmt: off
class MatchAll:
    def __repr__(self) -> str: return "<MATCH_ALL>"
MATCH_ALL = MatchAll()
class Empty:
    def __repr__(self) -> str: return "<EMPTY>"
EMPTY = Empty()
IndexableAtom = str | int | float | bool | None | date | datetime | UUID
Indexable = IndexableAtom | tuple[IndexableAtom, ...]
Serializable = steppingpack.Value

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
KTuple = TypeVar("KTuple", bound=tuple[IndexableAtom])
TIndexable = TypeVar("TIndexable", bound=Indexable)
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
    def __neg__(self) -> Self: ...
    def __add__(self, other: ZSet[T]) -> Self: ...
    def iter(self, match: frozenset[T] | MatchAll = MATCH_ALL) -> Iterator[tuple[T, int]]: ...
    # This should be: (See bottom of file).
    # def iter_by_index(
    #     self, index: Index[T, K], match_keys: frozenset[K] | MatchAll = MATCH_ALL
    # ) -> Iterator[tuple[K, T, int]]: ...
    iter_by_index: _IterByIndex[T]

class Store(Protocol):
    def get(self, vertex: graph.VertexUnaryDelay[Any, Any], time: Time | None) -> ZSet[Any]: ...
    def set(self, vertex: graph.VertexUnaryDelay[Any, Any], value: Any, time: Time) -> None: ...
    def inc(self, time: Time) -> None: ...
    def flush(self, vertices: Iterable[graph.VertexUnaryDelay[Any, Any]], time: Time) -> None: ...

@runtime_checkable
class Transformer(Protocol):
    def lift(self, ret: type) -> type: ...
    def unlift(self, arg: type) -> type: ...
    def transform(self, g: graph.Graph[Any, Any]) -> graph.Graph[Any, Any]: ...
@runtime_checkable
class TransformerBuilder(Protocol):
    def from_arg_types(self, args: list[tuple[str, type]]) -> Transformer: ...
# fmt: on


@dataclass(frozen=True)
class Pair(Generic[T, U]):
    left: T
    right: U


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


@dataclass(frozen=True)
class Time:
    input_time: int = -1  # this set of inputs' time
    frontier: int = -1  # only read changes when written up to this time
    # Where
    # - True is flush every time we call `Store.set(...)`
    # - False is flush every time we call `Store.inc(...)`
    # - None is never flush
    flush_every_set: bool | None = False


@dataclass(frozen=True, eq=False)
class Index(Generic[T_co, K_co]):
    names: tuple[str, ...]
    ascending: tuple[bool, ...]
    f: Callable[[T_co], K_co]
    t: type[T_co]
    k: type[K_co]
    is_composite: bool

    @classmethod
    def atom(
        cls,
        name: str,
        t: type[T],
        k: type[KAtom],
        f: Callable[[T], KAtom],
        ascending: bool = True,
    ) -> Index[T, KAtom]:
        return Index(
            names=(name,),
            ascending=(ascending,),
            f=f,
            t=t,
            k=k,
            is_composite=False,
        )

    @classmethod
    def composite(
        cls,
        names: tuple[str, ...],
        t: type[T],
        k: type[KTuple],
        f: Callable[[T], KTuple],
        ascendings: tuple[bool, ...] | None = None,
    ) -> Index[T, KTuple]:
        assert len(names) == len(get_args(t))
        if ascendings is None:
            ascendings = (True,) * len(names)
        assert len(names) == len(ascendings)
        return Index(
            names=names,
            ascending=ascendings,
            f=f,
            t=t,
            k=k,
            is_composite=True,
        )

    @classmethod
    def identity(cls, t: type[KAtom], ascending: bool = True) -> Index[KAtom, KAtom]:
        return Index[KAtom, KAtom](
            names=("identity",),
            ascending=(ascending,),
            f=lambda t: t,
            t=t,
            k=t,
            is_composite=False,
        )

    @classmethod
    def pick(
        cls,
        t: type[T],
        f: Callable[[T], K],
        ascending: bool | tuple[bool, ...] = True,
    ) -> Index[T, K]:
        proxy = f(Proxy(t))  # type: ignore[arg-type]
        # If the we have like f=lambda a: (a, b, c)
        if isinstance(proxy, tuple):
            k = tuple[*(p.t for p in proxy)]  # type: ignore
            names = tuple(p.fields for p in proxy)
            is_composite = True
        # If the we have like f=lambda a: a.b, where a.b is a tuple
        elif is_type(proxy.t, tuple):  # type: ignore
            k = proxy.t  # type: ignore
            names = tuple(f"{proxy.fields}.{i}" for i, _ in enumerate(get_args(k)))  # type: ignore
            is_composite = True
        # If the we have like f=lambda a: a.b, where a.b is not a tuple
        else:
            k = proxy.t  # type: ignore
            names = (proxy.fields,)  # type: ignore
            is_composite = False

        if ascending is True or ascending is False:
            ascending = (ascending,) * (len(get_args(k)) if is_type(k, tuple) else 1)
        assert len(ascending) == len(names)

        return Index(
            names=names,
            ascending=ascending,
            f=f,
            t=t,
            k=k,
            is_composite=is_composite,
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Index):
            return False
        return (
            self.names,
            self.ascending,
            self.t,
            self.k,
        ) == (
            other.names,
            other.ascending,
            other.t,
            other.k,
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.names,
                self.ascending,
                self.t,
                self.k,
            )
        )


@dataclass
class Signature:
    args: list[tuple[str, type]]
    kwargs: dict[str, type]
    ret: type
    transformer: Transformer | None = None


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
