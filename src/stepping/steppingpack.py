from __future__ import annotations

import hashlib
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from functools import cache
from types import NoneType, UnionType
from typing import TYPE_CHECKING, Any
from typing import Literal as L
from typing import TypeVar, dataclass_transform, get_args, get_origin, get_type_hints
from unittest.mock import ANY
from uuid import UUID

import ormsgpack
import pydantic

from stepping import types
from stepping.zset import python as zset_python


def dump(o: Value) -> bytes:
    if hasattr(o, "st_bytes") and isinstance(o.st_bytes, bytes):  # type: ignore
        return o.st_bytes  # type: ignore

    dumped_python = dump_python(o)
    # Use ormsgpack.OPT_SORT_KEYS if we ever implement dict support
    return ormsgpack.packb(dumped_python, option=ormsgpack.OPT_UTC_Z)


def load(t: type[TValue], b: bytes) -> TValue:
    schema = make_schema(t)  # type: ignore[arg-type]

    if id(schema) not in _SCHEMA_ID_D_CACHE:
        _SCHEMA_ID_D_CACHE[id(schema)] = _s_to_d(schema)
    d_schema = _SCHEMA_ID_D_CACHE[id(schema)]

    o = ormsgpack.unpackb(b)
    return load_python(d_schema, o)  # type: ignore


class Meta(type):
    def __new__(_cls, *args: Any, **kwargs: Any) -> type[Data]:
        cls: type[Data] = super().__new__(_cls, *args, **kwargs)
        if cls.__module__ == "stepping.steppingpack":  # skip anything in this module
            return cls
        cls = dataclass(kw_only=True, order=True)(cls)
        make_schema(cls)  # immediately make schema and cache
        cls.__hash__ = lambda self: self.st_hash  # type: ignore
        cls.st_field_names = tuple(f.name for f in fields(cls))  # type: ignore[arg-type]
        return cls


@dataclass_transform(kw_only_default=True)
class Data(metaclass=Meta):
    st_bytes: bytes = field(init=False, compare=False, repr=False)
    st_hash: int = field(init=False, compare=False, repr=False)
    st_identifier: UUID = field(init=False, compare=False, repr=False)
    st_field_names: tuple[str, ...] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        self.st_bytes = dump(self)
        md5 = hashlib.md5()
        self.st_hash = hash(self.st_bytes)
        md5.update(self.st_bytes)
        self.st_identifier = UUID(md5.hexdigest())


@cache
def make_schema(t: type[Value] | None) -> SType:
    args = get_args(t)
    origin: type | None = get_origin(t)
    if origin is None:
        origin = t
    schema: SType
    if origin is None or issubclass(origin, NoneType):
        schema = SAtom(type="none")
    elif issubclass(origin, str):
        schema = SAtom(type="str")
    elif issubclass(origin, int):
        schema = SAtom(type="int")
    elif issubclass(origin, float):
        schema = SAtom(type="float")
    elif issubclass(origin, bool):
        schema = SAtom(type="bool")
    elif issubclass(origin, datetime):
        schema = SAtom(type="datetime")
    elif issubclass(origin, date):
        schema = SAtom(type="date")
    elif issubclass(origin, UUID):
        schema = SAtom(type="uuid")
    elif issubclass(origin, Enum):
        values: list[int | bool] = [n.value for n in t]  # type: ignore
        literals = tuple(
            SLiteral(value=_assert_atom(make_schema(type(v))), literal=v)  # type: ignore[arg-type]
            for v in values
        )
        schema = SUnion(type="enum", options=literals)
    elif issubclass(origin, tuple):
        (u, ellipsis) = args
        assert ellipsis is ...
        schema = STuple(value=make_schema(u))
    elif issubclass(origin, frozenset):
        (u,) = args
        schema = SFrozenset(value=make_schema(u))
    elif issubclass(origin, zset_python.ZSetPython):
        (u,) = args
        schema = SZSet(value=make_schema(u))
    elif issubclass(origin, types.Pair):
        (u, v) = args
        schema = SPair(left=make_schema(u), right=make_schema(v))
    elif issubclass(origin, UnionType):
        options = tuple(make_schema(n) for n in args)
        seen_discriminants = set[str]()
        for option in options:
            if isinstance(option, SAtom):
                continue
            elif isinstance(option, SData):
                d = option.discriminant
                if d is None or d in seen_discriminants:
                    raise RuntimeError(
                        f"st.Data must have unique .{DISCRIMINANT_FIELD_NAME}"
                    )
                seen_discriminants.add(d)
            else:
                raise NotImplementedError("Only implements Unions for Atoms and Data")
        schema = SUnion(type="union", options=options)
    elif issubclass(origin, Data):
        type_map = get_type_hints(t)
        pairs = tuple[SDataPair, ...]()
        discriminant = None
        for field in fields(t):  # type: ignore[arg-type]
            u = make_schema(type_map[field.name])
            default = SNoValue()
            if field.default is not MISSING:
                default = field.default
            if field.name == DISCRIMINANT_FIELD_NAME:
                assert isinstance(default, str)
                discriminant = default
            pairs += (SDataPair(name=field.name, value=u, default=default),)
        schema = SData(pairs=pairs, discriminant=discriminant)
    else:
        raise NotImplementedError(f"No handler for type: {t}")

    schema._original_cls = t
    return schema


def _assert_atom(s: SType) -> SAtom:
    if not isinstance(s, SAtom):
        raise RuntimeError("Only atom keys supported")
    return s


DISCRIMINANT_FIELD_NAME = "st_discriminant"


class _SBase(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    _original_cls: type[Value] | None = ANY  # makes testing easier


class SNoValue(_SBase):
    type: L["novalue"] = "novalue"


class SAtom(_SBase):
    type: (
        L["str"]
        | L["int"]
        | L["float"]
        | L["bool"]
        | L["none"]
        | L["date"]
        | L["datetime"]
        | L["uuid"]
    )


class SLiteral(_SBase):
    type: L["literal"] = "literal"
    value: SAtom
    literal: Atom


class STuple(_SBase):
    type: L["tuple"] = "tuple"
    value: SType


class SFrozenset(_SBase):
    type: L["frozenset"] = "frozenset"
    value: SType


class SZSet(_SBase):
    type: L["zset"] = "zset"
    value: SType


class SPair(_SBase):
    type: L["pair"] = "pair"
    left: SType
    right: SType


class SUnion(_SBase):
    type: L["union"] | L["enum"]  # if enum, we will convert to enum on deserializing
    options: tuple[SType, ...]


class SDataPair(_SBase):
    type: L["pair"] = "pair"
    name: str
    value: SType
    default: Atom | SNoValue = SNoValue()


class SData(_SBase):
    type: L["data"] = "data"
    pairs: tuple[SDataPair, ...]  # note these are _ordered_
    discriminant: str | None = None


Atom = str | int | float | bool | None | date | datetime | UUID | Enum
Value = (
    Atom
    | tuple["Value", ...]
    | frozenset["Value"]
    | zset_python.ZSetPython["Value"]
    | types.Pair["Value", "Value"]
    | Data
)
TValue = TypeVar("TValue", bound=Value)
SType = SAtom | SLiteral | STuple | SFrozenset | SZSet | SPair | SUnion | SData


def dump_python(o: Value) -> Any:
    if o is None or isinstance(o, (str, int, float, bool, date, datetime, UUID, Enum)):
        return o
    if isinstance(o, (frozenset, tuple)):
        return sorted(dump_python(v) for v in o)
    if isinstance(o, zset_python.ZSetPython):
        return sorted((dump_python(v), c) for v, c in o.iter())
    if isinstance(o, types.Pair):
        return (dump_python(o.left), dump_python(o.right))
    if isinstance(o, Data):
        return [dump_python(getattr(o, f)) for f in o.st_field_names]
    raise NotImplementedError(f"No handler for value: {o}")


def load_python(s: DType, o: Value) -> Any:
    if type(s) is DAtom:  # quicker than isinstance for pydantic classes
        if s.type == "date":
            if TYPE_CHECKING:
                assert isinstance(o, str)
            return date.fromisoformat(o)
        if s.type == "datetime":
            if TYPE_CHECKING:
                assert isinstance(o, str)
            return datetime.fromisoformat(o)
        if s.type == "uuid":
            if TYPE_CHECKING:
                assert isinstance(o, str)
            return UUID(o)
        return o
    if type(s) is DLiteral:
        return load_python(s.value, o)
    if type(s) is DTuple:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return tuple(load_python(s.value, v) for v in o)
    if type(s) is DFrozenset:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return frozenset(load_python(s.value, v) for v in o)
    if type(s) is DZSet:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return zset_python.ZSetPython(iter(o))
    if type(s) is DPair:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return types.Pair(
            left=load_python(s.left, o[0]),
            right=load_python(s.right, o[1]),
        )
    if type(s) is DUnion:
        if s.type == "enum":
            if TYPE_CHECKING:
                assert isinstance(o, (int, str))
                assert s.st_original_cls is not None and issubclass(
                    s.st_original_cls, Enum
                )
            return s.st_original_cls(o)
        return _load_union(s.options, o)
    if type(s) is DData:
        if TYPE_CHECKING:
            assert isinstance(o, list)
            assert s.st_original_cls is not None and is_dataclass(s.st_original_cls)
        kw = {pair.name: load_python(pair.value, v) for pair, v in zip(s.pairs, o)}
        return s.st_original_cls(**kw)

    raise NotImplementedError(f"No handler for schema: {s}")


def _load_union(schemas: tuple[DType, ...], o: Value) -> Any:
    atoms = list[DAtom]()
    data = list[DData]()
    for s in schemas:
        if isinstance(s, DAtom):
            atoms.append(s)
        elif isinstance(s, DData):
            data.append(s)
        else:
            raise NotImplementedError("Only implements Unions for Atoms and Data")

    for s in atoms:
        try:
            return load_python(s, o)
        except ValueError:
            pass
    for s in data:
        assert isinstance(o, list)
        if o[s.discriminant_index] == s.discriminant:
            return load_python(s, o)

    raise RuntimeError(f"Unable to deserialize given schemas: {schemas}")


# Irritating duplication for performance


def _s_to_d(s: SType) -> DType:
    s_d_map = {
        SAtom: DAtom,
        SLiteral: DLiteral,
        STuple: DTuple,
        SFrozenset: DFrozenset,
        SZSet: DZSet,
        SPair: DPair,
        SUnion: DUnion,
        SDataPair: DDataPair,
        SData: DData,
    }
    kwargs: dict[str, Any] = {"st_original_cls": s._original_cls}
    for f in s.model_fields:
        sub = getattr(s, f)
        if isinstance(sub, SNoValue):
            kwargs[f] = DNoValue()
        elif isinstance(sub, _SBase):
            kwargs[f] = _s_to_d(sub)  # type:ignore[arg-type]
        elif isinstance(sub, tuple):
            kwargs[f] = tuple(_s_to_d(n) for n in sub)
        else:
            kwargs[f] = sub
    return s_d_map[type(s)](**kwargs)  # type: ignore


@dataclass(slots=True)
class _DBase:
    type: str
    st_original_cls: type[Value]  # type: ignore


@dataclass(slots=True, frozen=True)
class DNoValue:
    ...


@dataclass(slots=True)
class DAtom(_DBase):
    ...


@dataclass(slots=True)
class DLiteral(_DBase):
    value: DAtom
    literal: Atom


@dataclass(slots=True)
class DTuple(_DBase):
    value: DType


@dataclass(slots=True)
class DFrozenset(_DBase):
    value: DType


@dataclass(slots=True)
class DZSet(_DBase):
    value: DType


@dataclass(slots=True)
class DPair(_DBase):
    left: DType
    right: DType


@dataclass(slots=True)
class DUnion(_DBase):
    options: tuple[DType, ...]


@dataclass(slots=True)
class DDataPair(_DBase):
    name: str
    value: DType
    default: Atom | DNoValue = DNoValue()


@dataclass(slots=True)
class DData(_DBase):
    pairs: tuple[DDataPair, ...]  # note these are _ordered_
    discriminant: str | None = None
    discriminant_index: int = -1

    def __post_init__(self) -> None:
        self.discriminant_index = next(
            (
                i
                for i, pair in enumerate(self.pairs)
                if pair.name == DISCRIMINANT_FIELD_NAME
            ),
            -1,
        )


DType = DAtom | DLiteral | DTuple | DFrozenset | DZSet | DPair | DUnion | DData
_SCHEMA_ID_D_CACHE = dict[int, DType]()
