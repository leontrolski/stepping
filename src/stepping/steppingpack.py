from __future__ import annotations

import hashlib
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
from functools import cache
from types import NoneType, UnionType
from typing import TYPE_CHECKING, Any
from typing import Literal as L
from typing import (
    TypeVar,
    Union,
    cast,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)
from unittest.mock import ANY
from uuid import UUID

import ormsgpack

# fmt:off
if TYPE_CHECKING:
    from stepping import types
    from stepping.zset import python as zset_python
HAS_IMPORTED = False
def _import() -> None:  # only import at run-time to prevent cycles
    global HAS_IMPORTED
    if HAS_IMPORTED:
        return
    global types
    global zset_python
    from stepping import types as types
    from stepping.zset import python as zset_python
    HAS_IMPORTED = True
# fmt: on


def dump(o: Value) -> bytes:
    if hasattr(o, "st_bytes") and isinstance(o.st_bytes, bytes):  # type: ignore
        return o.st_bytes  # type: ignore

    dumped_python = dump_python(o)
    # Use ormsgpack.OPT_SORT_KEYS if we ever implement dict support
    return ormsgpack.packb(dumped_python, option=ormsgpack.OPT_UTC_Z)


def dump_python(o: Value) -> ValuePython:
    if hasattr(o, "st_bytes") and isinstance(o.st_bytes, bytes):  # type: ignore
        return ormsgpack.unpackb(o.st_bytes)  # type: ignore

    _import()
    if o is None or isinstance(o, (str, int, float, bool, date, UUID, Enum)):
        return o
    if isinstance(o, datetime):
        return o.astimezone(timezone.utc)
    if isinstance(o, tuple):
        return [dump_python(v) for v in o]
    if isinstance(o, frozenset):
        # Warning: these sorteds aren't safe for union types
        return sorted(dump_python(v) for v in o)  # type: ignore[type-var]
    if isinstance(o, zset_python.ZSetPython):
        return sorted([dump_python(v), c] for v, c in o.iter())  # type: ignore[return-value]
    if isinstance(o, types.Pair):
        return [dump_python(o.left), dump_python(o.right)]
    if isinstance(o, Data):
        return [dump_python(getattr(o, f)) for f in o.st_field_names]
    raise NotImplementedError(f"No handler for value: {o}")


def dump_indexable(o: types.Indexable) -> ValueJSON:
    dumped_bytes = ormsgpack.packb(o)
    dumped_json: ValueJSON = ormsgpack.unpackb(dumped_bytes)
    return dumped_json


def load(t: type[TValue], o: bytes | ValueJSON) -> TValue:
    schema = make_schema(t)  # type: ignore[arg-type]
    if isinstance(o, bytes):
        o = ormsgpack.unpackb(o)
    return _load(schema, o)  # type: ignore


IDENTITYLESS = (int, float, str, bool, UUID, date, datetime)


def make_identity(o: Value | tuple[Value, ...]) -> bytes:
    _import()

    assert not isinstance(o, IDENTITYLESS)
    if isinstance(o, (tuple, frozenset)):
        md5 = hashlib.md5()
        for n in o if isinstance(o, tuple) else sorted(o):  # type: ignore[type-var]
            md5.update(dump(n))
        return md5.digest()
    if isinstance(o, (types.Pair, zset_python.ZSetPython)):
        md5 = hashlib.md5()
        md5.update(dump(o))
        return md5.digest()
    if isinstance(o, Data):
        return o.st_identifier
    raise RuntimeError(f"Value of unknown type: {o}")


class Meta(type):
    def __new__(_cls, *args: Any, **kwargs: Any) -> type[Data]:
        cls: type[Data] = super().__new__(_cls, *args, **kwargs)
        if cls.__module__ == "stepping.steppingpack":  # skip anything in this module
            return cls
        cls = dataclass(kw_only=True, order=True)(cls)
        cls.__hash__ = lambda self: self.st_hash  # type: ignore
        cls.st_field_names = tuple(f.name for f in fields(cls))  # type: ignore[arg-type]
        return cls


def hash_self(self: Value) -> tuple[bytes, int, bytes]:
    st_bytes = dump(self)
    md5 = hashlib.md5()
    st_hash = hash(st_bytes)
    md5.update(st_bytes)
    st_identifier = md5.digest()
    return st_bytes, st_hash, st_identifier


@dataclass_transform(kw_only_default=True)
class Data(metaclass=Meta):
    st_bytes: bytes = field(init=False, compare=False, repr=False)
    st_hash: int = field(init=False, compare=False, repr=False)
    st_identifier: bytes = field(init=False, compare=False, repr=False)
    st_field_names: tuple[str, ...] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        make_schema(self.__class__)  # immediately make schema and cache
        self.st_bytes, self.st_hash, self.st_identifier = hash_self(self)


_TYPE_MAP: dict[type[Value], SType | None] = {}


@cache
def make_schema(t: type[Value] | None) -> SType:
    # Handle recursion
    if t in _TYPE_MAP:
        return t
    _TYPE_MAP[t] = None  # type: ignore

    _import()
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
        if len(args) == 2 and args[1] is ...:
            schema = STuple(values=(make_schema(args[0]),), many=True)
        else:
            schema = STuple(values=tuple(make_schema(u) for u in args), many=False)
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
            if isinstance(option, (SAtom, STuple)):
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
        discriminant_index = -1
        for i, field in enumerate(fields(t)):  # type: ignore[arg-type]
            u = make_schema(type_map[field.name])
            default = SNoValue()
            if field.default is not MISSING:
                default = field.default
            if field.name == DISCRIMINANT_FIELD_NAME:
                assert isinstance(default, str)
                discriminant = default
                discriminant_index = i
            pairs += (SDataPair(name=field.name, value=u, default=default),)
        schema = SData(
            pairs=pairs,
            discriminant=discriminant,
            discriminant_index=discriminant_index,
        )
    else:
        raise NotImplementedError(f"No handler for type: {t}")

    schema.st_original_cls = t
    _TYPE_MAP[t] = schema  # type: ignore
    return schema


def _assert_atom(s: SType) -> SAtom:
    if not isinstance(s, SAtom):
        raise RuntimeError("Only atom keys supported")
    return s


DISCRIMINANT_FIELD_NAME = "st_discriminant"


@dataclass(kw_only=True)
class _SBase:
    st_original_cls: type[Value] | None = field(
        init=False,
        compare=False,
        repr=False,
        default_factory=lambda: ANY,  # makes testing easier
    )


@dataclass(kw_only=True)
class SNoValue(_SBase):
    type: L["novalue"] = "novalue"


@dataclass(kw_only=True)
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


@dataclass(kw_only=True)
class SLiteral(_SBase):
    type: L["literal"] = "literal"
    value: SAtom
    literal: Atom


@dataclass(kw_only=True)
class STuple(_SBase):
    type: L["tuple"] = "tuple"
    values: tuple[SType, ...]
    many: bool


@dataclass(kw_only=True)
class SFrozenset(_SBase):
    type: L["frozenset"] = "frozenset"
    value: SType


@dataclass(kw_only=True)
class SZSet(_SBase):
    type: L["zset"] = "zset"
    value: SType


@dataclass(kw_only=True)
class SPair(_SBase):
    type: L["pair"] = "pair"
    left: SType
    right: SType


@dataclass(kw_only=True)
class SUnion(_SBase):
    type: L["union"] | L["enum"]  # if enum, we will convert to enum on deserializing
    options: tuple[SType, ...]


@dataclass(kw_only=True)
class SDataPair(_SBase):
    type: L["pair"] = "pair"
    name: str
    value: SType
    default: Atom | SNoValue = field(default_factory=SNoValue)


@dataclass(kw_only=True)
class SData(_SBase):
    type: L["data"] = "data"
    pairs: tuple[SDataPair, ...]  # note these are _ordered_
    discriminant: str | None = None
    discriminant_index: int = -1


Atom = str | int | float | bool | None | date | datetime | UUID | Enum
Value = Union[
    Atom,
    tuple["Value", ...],
    frozenset["Value"],
    "zset_python.ZSetPython[Value]",
    "types.Pair[Value, Value]",
    Data,
]
ValuePython = Atom | list["ValuePython"]
ValueJSON = str | int | float | bool | None | list["ValueJSON"]
TValue = TypeVar("TValue", bound=Value)
SType = (
    SAtom
    | SLiteral
    | STuple
    | SFrozenset
    | SZSet
    | SPair
    | SUnion
    | SData
    # In the case of recursive types, we look this up in `_TYPE_MAP`
    | type[Value]
)


def _load(s: SType, o: ValueJSON) -> Value:
    _import()
    if type(s) is SAtom:  # quicker than isinstance
        if isinstance(o, list):
            raise ValueError
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
    if type(s) is SLiteral:
        return _load(s.value, o)
    if type(s) is STuple:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        if s.many:
            return tuple(_load(s.values[0], v) for v in o)
        else:
            assert len(o) == len(s.values)
            return tuple(_load(value, v) for value, v in zip(s.values, o))
    if type(s) is SFrozenset:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return frozenset(_load(s.value, v) for v in o)
    if type(s) is SZSet:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        l = cast(list[tuple[ValueJSON, int]], o)
        return zset_python.ZSetPython((_load(s.value, v), c) for v, c in l)
    if type(s) is SPair:
        if TYPE_CHECKING:
            assert isinstance(o, list)
        return types.Pair(
            left=_load(s.left, o[0]),
            right=_load(s.right, o[1]),
        )
    if type(s) is SUnion:
        if s.type == "enum":
            if TYPE_CHECKING:
                assert isinstance(o, (int, str))
                assert s.st_original_cls is not None and issubclass(
                    s.st_original_cls, Enum
                )
            return s.st_original_cls(o)
        return _load_union(s.options, o)
    if type(s) is SData:
        if TYPE_CHECKING:
            assert isinstance(o, list)
            assert s.st_original_cls is not None and is_dataclass(s.st_original_cls)
        kw = {pair.name: _load(pair.value, v) for pair, v in zip(s.pairs, o)}
        return s.st_original_cls(**kw)  # type: ignore[return-value]
    # Handle recursive types
    if s in _TYPE_MAP:
        s_inner = _TYPE_MAP[s]  # type: ignore[index]
        assert s_inner is not None
        return _load(s_inner, o)

    raise NotImplementedError(f"No handler for schema: {s}")


def _load_union(schemas: tuple[SType, ...], o: ValueJSON) -> Value:
    atoms = list[SAtom]()
    tuples = list[STuple]()
    data = list[SData]()
    for s in schemas:
        if isinstance(s, SAtom):
            atoms.append(s)
        elif isinstance(s, STuple):
            tuples.append(s)
        elif isinstance(s, SData):
            data.append(s)
        else:
            raise NotImplementedError("Only implements Unions for Atoms and Data")

    for s in atoms:
        try:
            return _load(s, o)
        except ValueError:
            pass
    for s in tuples:
        try:
            return _load(s, o)
        except ValueError:
            pass
    for s in data:
        assert isinstance(o, list)
        if o[s.discriminant_index] == s.discriminant:
            return _load(s, o)

    raise RuntimeError(f"Unable to deserialize given schemas: {schemas}")


def serialize_schema(schema: SType) -> bytes:
    import pickle

    return pickle.dumps(schema)


def deserialize_schema(b: bytes) -> SType:
    import pickle

    return pickle.loads(b)  # type: ignore[no-any-return]
