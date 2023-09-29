from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime
from functools import cache
from types import NoneType, UnionType
from typing import Callable, Union, cast, get_args, get_origin
from uuid import UUID

from stepping import types


@cache  # we do all this as get_args(...), get_origin(...) are quite expensive
def _make_deserialize(
    t: type[types.TSerializable] | UnionType,
) -> Callable[[types.Serialized], types.TSerializable]:
    original_t = t
    origin = get_origin(t)

    if origin is Union or isinstance(t, UnionType):
        inner_types = get_args(t)

        def inner(n: types.Serialized) -> types.TSerializable:
            for inner_type in inner_types:
                try:
                    return deserialize(inner_type, n)
                except Exception:
                    pass
            raise RuntimeError(f"Unable to deserialise value: {n}")

        return inner

    if origin is tuple:
        inner_types = get_args(t)

        def inner(n: types.Serialized) -> types.TSerializable:
            assert isinstance(n, list)
            assert len(inner_types) == len(n)
            return t(deserialize(inner_type, m) for inner_type, m in zip(inner_types, n))  # type: ignore

        return inner

    t = cast(type[types.TSerializable], origin or t)

    if issubclass(t, (datetime, date)):

        def inner(n: types.Serialized) -> types.TSerializable:
            assert isinstance(n, str)
            try:  # fix this bodge?
                return date.fromisoformat(n)  # type: ignore[return-value]
            except ValueError:
                return datetime.fromisoformat(n)  # type: ignore[return-value]

        return inner

    if issubclass(t, UUID):

        def inner(n: types.Serialized) -> types.TSerializable:
            assert isinstance(n, str)
            return UUID(n)  # type: ignore[return-value]

        return inner

    if issubclass(t, (int, float, str, bool, NoneType)):  # or n is None:

        def inner(n: types.Serialized) -> types.TSerializable:
            assert isinstance(n, t)
            return n  # type: ignore[return-value]

        return inner

    if issubclass(t, types.SerializableObject):
        return t.make_deserialize.__func__(original_t)  # type: ignore

    raise RuntimeError(f"Unknown type: {t}")


def deserialize(
    t: type[types.TSerializable] | UnionType, n: types.Serialized
) -> types.TSerializable:
    return _make_deserialize(t)(n)  # type: ignore


def serialize(n: types.Serializable) -> types.Serialized:
    if isinstance(n, (int, float, str, bool)) or n is None:
        return n
    if isinstance(n, date):
        return n.isoformat()
    if isinstance(n, datetime):
        return n.isoformat()
    if isinstance(n, UUID):
        return str(n)
    if isinstance(n, (tuple, list)):
        return [serialize(m) for m in n]
    # if isinstance(n, types.SerializableObject):
    if hasattr(n, "serialize"):
        return n.serialize()
    raise RuntimeError(f"Value of unknown type: {n}")


def _hash(d: types.SerializableObject) -> str:
    json_str = json.dumps(d.serialize(), separators=(",", ":"), sort_keys=True)
    md5 = hashlib.md5()
    md5.update(json_str.encode())
    return str(uuid.UUID(md5.hexdigest()))


def make_identity(n: types.Serializable | tuple[types.Serializable, ...]) -> str:
    if isinstance(n, (int, float, str, bool, UUID)) or n is None:
        return str(n)
    if isinstance(n, date):
        return n.isoformat()
    if isinstance(n, datetime):
        return n.isoformat()
    if isinstance(n, (tuple, list)):
        return ",".join(make_identity(m) for m in n)
    # if isinstance(n, types.SerializableObject):
    if hasattr(n, "serialize"):
        return _hash(n)
    raise RuntimeError(f"Value of unknown type: {n}")
