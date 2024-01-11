from __future__ import annotations

import collections.abc
import inspect
from collections import defaultdict
from types import UnionType
from typing import Any, Callable, Iterator, TypeVar, Union, get_origin, get_type_hints

from stepping.types import Index, Signature

TraversePath = tuple[int, ...]


def _get_origin(t: type) -> type:
    if get_origin(t) is not None:
        t = get_origin(t)  # type: ignore
    if t is collections.abc.Callable:  # type: ignore
        t = Callable  # type: ignore
    return t


def _args(t: type) -> tuple[type, ...]:
    return getattr(t, "__args__", ()) + getattr(t, "__metadata__", ())


def _traverse_type(
    t: type, path: TraversePath = ()
) -> Iterator[tuple[TraversePath, type]]:
    yield path, t
    for i, arg in enumerate(_args(t)):
        yield from _traverse_type(arg, path + (i,))


def _get_type(t: type, path: TraversePath) -> type:
    if not path:
        return t
    i, *rest = path
    t_new = _args(t)[i]
    return _get_type(t_new, tuple(rest))


def _set_type(
    t: type, new_path: TraversePath, new_type: type, path: TraversePath = ()
) -> type:
    if not new_path:
        return new_type
    if not hasattr(t, "__args__"):
        return t

    u = _get_origin(t)
    args = [
        new_type
        if (path + (i,)) == new_path
        else _set_type(arg, new_path, new_type, path + (i,))
        for i, arg in enumerate(_args(t))
    ]
    if u is Callable:  # type: ignore[comparison-overlap]
        *head, tail = args
        return u[head, tail]  # type: ignore
    if u is UnionType:
        u = Union  # type: ignore
    return u[*args]  # type: ignore


def has_any_type_vars(signature: Signature) -> bool:
    for t in (dict(signature.args) | signature.kwargs).values():
        for _, inner_t in _traverse_type(signature.ret):
            if type(inner_t) is TypeVar:  # type: ignore[comparison-overlap]
                return True
    return False


def _is_concrete_type(t: type) -> bool:
    for _, u in _traverse_type(t):
        if get_origin(u) is Union:
            if any(not _is_concrete_type(v) for v in _args(u)):
                return False
        elif type(u) is TypeVar:  # type: ignore[comparison-overlap]
            return False
    return True


def _resolve_type(
    type_scope: dict[str, type], signature: Signature, typevar: type
) -> type:
    original_type_scope = dict(signature.args) | signature.kwargs

    new_types = set[type]()
    for name, original_composite in original_type_scope.items():
        for original_path, original_t in _traverse_type(original_composite):
            if original_t is not typevar:
                continue
            arg_t = type_scope[name]
            new_type = _get_type(arg_t, original_path)
            if _is_concrete_type(new_type):
                new_types.add(new_type)
                # return new_type

    if len(new_types) == 1:
        return new_types.pop()
    if len(new_types) == 2:
        return _reduce_union(Union[*new_types])  # type: ignore
    raise RuntimeError(f"Couldn't resolve type: {typevar}")


def _reduce_union(t: type) -> type:
    """Reduces Unions, assumes covariant.

    Given:

        Union[
            ZSet[tuple[Class, tuple[str, ZSetPython[Class]]]],
            ZSet[tuple[Class, Empty]],
        ]

    Return:

        ZSet[tuple[Class, tuple[str, ZSetPython[Class]] | Empty]]

    """
    if _get_origin(t) is not Union:  # type: ignore[comparison-overlap]
        return t
    a, b = _args(t)
    if a == b:
        return a
    origin_a = _get_origin(a)
    origin_b = _get_origin(b)
    args_a = _args(a)
    args_b = _args(b)
    if origin_a != origin_b or len(args_a) != len(args_b):
        return Union[a, b]  # type: ignore[return-value]
    args = [_reduce_union(Union[x, y]) for x, y in zip(args_a, args_b)]  # type: ignore[arg-type]
    return origin_a[*args]  # type: ignore


def resolve_type(
    type_scope: dict[str, type], signature: Signature, type_: type
) -> type:
    to_resolve: dict[type, set[TraversePath]] = defaultdict(set)
    for path, t in _traverse_type(type_):
        if type(t) is TypeVar:  # type: ignore[comparison-overlap]
            to_resolve[t].add(path)

    for typevar, typevar_paths in to_resolve.items():
        new_type = _resolve_type(type_scope, signature, typevar)
        for typevar_path in typevar_paths:
            type_ = _set_type(type_, typevar_path, new_type)

    return type_


def get_signature(func: Callable[..., Any]) -> Signature:
    if func in {int, float, str, list, tuple}:
        return Signature([], {}, func)  # type: ignore[arg-type]

    msg = f"In function: {func.__module__}.{func.__name__}\n"
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    if not type_hints or type_hints.get("return") is None:
        raise TypeError(msg + "Make sure function has type annotations")
    ret = type_hints["return"]
    if ret is None:
        raise TypeError(msg + f"Function return type is None")
    signature = Signature([], {}, ret)
    for parameter in sig.parameters.values():
        type_hint = type_hints[parameter.name]
        if parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD:
            signature.args.append((parameter.name, type_hint))
        elif parameter.kind is inspect.Parameter.KEYWORD_ONLY:
            signature.kwargs[parameter.name] = type_hint
        else:
            raise TypeError(msg + "Function must be of form f(a, b, *, c, d)")
    return signature


def value_to_type(value: Any) -> type:
    if isinstance(value, tuple):
        inner_types = [value_to_type(n) for n in value]
        return tuple[*inner_types]  # type: ignore
    if isinstance(value, Index):
        return Index[value.t, value.k]
    if not callable(value):
        return type(value)

    signature = get_signature(value)
    if signature.kwargs:
        raise TypeError(f"Cannot convert {value} to Callable as it has kwargs")
    return Callable[[t for _, t in signature.args], signature.ret]  # type: ignore
