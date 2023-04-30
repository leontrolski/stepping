from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path
from typing import Any, Generic, TypeVar, get_args, get_origin, get_type_hints

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def call_site_code(called_from: inspect.FrameInfo) -> ast.Module:
    """Return the parsed AST of a particular stack frame."""
    ps = called_from.positions
    assert ps is not None and ps.lineno is not None
    lines = Path(called_from.filename).read_text().splitlines()
    lines = lines[ps.lineno - 1 : ps.end_lineno]
    lines[-1] = lines[-1][: ps.end_col_offset]
    lines[0] = lines[0][ps.col_offset :]
    return ast.parse("\n".join(lines))


def from_ast(scope: dict[type, Any], node: ast.AST) -> type:
    """Find the value of the type from the scope."""
    if isinstance(node, ast.Name):
        return scope[node.id]  # type: ignore
    if isinstance(node, ast.Attribute):
        v = from_ast(scope, node.value)
        k = node.attr
        return getattr(v, k)  # type: ignore
    if isinstance(node, ast.BinOp):
        if isinstance(node.op, ast.BitOr):
            return from_ast(scope, node.left) | from_ast(scope, node.right)  # type: ignore
    if isinstance(node, ast.Subscript):
        t = from_ast(scope, node.value)
        if isinstance(node.slice, ast.Tuple):
            return t[*(from_ast(scope, n) for n in node.slice.elts)]  # type: ignore
        return t[from_ast(scope, node.slice)]  # type: ignore
    raise RuntimeError(f"Unknown node type: {node}")


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


def name_type_map_from_dataclass(t: type) -> dict[str, type]:
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


ERROR_MESSAGE = "Must be called with lambdas, each on one line like: pick_index(Cat, lambda c: c.name)"


def retrieve_fields() -> str | tuple[str, ...]:
    """Turn a lambda call into path(s) to fields.

    lambda: foo                  -> ""
    lambda: foo[0].bar           -> "0.bar"
    lambda: (foo[0].bar, foo[1]) -> ("0.bar", "1")

    """
    called_from = inspect.stack()[2]
    code = call_site_code(called_from)
    try:
        lambda_ = code.body[0].value.args[1]  # type: ignore
        name = ast.unparse(lambda_.args)
    except:
        raise RuntimeError(ERROR_MESSAGE)

    if isinstance(lambda_.body, ast.Tuple):
        args = lambda_.body.elts
        if len(args) == 0:
            raise RuntimeError(ERROR_MESSAGE)
        return tuple(_retrieve_fields_from_part(name, arg) for arg in args)

    return _retrieve_fields_from_part(name, lambda_.body)


def _retrieve_fields_from_part(prefix: str, node: ast.AST) -> str:
    """Turn an AST into a path to a field.

    AST(foo[0].bar) -> "0.bar"

    """
    if not isinstance(node, (ast.Name, ast.Attribute, ast.Subscript)):
        raise RuntimeError(ERROR_MESSAGE)
    part = ast.unparse(node)
    if re.search(r"^ *[\w\.]+", part) is None:
        raise RuntimeError(ERROR_MESSAGE)
    # foo[0].bar -> foo.0.bar
    part = re.sub(r"\[(\d+)\]", r".\1", part)
    return part.strip().replace(prefix + ".", "")
