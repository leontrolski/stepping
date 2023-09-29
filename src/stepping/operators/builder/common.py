import importlib
import inspect
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cache
from types import ModuleType
from typing import Any, Callable, Iterator, NewType, TypeVar, get_origin

from stepping import types
from stepping.graph import OperatorKind
from stepping.operators.builder import ast, traverse

Identifier = NewType("Identifier", str)  # function name namespaced by module
TargetPort = tuple[ast.Target, int]
TCallable = TypeVar("TCallable", bound=Callable[..., Any])
VCallable = TypeVar("VCallable", bound=Callable[..., Any])


class BuilderError(RuntimeError):
    ...


@dataclass
class FuncGraph:
    target_identifier_map: dict[ast.Target, Identifier]
    target_kwargs_code_map: dict[ast.Target, ast.KwargsCode]
    input: dict[int, list[TargetPort]]  # in -> a
    internal: set[tuple[ast.Target, TargetPort]]  # a -> b
    output: list[ast.Target]


@cache
def get_func(identifier: Identifier) -> Callable[..., Any]:
    *module_split, func = identifier.split(".")
    module = importlib.import_module(".".join(module_split))
    return getattr(module, func)  # type: ignore[no-any-return]


@cache
def get_identifier(func: Callable[..., Any]) -> Identifier:
    return Identifier(f"{func.__module__}.{func.__name__}")


@cache
def get_module(func: Callable[..., Any]) -> ModuleType:
    module = inspect.getmodule(func)
    assert module is not None
    return module


@cache
def get_operator_kind(func: Callable[..., Any]) -> OperatorKind | None:
    return getattr(func, "operator_kind", None)


def evaluate(
    func: Callable[..., Any],
    scope: dict[str, Any],
    code: ast.Code | ast.TypeCode,
) -> Any:
    total_scope = dict[str, Any]()
    total_scope |= get_module(func).__dict__
    total_scope |= scope
    total_scope["_GLOBAL_KWARG_TYPES"] = ast._GLOBAL_KWARG_TYPES
    out = eval(code, total_scope)
    return out


@cache
def build_graph(func: Callable[..., Any]) -> FuncGraph:
    msg = f"In function: {get_identifier(func)}\n"
    func_internal = ast.build_internal(ast.parse(func))

    target_identifier_map = dict[ast.Target, Identifier]()
    for assign in func_internal.assigns:
        assign_func = evaluate(func, {}, assign.f_name)
        assign_identifier = get_identifier(assign_func)
        for from_target in assign.targets:
            target_identifier_map[from_target] = assign_identifier

    target_kwargs_code_map = dict[ast.Target, ast.KwargsCode]()
    internal = set[tuple[ast.Target, TargetPort]]()
    input: dict[int, list[TargetPort]] = defaultdict(list)
    arg_i_map = {
        name: i for i, [name, _] in enumerate(traverse.get_signature(func).args)
    }
    for assign in func_internal.assigns:
        for i, arg in enumerate(assign.args):
            for to_target in assign.targets:
                port = to_target, i
                if arg in arg_i_map:
                    input_i = arg_i_map[arg]
                    input[input_i].append(port)
                else:
                    from_target = ast.Target(arg)
                    if from_target not in target_identifier_map:
                        raise BuilderError(msg + f"{from_target} not a valid target")
                    internal.add((from_target, port))

        for to_target in assign.targets:
            target_kwargs_code_map[to_target] = assign.kwargs

    for from_target in func_internal.ret:
        if from_target not in target_identifier_map:
            raise BuilderError(msg + f"{from_target} not a valid target")

    return FuncGraph(
        target_identifier_map=target_identifier_map,
        target_kwargs_code_map=target_kwargs_code_map,
        input=input,
        internal=internal,
        output=func_internal.ret,
    )


def build_target_signatures(
    parent_func: Callable[..., Any],
    type_scope_global: dict[str, type],
    assign: ast.Assign,
) -> Iterator[tuple[ast.Target, types.Signature]]:
    """Given an ast.Assign, yield each target and the function signature."""
    type_scope = {**type_scope_global}
    func = evaluate(parent_func, {}, assign.f_name)
    signature = traverse.get_signature(func)

    zipped = zip(signature.args, assign.args)
    for name, code in [(name, code) for [name, _], code in zipped]:
        type_scope[name] = type_scope_global[code]

    for name, code in assign.kwargs.items():
        codes = ast.build_tuple_of_names(code)
        if isinstance(codes, str) and code in type_scope:
            type_scope[name] = type_scope[code]
        elif isinstance(codes, tuple) and all(c in type_scope for c in codes):
            type_scope[name] = tuple[*(type_scope[c] for c in codes)]  # type: ignore[misc]
        else:
            value = evaluate(parent_func, {}, code)
            type_scope[name] = traverse.value_to_type(value)

    arg_types = [(name, type_scope[name]) for name, _ in signature.args]
    kwarg_types = {name: type_scope[name] for name in signature.kwargs}

    # potentially the assign function is a transformer that lifts the graph in some way
    transformer = None
    if assign.transform_name is not None:
        transformer_builder = evaluate(parent_func, {}, assign.transform_name)
        assert isinstance(transformer_builder, types.TransformerBuilder)
        transformer = transformer_builder.from_arg_types(arg_types)
        signature.ret = transformer.lift(signature.ret)

    ret = traverse.resolve_type(type_scope, signature, signature.ret)

    for i, target in enumerate(assign.targets):
        this_ret = ret
        if len(assign.targets) > 1:
            assert get_origin(ret) is tuple
            this_ret = ret.__args__[i]  # type: ignore[attr-defined]
        signature_resolved = types.Signature(
            args=arg_types,
            kwargs=kwarg_types,
            ret=this_ret,
            transformer=transformer,
        )
        yield target, signature_resolved
