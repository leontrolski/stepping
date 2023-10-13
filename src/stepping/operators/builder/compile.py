from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, Callable, Iterator

from stepping.graph import (
    Graph,
    OperatorKind,
    Path,
    Port,
    Vertex,
    VertexBinary,
    VertexUnary,
    VertexUnaryDelay,
)
from stepping.operators.builder import ast, common, traverse
from stepping.types import Index, Signature, T


@contextmanager
def _at_compile_time() -> Iterator[None]:
    """Execute this code at compile time."""
    yield None


at_compile_time = _at_compile_time()


def compile_typeof(t: T) -> type[T]:
    """Get the type of a variable at compile time."""
    raise NotImplementedError("Should be replaced at compile time")


def vertex(
    operator_kind: OperatorKind,
) -> Callable[[common.TCallable], common.TCallable]:
    """Decorator to set `.operator_kind` on a function."""

    def inner(f: common.TCallable) -> common.TCallable:
        f.operator_kind = operator_kind  # type: ignore[attr-defined]
        return f

    return inner


def _build_signatures(
    func: Callable[..., Any],
    type_scope: dict[str, type],
) -> dict[ast.Target, Signature]:
    internal = ast.build_internal(ast.parse(func))
    type_scope_global = {**type_scope}

    target_signature_map = dict[ast.Target, Signature]()
    for assign in internal.assigns * 2:  # `* 2` handles recursive definitions
        for target, assign_signature in common.build_target_signatures(
            func, type_scope_global, assign
        ):
            type_scope_global[target] = assign_signature.ret
            target_signature_map[target] = assign_signature

    return target_signature_map


def _make_scopes(
    func: Callable[..., Any],
    signature: Signature,
    kwargs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, type]]:
    """Build a scope and type_scope from the kwargs, annotations and defs."""
    scope, type_scope = {**kwargs}, dict(signature.args) | dict(signature.kwargs)
    internal = ast.build_internal(ast.parse(func))

    for target, code in internal.annotations.items():
        type_scope[target] = common.evaluate(func, {}, code)

    # Immediately evaluate `with st.at_compile_time:` block
    for target, [type_code, value_code] in internal.with_assigns.items():
        value_code = ast.replace_compile_typeof(type_scope, value_code)
        scope[target] = common.evaluate(func, scope, value_code)
        type_ = common.evaluate(func, scope, type_code)
        original_signature = traverse.get_signature(func)
        type_scope[target] = traverse.resolve_type(
            type_scope, original_signature, type_
        )

    return scope, type_scope


@dataclass
class _Partialled:
    func: Callable[..., Any]
    kwargs: dict[str, Any]

    def __call__(self, *args: Any) -> Any:
        return self.func(*args, **self.kwargs)


def _make_vertex(
    func: Callable[..., Any],
    kwargs: dict[str, Any],
    signature: Signature,
    path: Path,
) -> Vertex:
    operator_kind = common.get_operator_kind(func)
    assert operator_kind is not None
    f: Callable[..., Any] = func
    if kwargs:
        f = _Partialled(func, kwargs)
    v = signature.ret

    if len(signature.args) == 1:
        ([_, t],) = signature.args
        if operator_kind is OperatorKind.delay:
            # if there's an index kwarg, add it to the vertex
            indexes: tuple[Index[Any, Any], ...] | None = kwargs.get("indexes")
            return VertexUnaryDelay(t, v, operator_kind, path, f, indexes=indexes or ())

        return VertexUnary(t, v, operator_kind, path, f)
    else:
        [_, t], [_, u] = signature.args
        return VertexBinary(t, u, v, operator_kind, path, f)


def _compile_target(
    func: Callable[..., Any],
    signature: Signature,
    kwargs: dict[str, Any],
    path: Path,
) -> Graph[Any, Any]:
    transformer = signature.transformer
    if transformer is None:
        return compile_generic(func, kwargs, signature, path)
    else:
        args = [(name, transformer.unlift(arg)) for name, arg in signature.args]
        ret = transformer.unlift(signature.ret)
        signature = replace(signature, args=args, ret=ret)
        target_graph = compile_generic(func, kwargs, signature, path)
        return transformer.transform(target_graph)


def _identity_inputs(
    func_graph: common.FuncGraph,
    target_graphs: dict[ast.Target, Graph[Any, Any]],
    path: Path,
) -> list[Vertex]:
    # Check we have keys 0..n_inputs
    assert sorted(func_graph.input.keys()) == list(range(len(func_graph.input)))

    # Determine the types, checking they are all the same per `i`
    input_t_map = dict[int, type]()
    for i, target_ports in func_graph.input.items():
        for target, target_i in target_ports:
            p, vertex_i = target_graphs[target].input[target_i]
            vertex = target_graphs[target].vertices[p]
            t: type = vertex.t
            if vertex_i == 1:
                assert isinstance(vertex, VertexBinary)
                t = vertex.u
            if i in input_t_map:
                assert input_t_map[i] == t
            input_t_map[i] = t

    return [
        VertexUnary(t, t, OperatorKind.identity, path / f"input_{i}", lambda a: a)
        for i, t in sorted(input_t_map.items())
    ]


def compile_generic(
    func: Callable[..., Any],
    kwargs: dict[str, Any],
    signature: Signature,
    path: Path,
) -> Graph[Any, Any]:
    """Compile a query function to a Graph."""
    operator_kind = common.get_operator_kind(func)
    if operator_kind is not None:
        return _make_vertex(func, kwargs, signature, path).as_graph

    path /= common.get_identifier(func)
    scope, type_scope = _make_scopes(func, signature, kwargs)
    signature_map = _build_signatures(func, type_scope)
    func_graph = common.build_graph(func)

    target_graphs = {
        target: _compile_target(
            func=common.get_func(target_identifier),
            signature=signature_map[target],
            kwargs={
                name: common.evaluate(func, scope, code)
                for name, code in func_graph.target_kwargs_code_map[target].items()
            },
            path=path / target,
        )
        for target, target_identifier in func_graph.target_identifier_map.items()
    }
    # We add an identity to allow re-use of input arguments
    input_identities = _identity_inputs(func_graph, target_graphs, path)

    input: list[Port] = [(vertex.path, 0) for vertex in input_identities]
    vertices: list[Vertex] = input_identities
    internal = set[tuple[Path, Port]]()
    output = list[Path]()
    run_no_output = list[Path]()

    for i, target_ports in func_graph.input.items():
        for target, target_i in target_ports:
            internal.add(
                (input_identities[i].path, target_graphs[target].input[target_i])
            )
    for target in func_graph.output:
        for p in target_graphs[target].output:
            output.append(p)
    for from_target, [to_target, i] in func_graph.internal:
        for from_vertex in target_graphs[from_target].output:
            internal.add((from_vertex, target_graphs[to_target].input[i]))
        for p in target_graphs[to_target].run_no_output:
            run_no_output.append(p)
    for target_graph in target_graphs.values():
        for vertex in target_graph.vertices.values():
            vertices.append(vertex)
        for connection in target_graph.internal:
            internal.add(connection)

    return Graph(
        vertices={v.path: v for v in vertices},
        input=input,
        internal=internal,
        output=output,
        run_no_output=run_no_output,
    )
