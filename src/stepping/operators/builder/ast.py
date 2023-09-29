import ast
import inspect
import pathlib
from dataclasses import dataclass
from typing import Any, NewType, Sequence

Code = NewType("Code", str)
TypeCode = NewType("TypeCode", str)
Target = NewType("Target", str)
KwargsCode = dict[str, Code]


class BuilderParseError(RuntimeError):
    ...


@dataclass
class _AST:
    filename: str
    offset: int
    body: list[ast.stmt]


@dataclass
class Assign:
    targets: Sequence[Target]
    f_name: Code
    args: list[Code]
    kwargs: KwargsCode
    transform_name: Code | None = None


@dataclass
class FuncInternal:
    annotations: dict[Target, Code]
    with_assigns: dict[Target, tuple[TypeCode, Code]]
    assigns: list[Assign]
    ret: list[Target]


def func_name(code: Code) -> str:
    return ast.parse(code).body[0].name  # type: ignore


def parse(func: Any) -> _AST:
    filename = inspect.getsourcefile(func)
    assert filename is not None
    _, offset = inspect.getsourcelines(func)
    module_ast = ast.parse(inspect.getsource(func))
    assert isinstance(module_ast, ast.Module)
    assert isinstance(module_ast.body[0], ast.FunctionDef)
    return _AST(filename, offset - 1, module_ast.body[0].body)


def _is_only_dotted_name(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return True
    elif isinstance(node, ast.Attribute):
        return _is_only_dotted_name(node.value)
    return False


def build_internal(func_ast: _AST) -> FuncInternal:
    annotations = dict[Target, Code]()
    with_assigns = dict[Target, tuple[TypeCode, Code]]()
    assigns = list[Assign]()
    ret: list[Target] | None = None

    for stmt in func_ast.body:
        lineno = func_ast.offset + stmt.lineno
        line = pathlib.Path(func_ast.filename).read_text().splitlines()[lineno - 1]
        msg = f" {func_ast.filename}:{lineno}:\n\n    {line}"

        # preliminiary definitions, foo: Bar
        if isinstance(stmt, ast.AnnAssign) and stmt.value is None:
            if not isinstance(stmt.target, ast.Name):
                raise BuilderParseError("Expected single name annotation" + msg)
            target = Target(stmt.target.id)
            if target in annotations:
                raise BuilderParseError("Can only annotate `{target}` once" + msg)
            annotations[target] = Code(ast.unparse(stmt.annotation))

        # foo = f(a, b, c=c)
        elif isinstance(stmt, ast.Assign):
            if not len(stmt.targets) == 1:
                raise BuilderParseError("Can only assign once per statement" + msg)
            target_expr = stmt.targets[0]
            targets = _build_targets(msg, target_expr)
            for target_ in targets:
                if target_ in {t for assign in assigns for t in assign.targets}:
                    raise BuilderParseError("Can only assign to a target once" + msg)

            if not isinstance(stmt.value, ast.Call):
                raise BuilderParseError("Can only assign function calls" + msg)

            assigns.append(_build_assign(msg, stmt.value, targets))

        # return foo
        elif isinstance(stmt, ast.Return):
            if ret is not None:
                raise BuilderParseError("Can only return once" + msg)

            tuple_msg = "Can only return named variable or tuple of variables"
            if isinstance(stmt.value, ast.Tuple):
                ret = []
                for expr in stmt.value.elts:
                    if not isinstance(expr, ast.Name):
                        raise BuilderParseError(tuple_msg + msg)
                    ret.append(Target(expr.id))
            elif isinstance(stmt.value, ast.Name):
                ret = [Target(stmt.value.id)]
            else:
                raise BuilderParseError(tuple_msg + msg)

        # with st.at_compile_time
        elif isinstance(stmt, ast.With):
            with_assigns = _build_with_assigns(msg, stmt)

        # Allow expressions (mostly for docstrings)
        elif isinstance(stmt, ast.Expr):
            pass

        else:
            raise BuilderParseError("Can only assign, return, def" + msg)

    assert ret is not None
    return FuncInternal(annotations, with_assigns, assigns, ret)


def _build_targets(msg: str, target_expr: ast.expr) -> Sequence[Target]:
    if isinstance(target_expr, ast.Name):
        return [Target(target_expr.id)]
    elif isinstance(target_expr, ast.Tuple):
        return [t for elt in target_expr.elts for t in _build_targets(msg, elt)]
    raise BuilderParseError("Can only assign to target of name(s)" + msg)


def build_tuple_of_names(code: Code) -> Code | tuple[Code, ...] | None:
    module = ast.parse(code)
    if not isinstance(module.body[0], ast.Expr):
        return None
    expr = module.body[0].value
    if isinstance(expr, ast.Name):
        return Code(expr.id)
    elif isinstance(expr, ast.Tuple):
        out = tuple[Code, ...]()
        for elt in expr.elts:
            if not isinstance(elt, ast.Name):
                return None
            out += (Code(elt.id),)
        return out
    return None


def _build_assign(msg: str, stmt: ast.Call, targets: Sequence[Target]) -> Assign:
    if isinstance(stmt.func, ast.Subscript):
        transform_name = Code(ast.unparse(stmt.func.value))
        if isinstance(stmt.func.slice, ast.Tuple):
            args = [Code(ast.unparse(arg)) for arg in stmt.func.slice.elts]
        else:
            args = [Code(ast.unparse(stmt.func.slice))]
        if len(stmt.args) != 1:
            raise BuilderParseError("Can only have one lambda arg" + msg)
        lambda_ = stmt.args[0]
        if not isinstance(lambda_, ast.Lambda) or len(lambda_.args.args) != 1:
            raise BuilderParseError(
                "Must have be in the form lambda a: f(a, ...)" + msg
            )
        if not isinstance(lambda_.body, ast.Call):
            raise BuilderParseError(
                "Must have be in the form lambda a: f(a, ...)" + msg
            )
        f_name = Code(ast.unparse(lambda_.body.func))
        assign = Assign(targets, f_name, args, {}, transform_name)
        if len(lambda_.body.args) != len(args):
            raise BuilderParseError(
                "Must match transform[a, b](lambda x, y: ...)" + msg
            )
        for kwarg in lambda_.body.keywords:
            assert isinstance(kwarg.arg, str)
            assign.kwargs[kwarg.arg] = Code(ast.unparse(kwarg.value))
    else:
        f_name = Code(ast.unparse(stmt.func))
        assign = Assign(targets, f_name, [], {})
        for arg in stmt.args:
            if not _is_only_dotted_name(arg):
                raise BuilderParseError("Can only call plain names" + msg)
            assign.args.append(Code(ast.unparse(arg)))
        for kwarg in stmt.keywords:
            assert isinstance(kwarg.arg, str)
            assign.kwargs[kwarg.arg] = Code(ast.unparse(kwarg.value))

    return assign


def _build_with_assigns(
    msg: str, with_: ast.With
) -> dict[Target, tuple[TypeCode, Code]]:
    with_assigns = dict[Target, tuple[TypeCode, Code]]()

    if len(with_.items) != 1:
        raise BuilderParseError("Can only do: with st.at_compile_time" + msg)
    attribute = with_.items[0].context_expr
    if not isinstance(attribute, ast.Attribute):
        raise BuilderParseError("Can only do: with st.at_compile_time" + msg)
    if not attribute.attr == "at_compile_time":
        raise BuilderParseError("Can only do: with st.at_compile_time" + msg)

    for assign in with_.body:
        if not isinstance(assign, ast.AnnAssign):
            raise BuilderParseError(
                "Must provide type with st.compile statements" + msg
            )
        if not isinstance(assign.target, ast.Name):
            raise BuilderParseError("Can only assign to single name" + msg)
        if assign.value is None:
            raise BuilderParseError("Must assign a value" + msg)
        with_assigns[Target(assign.target.id)] = (
            TypeCode(ast.unparse(assign.annotation)),
            Code(ast.unparse(assign.value)),
        )

    return with_assigns


_GLOBAL_KWARG_TYPES = list[type]()


@dataclass
class RewriteCompileTypeof(ast.NodeTransformer):
    type_scope: dict[str, type]

    def visit_Call(self, node: ast.Call) -> ast.AST:
        attribute = node.func
        if not isinstance(attribute, ast.Attribute):
            return ast.Call(
                node.func, [self.visit(n) for n in node.args], node.keywords
            )
        if attribute.attr != "compile_typeof":
            return ast.Call(
                node.func, [self.visit(n) for n in node.args], node.keywords
            )
        assert len(node.args) == 1
        if not isinstance(node.args[0], ast.Name):
            raise BuilderParseError("Can only call st.compile_typeof with name")
        kwarg_name = node.args[0].id
        if kwarg_name not in self.type_scope:
            raise BuilderParseError(f"Couldn't see {kwarg_name} in kwargs")
        _GLOBAL_KWARG_TYPES.append(self.type_scope[kwarg_name])
        return ast.parse(f"_GLOBAL_KWARG_TYPES[{len(_GLOBAL_KWARG_TYPES) - 1}]")


def replace_compile_typeof(type_scope: dict[str, type], code: Code) -> Code:
    # See test_replace_compile_typeof
    node = RewriteCompileTypeof(type_scope).visit(ast.parse(code))
    return Code(ast.unparse(node))
