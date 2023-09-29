from typing import Callable, Union

import stepping as st
from stepping import zset
from stepping.graph import OperatorKind, Path, VertexUnary
from stepping.operators import builder, transform
from stepping.operators.builder import ast, common, traverse
from stepping.operators.builder.compile import _build_signatures
from stepping.types import (
    Empty,
    Grouped,
    Index,
    K,
    Pair,
    Signature,
    T,
    TAddable,
    V,
    ZSet,
    get_annotation_zset,
)
from stepping.zset import functions
from stepping.zset.python import ZSetPython


@builder.vertex(OperatorKind.add)
def add(a: TAddable, b: TAddable) -> TAddable:
    return a + b


@builder.vertex(OperatorKind.identity)
def identity(a: T) -> T:
    return a


@builder.vertex(OperatorKind.delay)
def delay(a: T) -> T:
    raise RuntimeError("This should never be evaluated in a normal way")


@builder.vertex(OperatorKind.first_n)
def first_n(a: ZSet[T], *, index: Index[T, K], n: int) -> ZSet[T]:
    return functions.first_n(a, index, n)


@builder.vertex(OperatorKind.map)
def map(a: ZSet[T], *, f: Callable[[T], V]) -> ZSet[V]:
    return functions.map(a, f)


def add3(a: TAddable, b: TAddable, c: TAddable) -> TAddable:
    added_two = add(a, b)
    added_three = add(added_two, c)
    return added_three


def first_10(a: ZSet[T], *, index: Index[T, K]) -> ZSet[T]:
    picked = first_n(a, index=index, n=10)
    return picked


def map_unbound(a: ZSet[T], *, f: Callable[[T], V]) -> ZSet[V]:
    unbound = map(a, f=f)
    identityed = identity(unbound)
    return identityed


def _times_pi(x: int) -> float:
    return x * 3.14


def map_bound(a: ZSet[int]) -> ZSet[float]:
    bound = map_unbound(a, f=_times_pi)
    return bound


def tee(a: T) -> tuple[T, T]:
    identity1 = identity(a)
    return identity1, identity1


def double(a: int) -> int:
    b = identity(a)
    x, y = tee(b)
    z = add(x, y)
    return z


def integrate(a: TAddable) -> TAddable:
    """Definition 3.27"""
    delayed: TAddable

    i = identity(a)
    added = add(delayed, i)
    delayed = delay(added)
    return added  # delayed if output_delay else added


@builder.vertex(OperatorKind.delay)
def delay_indexed(a: ZSet[T], *, index: Index[T, K]) -> ZSet[T]:
    raise RuntimeError("This should never be evaluated in a normal way")


def use_delay_indexed(
    a: ZSet[tuple[int, float]], *, index: Index[tuple[int, float], float]
) -> ZSet[tuple[int, float]]:
    foo = delay_indexed(a, index=index)
    return foo


def _upper(s: str) -> str:
    return s.upper()


def map_over_group(grouped: Grouped[ZSet[str], str]) -> Grouped[ZSet[str], str]:
    mapped = transform.per_group[grouped](lambda g: map(g, f=_upper))
    return mapped


def _str_y(a: "TAddable") -> TAddable:
    return a


def use_delay_indexed_pair(a: ZSet[Pair[T, K]]) -> ZSet[Pair[T, K]]:
    with st.at_compile_time:
        index: Index[Pair[T, K], K] = st.pick_index(
            get_annotation_zset(st.compile_typeof(a)),
            lambda p: p.right,
        )

    integrated = delay_indexed(a, index=index)
    return integrated


def test_get_signature() -> None:
    actual = traverse.get_signature(first_n)
    expected = Signature(
        args=[("a", ZSet[T])],  # type:ignore
        kwargs={"index": Index[T, K], "n": int},  # type:ignore
        ret=ZSet[T],  # type:ignore
    )
    assert actual == expected

    actual = traverse.get_signature(add)
    expected = Signature(
        args=[("a", TAddable), ("b", TAddable)],  # type:ignore
        kwargs={},
        ret=TAddable,  # type:ignore
    )
    assert actual == expected

    actual = traverse.get_signature(_str_y)
    expected = Signature(
        args=[("a", TAddable)],  # type:ignore
        kwargs={},
        ret=TAddable,  # type:ignore
    )
    assert actual == expected


def test_internal() -> None:
    actual = ast.build_internal(ast.parse(add3))
    assert actual == ast.FuncInternal(
        annotations={},
        with_assigns={},
        assigns=[
            ast.Assign(
                targets=[ast.Target("added_two")],
                f_name=ast.Code("add"),
                args=[ast.Code("a"), ast.Code("b")],
                kwargs={},
            ),
            ast.Assign(
                targets=[ast.Target("added_three")],
                f_name=ast.Code("add"),
                args=[ast.Code("added_two"), ast.Code("c")],
                kwargs={},
            ),
        ],
        ret=[ast.Target("added_three")],
    )

    actual = ast.build_internal(ast.parse(first_10))
    assert actual == ast.FuncInternal(
        annotations={},
        with_assigns={},
        assigns=[
            ast.Assign(
                targets=[ast.Target("picked")],
                f_name=ast.Code("first_n"),
                args=[ast.Code("a")],
                kwargs={"index": ast.Code("index"), "n": ast.Code("10")},
            ),
        ],
        ret=[ast.Target("picked")],
    )

    actual = ast.build_internal(ast.parse(map_over_group))
    assert actual == ast.FuncInternal(
        annotations={},
        with_assigns={},
        assigns=[
            ast.Assign(
                targets=[ast.Target("mapped")],
                f_name=ast.Code("map"),
                args=[ast.Code("grouped")],
                kwargs={"f": ast.Code("_upper")},
                transform_name=ast.Code("transform.per_group"),
            )
        ],
        ret=[ast.Target("mapped")],
    )

    actual = ast.build_internal(ast.parse(use_delay_indexed_pair))
    assert actual == ast.FuncInternal(
        annotations={},
        with_assigns={
            ast.Target("index"): (
                ast.TypeCode("Index[Pair[T, K], K]"),
                ast.Code(
                    "st.pick_index("
                    "get_annotation_zset(st.compile_typeof(a)), "
                    "lambda p: p.right)"
                ),
            ),
        },
        assigns=[
            ast.Assign(
                targets=[ast.Target("integrated")],
                f_name=ast.Code("delay_indexed"),
                args=[ast.Code("a")],
                kwargs={"index": ast.Code("index")},
                transform_name=None,
            )
        ],
        ret=[ast.Target("integrated")],
    )


class Class:
    ...


def test_reduce_union() -> None:
    t = Union[
        ZSet[Pair[Class, Pair[str, ZSetPython[Class]]]],
        ZSet[Pair[Class, Empty]],
    ]
    actual = traverse._reduce_union(t)  # type: ignore[arg-type]
    expected = ZSet[Pair[Class, Pair[str, ZSetPython[Class]] | Empty]]
    assert actual == expected


def test_resolve_type_outer() -> None:
    signature = Signature(args=[("a", T), ("b", T)], kwargs={}, ret=T)  # type: ignore
    type_scope = {
        "a": ZSet[Pair[Class, Pair[str, ZSetPython[Class]]]],
        "b": ZSet[Pair[Class, Empty]],
    }
    actual = traverse.resolve_type(type_scope, signature, signature.ret)  # type: ignore
    expected = ZSet[Pair[Class, Pair[str, ZSetPython[Class]] | Empty]]
    assert actual == expected


def test_replace_compile_typeof() -> None:
    prev = ast._GLOBAL_KWARG_TYPES
    ast._GLOBAL_KWARG_TYPES = []

    code = ast.Code(
        "st.pick_index("
        "get_annotation_zset(st.compile_typeof(a)), "
        "lambda p: p.right)"
    )
    actual = ast.replace_compile_typeof({"a": ZSet[Pair[str, int]]}, code)
    ast._GLOBAL_KWARG_TYPES = prev

    expected = ast.Code(
        "st.pick_index("
        "get_annotation_zset(\n_GLOBAL_KWARG_TYPES[0]), "
        "lambda p: p.right)"
    )
    assert actual == expected


def test_build_graph() -> None:
    actual = common.build_graph(add3)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("added_three"): common.Identifier("tests.test_builder.add"),
            ast.Target("added_two"): common.Identifier("tests.test_builder.add"),
        },
        target_kwargs_code_map={
            ast.Target("added_three"): {},
            ast.Target("added_two"): {},
        },
        input={
            0: [(ast.Target("added_two"), 0)],
            1: [(ast.Target("added_two"), 1)],
            2: [(ast.Target("added_three"), 1)],
        },
        internal={
            (ast.Target("added_two"), (ast.Target("added_three"), 0)),
        },
        output=[ast.Target("added_three")],
    )
    assert actual == expected

    actual = common.build_graph(first_10)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("picked"): common.Identifier("tests.test_builder.first_n")
        },
        target_kwargs_code_map={
            ast.Target("picked"): {
                "index": ast.Code("index"),
                "n": ast.Code("10"),
            }
        },
        input={0: [(ast.Target("picked"), 0)]},
        internal=set(),
        output=[ast.Target("picked")],
    )
    assert actual == expected

    actual = common.build_graph(tee)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("identity1"): common.Identifier("tests.test_builder.identity"),
        },
        target_kwargs_code_map={
            ast.Target("identity1"): {},
        },
        input={0: [(ast.Target("identity1"), 0)]},
        internal=set(),
        output=[ast.Target("identity1"), ast.Target("identity1")],
    )
    assert actual == expected

    actual = common.build_graph(double)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("b"): common.Identifier("tests.test_builder.identity"),
            ast.Target("x"): common.Identifier("tests.test_builder.tee"),
            ast.Target("y"): common.Identifier("tests.test_builder.tee"),
            ast.Target("z"): common.Identifier("tests.test_builder.add"),
        },
        target_kwargs_code_map={
            ast.Target("b"): {},
            ast.Target("x"): {},
            ast.Target("y"): {},
            ast.Target("z"): {},
        },
        input={0: [(ast.Target("b"), 0)]},
        internal={
            (ast.Target("b"), (ast.Target("x"), 0)),
            (ast.Target("b"), (ast.Target("y"), 0)),
            (ast.Target("x"), (ast.Target("z"), 0)),
            (ast.Target("y"), (ast.Target("z"), 1)),
        },
        output=[ast.Target("z")],
    )
    assert actual == expected

    actual = common.build_graph(integrate)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("added"): common.Identifier("tests.test_builder.add"),
            ast.Target("delayed"): common.Identifier("tests.test_builder.delay"),
            ast.Target("i"): common.Identifier("tests.test_builder.identity"),
        },
        target_kwargs_code_map={
            ast.Target("added"): {},
            ast.Target("delayed"): {},
            ast.Target("i"): {},
        },
        input={0: [(ast.Target("i"), 0)]},
        internal={
            (ast.Target("i"), (ast.Target("added"), 1)),
            (ast.Target("delayed"), (ast.Target("added"), 0)),
            (ast.Target("added"), (ast.Target("delayed"), 0)),
        },
        output=[ast.Target("added")],
    )
    assert actual == expected

    actual = common.build_graph(map_over_group)
    expected = common.FuncGraph(
        target_identifier_map={
            ast.Target("mapped"): common.Identifier("tests.test_builder.map")
        },
        target_kwargs_code_map={ast.Target("mapped"): {"f": ast.Code("_upper")}},
        input={0: [(ast.Target("mapped"), 0)]},
        internal=set(),
        output=[ast.Target("mapped")],
    )
    assert actual == expected


def test_traverse_type() -> None:
    actual = list(traverse._traverse_type(Callable[[int], float]))  # type: ignore
    expected = [
        ((), Callable[[int], float]),
        ((0,), int),
        ((1,), float),
    ]
    assert actual == expected

    actual = list(traverse._traverse_type(ZSet[V]))  # type: ignore
    expected = [
        ((), ZSet[V]),  # type: ignore
        ((0,), V),
    ]
    assert actual == expected

    assert (
        traverse._get_type(Callable[[int], float], (0,))  # type:ignore
        == int
    )
    assert (
        traverse._get_type(Callable[[int], float], ())  # type:ignore
        == Callable[[int], float]
    )

    actual = list(traverse._traverse_type(ZSet[Pair[T, V]]))  # type: ignore
    expected = [
        ((), ZSet[Pair[T, V]]),  # type: ignore
        ((0,), Pair[T, V]),  # type: ignore
        ((0, 0), T),
        ((0, 1), V),
    ]
    assert actual == expected


def test_set_type() -> None:
    actual = traverse._set_type(Callable[[T], float], (0,), int)  # type: ignore
    assert actual == Callable[[int], float]

    actual = traverse._set_type(Callable[[int], V], (1,), float)  # type: ignore
    assert actual == Callable[[int], float]

    original = ZSet[Pair[T, V]]
    actual = traverse._set_type(original, (0, 1), float)
    assert actual == ZSet[Pair[T, float]]  # type: ignore
    assert original == ZSet[Pair[T, V]]  # type: ignore

    original = ZSet[V]  # type: ignore
    actual = traverse._set_type(original, (0,), str)
    assert actual == ZSet[str]
    assert original.__args__ == (V,)  # type: ignore


def test_resolve_ret_type() -> None:
    actual = traverse.resolve_type(
        {"a": ZSet[int], "f": Callable[[int], float]},  # type:ignore
        Signature(
            args=[("a", ZSet[T])],  # type:ignore
            kwargs={"f": Callable[[T], V]},  # type:ignore
            ret=ZSet[V],  # type:ignore
        ),
        ZSet[V],  # type:ignore
    )
    assert actual == ZSet[float]

    actual = traverse.resolve_type(
        {"a": ZSet[int]},
        Signature(
            args=[("a", T)],  # type:ignore
            kwargs={},
            ret=T,  # type:ignore
        ),
        T,  # type:ignore
    )
    assert actual == ZSet[int]


def test_bind_types() -> None:
    map_bound_graph = _build_signatures(map_bound, {"a": ZSet[int]})
    expected = {
        ast.Target("bound"): Signature(
            args=[("a", ZSet[int])],
            kwargs={"f": Callable[[int], float]},  # type: ignore
            ret=ZSet[float],
        )
    }
    assert map_bound_graph == expected

    map_bound_graph = _build_signatures(double, {"a": int})
    expected = {
        ast.Target("b"): Signature(args=[("a", int)], kwargs={}, ret=int),
        ast.Target("x"): Signature(args=[("a", int)], kwargs={}, ret=int),
        ast.Target("y"): Signature(args=[("a", int)], kwargs={}, ret=int),
        ast.Target("z"): Signature(args=[("a", int), ("b", int)], kwargs={}, ret=int),
    }
    assert map_bound_graph == expected

    map_bound_graph = _build_signatures(
        first_10, {"a": ZSet[str], "index": Index[str, float]}
    )
    expected = {
        ast.Target("picked"): Signature(
            args=[("a", ZSet[str])],
            kwargs={"index": Index[str, float], "n": int},
            ret=ZSet[str],
        )
    }
    assert map_bound_graph == expected

    map_bound_graph = _build_signatures(
        first_10, {"a": ZSet[int], "index": Index[int, tuple[float, str]]}
    )
    expected = {
        ast.Target("picked"): Signature(
            args=[("a", ZSet[int])],
            kwargs={
                "index": Index[int, tuple[float, str]],
                "n": int,
            },
            ret=ZSet[int],
        )
    }
    assert map_bound_graph == expected

    map_bound_graph = _build_signatures(integrate, {"a": int, "delayed": int})
    expected = {
        ast.Target("added"): Signature(
            args=[("a", int), ("b", int)], kwargs={}, ret=int
        ),
        ast.Target("delayed"): Signature(args=[("a", int)], kwargs={}, ret=int),
        ast.Target("i"): Signature(args=[("a", int)], kwargs={}, ret=int),
    }
    assert map_bound_graph == expected

    map_bound_graph = _build_signatures(
        integrate, {"a": ZSet[int], "delayed": ZSet[int]}
    )
    expected = {
        ast.Target("added"): Signature(
            args=[("a", ZSet[int]), ("b", ZSet[int])], kwargs={}, ret=ZSet[int]
        ),
        ast.Target("delayed"): Signature(
            args=[("a", ZSet[int])], kwargs={}, ret=ZSet[int]
        ),
        ast.Target("i"): Signature(args=[("a", ZSet[int])], kwargs={}, ret=ZSet[int]),
    }
    assert map_bound_graph == expected

    map_over_group_graph = _build_signatures(
        map_over_group, {"grouped": Grouped[ZSet[str], str]}
    )
    expected = {
        ast.Target("mapped"): Signature(
            args=[("a", Grouped[ZSet[str], str])],
            kwargs={"f": Callable[[str], str]},  # type: ignore
            ret=Grouped[ZSet[str], str],
            transformer=transform.GroupTransformer(str),
        )
    }
    assert map_over_group_graph == expected


def test_compile_vertex() -> None:
    signature = Signature(
        args=[("a", int)],
        kwargs={},
        ret=int,
    )
    actual = builder.compile_generic(identity, {}, signature, Path())

    expected = VertexUnary(
        t=int,
        v=int,
        operator_kind=OperatorKind.identity,
        path=Path(),
        f=identity,  # function
    ).as_graph
    assert actual == expected


def test_compile_double() -> None:
    actual = builder.compile_generic(double, {}, traverse.get_signature(double), Path())

    signature = Signature(
        args=[("a", ZSet[Pair[str, int]])],
        kwargs={},
        ret=ZSet[Pair[str, int]],
    )
    actual = builder.compile_generic(use_delay_indexed_pair, {}, signature, Path())
    assert actual.output[0].v == ZSet[Pair[str, int]]
