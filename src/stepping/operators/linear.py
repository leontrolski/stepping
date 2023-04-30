from typing import Annotated, Callable

from stepping import zset
from stepping.graph import (
    A1,
    A2,
    A3,
    Graph,
    VertexBinary,
    VertexKind,
    VertexUnary,
    get_single_vertex,
    stack,
)
from stepping.types import Index, Indexable, K, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import T, TAddable, TAddAndNegable, TNegable, U, V, ZSet
from stepping.zset.python import ZSetPython

# Named rather than lambdas to aid debugging


def _identity(n: T) -> T:
    return n


def _identity_print(n: T) -> T:
    print(n)
    return n


def _neg(a: TNegable) -> TNegable:
    return -a


def _add(a: TAddable, b: TAddable) -> TAddable:
    return a + b


def _z_make_set(n: T) -> ZSet[T]:
    return ZSetPython({n: 1})


def _z_haitch(a: ZSet[T], b: ZSet[T]) -> ZSet[T]:
    return zset.haitch(a, b)


# Generic over all T


def identity(
    name: str,
    t: type[T],
) -> Graph[A1[T], A1[T]]:
    name = "identity_" + name
    return VertexUnary(t, t, name, f=_identity, kind=VertexKind.IDENTITY).g


def identity_print(
    name: str,
    t: type[T],
) -> Graph[A1[T], A1[T]]:
    """Useful for debugging."""
    name = "identity_print_" + name
    return VertexUnary(t, t, name, f=_identity_print).g


def delay(
    name: str,
    t: type[T],
) -> Graph[A1[T], A1[T]]:
    name = "delay_" + name
    return VertexUnary(t, t, name, f=_identity, kind=VertexKind.DELAY).g


# Generic over TNegable | TAddable


def neg(
    name: str,
    t: type[TNegable],
) -> Graph[A1[TNegable], A1[TNegable]]:
    name = "neg_" + name
    return VertexUnary(t, t, name, f=_neg).g


def add(
    name: str,
    t: type[TAddable],
) -> Graph[A2[TAddable, TAddable], A1[TAddable]]:
    name = "add_" + name
    return VertexBinary(t, t, t, name, f=_add, kind=VertexKind.ADD).g


def add3(
    name: str,
    t: type[TAddable],
) -> Graph[A3[TAddable, TAddable, TAddable], A1[TAddable]]:
    name = "_add3" + name
    return stack(add("_1" + name, t), identity("_2" + name, t)).connect(add(name, t))


def integrate(
    name: str,
    t: type[TAddable],
    output_delay: bool = False,
) -> Graph[A1[TAddable], A1[TAddable]]:
    """Definition 3.27"""
    name = "_integrate" + name

    i = get_single_vertex(identity(name, t))  # this can come in handy later
    a = get_single_vertex(add(name, t))
    d = get_single_vertex(delay(name, t))
    return Graph[A1[TAddable], A1[TAddable]](
        vertices=[i, a, d],
        input=[(i, 0)],
        internal={
            (i, (a, 1)),
            (a, (d, 0)),
            (d, (a, 0)),
        },
        output=[d if output_delay else a],
    )


def differentiate(
    name: str,
    t: type[TAddAndNegable],
) -> Graph[A1[TAddAndNegable], A1[TAddAndNegable]]:
    """Definition 3.25"""
    name = "_differentiate" + name

    d = delay(name, t)
    return identity("_1" + name, t).connect(
        stack(
            identity("_2" + name, t),
            d.connect(neg(name, t)),
        ).connect(
            add(name, t),
        ),
    )


# Generic over ZSet[T]


def map(
    name: str,
    t: type[T],
    v: type[V],
    f: Callable[[T], V],
) -> Graph[A1[ZSet[T]], A1[ZSet[V]]]:
    name = "map_" + name

    def z_map(n: ZSet[T]) -> ZSet[V]:
        return zset.map(n, f)

    return VertexUnary(R[ZSet[T]].sub(T=t), R[ZSet[V]].sub(V=v), name, f=z_map).g


def filter(
    name: str,
    t: type[T],
    f: Callable[[T], bool],
) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    name = "filter_" + name

    def z_map(n: ZSet[T]) -> ZSet[T]:
        return zset.filter(n, f)

    return VertexUnary(R[ZSet[T]].sub(T=t), R[ZSet[T]].sub(T=t), name, f=z_map).g


def reduce(
    name: str,
    t: type[T],
    v: type[V],
    reduce: Callable[[ZSet[T]], V],
) -> Graph[A1[ZSet[T]], A1[V]]:
    name = "reduce_" + name

    return VertexUnary(R[ZSet[T]].sub(T=t), v, name, reduce).g


def make_set(
    name: str,
    t: type[T],
) -> Graph[A1[T], A1[ZSet[T]]]:
    """Section 11.1"""
    name = "make_set_" + name
    return VertexUnary(t, R[ZSet[T]].sub(T=t), name, _z_make_set).g


def make_scalar(
    name: str,
    t: type[T],
) -> Graph[A1[ZSet[T]], A1[T]]:
    name = "make_scalar_" + name

    def z_make_scalar(a: ZSet[T]) -> T:
        values_counts = list(a.iter())
        if len(values_counts) == 0:
            return t()
        if len(values_counts) == 1:
            ([value, count],) = values_counts
            if count == 1:
                return value
        raise RuntimeError("Can only make scalars from ZSets length 1, count 1")

    return VertexUnary(R[ZSet[T]].sub(T=t), t, name, z_make_scalar).g


def haitch(
    name: str,
    t: type[T],
) -> Graph[A2[ZSet[T], ZSet[T]], A1[ZSet[T]]]:
    name = "haitch_" + name
    zset_t = R[ZSet[T]].sub(T=t)
    return VertexBinary(zset_t, zset_t, zset_t, name, f=_z_haitch).g


def join(
    name: str,
    t: type[T],
    u: type[U],
    on_left: Index[T, K],
    on_right: Index[U, K],
) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[Pair[T, U]]]]:
    name = "join_" + name

    def z_join(
        l: ZSet[T],
        r: ZSet[U],
    ) -> ZSet[Pair[T, U]]:
        return zset.join(l, r, on_left, on_right)

    return VertexBinary(
        R[ZSet[T]].sub(T=t),
        R[ZSet[U]].sub(U=u),
        R[ZSet[Pair[T, U]]].sub(T=t, U=u),
        name,
        f=z_join,
    ).g


def first_n(
    name: str, t: type[T], index: Index[T, K], n: int
) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    name = "first_n_" + name

    def first_n(z: ZSet[T]) -> ZSet[T]:
        return zset.first_n(z, index, n)

    return VertexUnary(
        R[Annotated[ZSet[T], index]].sub(T=t, K=index.k),
        R[Annotated[ZSet[T], index]].sub(T=t, K=index.k),
        name,
        f=first_n,
    ).g
