from typing import Annotated, Any, Callable, cast

from stepping import graph
from stepping.graph import A1, A2, Graph
from stepping.operators import linear
from stepping.types import EMPTY, Empty, Index, K, Pair
from stepping.types import RuntimeComposite as R
from stepping.types import T, TAddable, TReducable, U, ZSet

# Generic factories


def _incrementalise_aggregate_linear(
    name: str,
    t: type[T],
    v: type[TAddable],
    reduce: Callable[[ZSet[T]], TAddable],
) -> Graph[A1[ZSet[T]], A1[ZSet[TAddable]]]:
    """Section 11.1"""
    name = "_aggregate_linear" + name

    s = linear.reduce(name, t, v, reduce)
    i = linear.integrate(name, v)
    m = linear.make_set(name, v)
    d = linear.differentiate(name, R[ZSet[TAddable]].sub(TAddable=v))

    return s.connect(i).connect(m).connect(d)


# Generic over ZSet[T]


def distinct(
    name: str,
    t: type[T],
) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    """Proposition 6.3"""
    name = "_distinct" + name
    t_zset = R[ZSet[T]].sub(T=t)

    d = linear.delay(name, t_zset)
    i = linear.integrate(name, t_zset)
    return linear.identity("_1" + name, t_zset).connect(
        graph.stack(
            i.connect(d),
            linear.identity("_2" + name, t_zset),
        ).connect(
            linear.haitch(name, t),
        ),
    )


def join(
    name: str,
    t: type[T],
    u: type[U],
    on_t: Index[T, K],
    on_u: Index[U, K],
) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[Pair[T, U]]]]:
    """Theorem 5.5"""
    name = "_join" + name

    it = linear.identity("_1" + name, R[ZSet[T]].sub(T=t))
    iu = linear.identity("_2" + name, R[ZSet[U]].sub(U=u))

    join = graph.stack(it, iu).connect(linear.join(name, t, u, on_t, on_u))
    integrate_t = linear.integrate(
        "_t" + name,
        R[Annotated[ZSet[T], on_t]].sub(T=t, K=on_t.k),
        output_delay=True,
    )
    integrate_u = linear.integrate(
        "_u" + name,
        R[Annotated[ZSet[U], on_u]].sub(U=u, K=on_u.k),
        output_delay=True,
    )
    it_z_op = graph.stack(
        it.connect(integrate_t),
        iu,
    ).connect(linear.join("_t" + name, t, u, on_t, on_u))
    iu_z_op = graph.stack(
        it,
        iu.connect(integrate_u),
    ).connect(linear.join("_u" + name, t, u, on_t, on_u))
    unioned = graph.union_inputs(graph.union_inputs(it_z_op, iu_z_op), join)
    return unioned.connect(linear.add3(name, R[ZSet[Pair[T, U]]].sub(T=t, U=u)))


def outer_join(
    name: str,
    t: type[T],
    u: type[U],
    on_t: Index[T, K],
    on_u: Index[U, K],
) -> Graph[A2[ZSet[T], ZSet[U]], A1[ZSet[Pair[T, U | Empty]]]]:
    # This looks _way_ more complicated than it is, see .png
    name = "_outer_join" + name

    it1 = linear.identity("_t1" + name, R[ZSet[T]].sub(T=t))
    it2 = linear.identity("_t2" + name, R[ZSet[T]].sub(T=t))
    it3 = linear.identity("_t3" + name, R[ZSet[T]].sub(T=t))
    it4 = linear.identity("_t4" + name, R[ZSet[T]].sub(T=t))
    it5 = linear.identity("_t5" + name, R[ZSet[T]].sub(T=t))
    iu1 = linear.identity("_u1" + name, R[ZSet[U]].sub(U=u))
    iu2 = linear.identity("_u2" + name, R[ZSet[U]].sub(U=u))
    ip1 = linear.identity("_ip1" + name, R[ZSet[Pair[T, U]]].sub(T=t, U=u))
    ip2 = linear.identity("_ip2" + name, R[ZSet[Pair[T, U]]].sub(T=t, U=u))
    ip3 = linear.identity("_ip3" + name, R[ZSet[Pair[T, U]]].sub(T=t, U=u))
    ip4 = linear.identity("_ip4" + name, R[ZSet[Pair[T, U]]].sub(T=t, U=u))

    j = join(name, t, u, on_t, on_u)
    left = linear.map(
        "_left" + name,
        R[Pair[T, U]].sub(T=t, U=u),
        R[T].sub(T=t),
        lambda p: p.left,
    )
    neg = linear.neg(name, R[ZSet[T]].sub(T=t))
    add1 = linear.add("_1" + name, R[ZSet[T]].sub(T=t))
    empty = linear.map(
        "_empty" + name,
        R[T].sub(T=t),
        R[Pair[T, U | Empty]].sub(T=t, U=u),
        lambda v: Pair(v, EMPTY),
    )
    add2 = linear.add("_2" + name, R[ZSet[Pair[T, U | Empty]]].sub(T=t, U=u))

    # Hack to make the run-time types for the following add work
    graph.get_single_vertex(ip4).v = R[ZSet[Pair[T, U | Empty]]].sub(T=t, U=u)  # type: ignore
    ip4_typed = cast(Graph[A1[ZSet[Pair[T, U]]], A1[ZSet[Pair[T, U | Empty]]]], ip4)

    stack1 = graph.stack(it1.connect(graph.stack(it2, it3)), iu1.connect(iu2))
    stack2 = graph.stack(it4, j.connect(graph.stack(ip1, ip2)))
    stack3 = graph.stack(it5, graph.stack(left.connect(neg), ip3))
    stack4 = graph.stack(add1.connect(empty), ip4_typed)
    out = stack1.connect(stack2).connect(stack3).connect(stack4).connect(add2)
    return out


def sum(
    name: str,
    t: type[T],
    v: type[TReducable],
    pick_number: Callable[[T], TReducable],
) -> Graph[A1[ZSet[T]], A1[ZSet[TReducable]]]:
    name = "_sum" + name
    zero = v()

    def reduce(z: ZSet[T]) -> TReducable:
        total = zero
        for v, count in z.iter():
            total += pick_number(v) * count
        return total

    return _incrementalise_aggregate_linear(name, t, v, reduce)


def count(
    name: str,
    t: type[T],
) -> Graph[A1[ZSet[T]], A1[ZSet[int]]]:
    name = "_count" + name

    def reduce(z: ZSet[Any]) -> int:
        total = 0
        for _, count in z.iter():
            total += count
        return total

    return _incrementalise_aggregate_linear(name, R[T].sub(T=t), int, reduce)


def first_n(
    name: str,
    t: type[T],
    index: Index[T, K],
    n: int,
) -> Graph[A1[ZSet[T]], A1[ZSet[T]]]:
    """Section 11.1

    "Not incremental in general, since for handling deletions
    they may need to know the full set and not just its changes."
    """
    name = "_first_n" + name

    i = linear.integrate(name, R[Annotated[ZSet[T], index]].sub(T=t, K=index.k))
    f = linear.first_n(name, t, index, n)
    d = linear.differentiate(name, R[Annotated[ZSet[T], index]].sub(T=t, K=index.k))
    return i.connect(f).connect(d)


def reduce(
    name: str,
    t: type[T],
    v: type[TReducable],
    pick_reducable: Callable[[T], TReducable],
) -> Graph[A1[ZSet[T]], A1[ZSet[TReducable]]]:
    name = "_reduce" + name

    def reduce(z: ZSet[T]) -> TReducable:
        total: TReducable = v()
        for a, count in z.iter():
            total += pick_reducable(a) * count
        return total

    return _incrementalise_aggregate_linear(name, t, v, reduce)
