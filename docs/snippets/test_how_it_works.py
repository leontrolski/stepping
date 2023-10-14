from pathlib import Path
from textwrap import dedent
import stepping as st

IMAGES = (Path(__file__).parents[2] / "docs/doks/assets/images").resolve()
assert IMAGES.exists()


class A(st.Data):
    x: int
    name: str


class B(st.Data):
    y: int
    name: str


def test_a() -> None:
    a = st.ZSetPython({A(x=24, name="Bob"): 1})
    b = st.ZSetPython({A(x=24, name="Bob"): 3, A(x=4, name="Steve"): -1})
    assert a + b == st.ZSetPython({A(x=24, name="Bob"): 4, A(x=4, name="Steve"): -1})


def query_delay(a: st.ZSet[str]) -> st.ZSet[str]:
    delayed = st.delay(a)
    return delayed


def test_b() -> None:
    graph = st.compile(query_delay)
    store = st.StorePython.from_graph(graph)

    (output,) = st.iteration(store, graph, (st.ZSetPython({"first": 1}),))
    assert output == st.ZSetPython()

    (output,) = st.iteration(store, graph, (st.ZSetPython({"second": 1}),))
    assert output == st.ZSetPython({"first": 1})


def _upper(a: A) -> A:
    return A(x=a.x, name=a.name.upper())


from stepping.operators import linear


def query_graph(a: st.ZSet[A], b: st.ZSet[B]) -> st.ZSet[st.Pair[A, B]]:
    a_uppered = st.map(a, f=_upper)
    joined = linear.join(
        a_uppered,
        b,
        on_left=st.Index.pick(A, lambda a: a.name),
        on_right=st.Index.pick(B, lambda b: b.name),
    )
    integrated = st.integrate(joined)
    return integrated


def test_c() -> None:
    graph = st.compile(query_graph)
    # st.write_png(graph, str(IMAGES / "test_how_it_works_c.png"))


def query_integrate(a: st.ZSet[str]) -> st.ZSet[str]:
    integrated = st.integrate(a)
    return integrated


def test_d() -> None:
    graph = st.compile(query_integrate)
    store = st.StorePython.from_graph(graph)

    (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 1}),))
    assert output == st.ZSetPython({"a": 1})

    (output,) = st.iteration(store, graph, (st.ZSetPython({"b": -1}),))
    assert output == st.ZSetPython({"a": 1, "b": -1})

    (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 4}),))
    assert output == st.ZSetPython({"a": 5, "b": -1})


def query_differentiate(a: st.ZSet[str]) -> st.ZSet[str]:
    differentiated = st.differentiate(a)
    return differentiated


def test_e() -> None:
    graph = st.compile(query_differentiate)
    store = st.StorePython.from_graph(graph)

    (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 1}),))
    assert output == st.ZSetPython({"a": 1})

    (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 1, "b": -1}),))
    assert output == st.ZSetPython({"b": -1})

    (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 5, "b": -1}),))
    assert output == st.ZSetPython({"a": 4})


def query_dumb(a: st.ZSet[A], b: st.ZSet[B]) -> st.ZSet[st.Pair[A, B]]:
    a_integrated = st.integrate(a)
    b_integrated = st.integrate(b)
    joined = linear.join(
        a_integrated,
        b_integrated,
        on_left=st.Index.pick(A, lambda a: a.name),
        on_right=st.Index.pick(B, lambda b: b.name),
    )
    differentiated = st.differentiate(joined)
    return differentiated


def test_f() -> None:
    graph = st.compile(query_dumb)
    store = st.StorePython.from_graph(graph)

    (output,) = st.iteration(
        store,
        graph,
        (
            st.ZSetPython({A(x=1, name="Bob"): 1, A(x=2, name="Jeff"): 1}),
            st.ZSetPython({B(y=3, name="Bob"): 1}),
        ),
    )
    expected_str = dedent(
        """
        <ZSetPython>
        ╒═══════════╤════════════════════╤════════════════════╕
        │   _count_ │ left               │ right              │
        ╞═══════════╪════════════════════╪════════════════════╡
        │         1 │ A(x=1, name='Bob') │ B(y=3, name='Bob') │
        ╘═══════════╧════════════════════╧════════════════════╛
        """
    ).strip()
    assert set(str(output).splitlines()) == set(str(expected_str).splitlines())

    (output,) = st.iteration(
        store,
        graph,
        (
            st.ZSetPython[A](),
            st.ZSetPython({B(y=4, name="Bob"): 2}),
        ),
    )
    expected_str = dedent(
        """
        <ZSetPython>
        ╒═══════════╤════════════════════╤════════════════════╕
        │   _count_ │ left               │ right              │
        ╞═══════════╪════════════════════╪════════════════════╡
        │         2 │ A(x=1, name='Bob') │ B(y=4, name='Bob') │
        ╘═══════════╧════════════════════╧════════════════════╛
        """
    ).strip()
    assert set(str(output).splitlines()) == set(str(expected_str).splitlines())

    (output,) = st.iteration(
        store,
        graph,
        (
            st.ZSetPython({A(x=1, name="Bob"): -1}),
            st.ZSetPython[B](),
        ),
    )
    expected_str = dedent(
        """
        <ZSetPython>
        ╒═══════════╤════════════════════╤════════════════════╕
        │   _count_ │ left               │ right              │
        ╞═══════════╪════════════════════╪════════════════════╡
        │        -1 │ A(x=1, name='Bob') │ B(y=3, name='Bob') │
        ├───────────┼────────────────────┼────────────────────┤
        │        -2 │ A(x=1, name='Bob') │ B(y=4, name='Bob') │
        ╘═══════════╧════════════════════╧════════════════════╛
        """
    ).strip()
    assert set(str(output).splitlines()) == set(str(expected_str).splitlines())

    # (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 1, "b": -1}),))
    # assert output == st.ZSetPython({"b": -1})

    # (output,) = st.iteration(store, graph, (st.ZSetPython({"a": 5, "b": -1}),))
    # assert output == st.ZSetPython({"a": 4})


def _zero_zset() -> st.ZSetPython[str]:
    return st.ZSetPython()


def _pick_zset(n: str) -> st.ZSetPython[str]:
    return st.ZSetPython({n: 1})


def sum_by_length(a: st.ZSet[str]) -> st.ZSet[st.Pair[st.ZSetPython[str], int]]:
    grouped = st.group_reduce_flatten(
        a,
        by=st.Index.atom("length", str, int, lambda n: len(n)),
        zero=_zero_zset,
        pick_value=_pick_zset,
    )
    return grouped


def test_g() -> None:
    graph = st.compile(sum_by_length)
    store = st.StorePython.from_graph(graph)
    (action,) = st.actions(store, graph)
    (output,) = action.insert("foo", "bar", "hullo")
    expected_str = dedent(
        """
        <ZSetPython>
        ╒═══════════╤═══════════════════════════╤═════════╕
        │   _count_ │ left                      │   right │
        ╞═══════════╪═══════════════════════════╪═════════╡
        │         1 │ <ZSetPython>              │       5 │
        │           │ ╒═══════════╤═══════════╕ │         │
        │           │ │   _count_ │ _value_   │ │         │
        │           │ ╞═══════════╪═══════════╡ │         │
        │           │ │         1 │ hullo     │ │         │
        │           │ ╘═══════════╧═══════════╛ │         │
        ├───────────┼───────────────────────────┼─────────┤
        │         1 │ <ZSetPython>              │       3 │
        │           │ ╒═══════════╤═══════════╕ │         │
        │           │ │   _count_ │ _value_   │ │         │
        │           │ ╞═══════════╪═══════════╡ │         │
        │           │ │         1 │ foo       │ │         │
        │           │ ├───────────┼───────────┤ │         │
        │           │ │         1 │ bar       │ │         │
        │           │ ╘═══════════╧═══════════╛ │         │
        ╘═══════════╧═══════════════════════════╧═════════╛
        """
    ).strip()
    assert set(str(output).splitlines()) == set(str(expected_str).splitlines())

    (output,) = action.remove("foo")
    expected_str = dedent(
        """
        <ZSetPython>
        ╒═══════════╤═══════════════════════════╤═════════╕
        │   _count_ │ left                      │   right │
        ╞═══════════╪═══════════════════════════╪═════════╡
        │        -1 │ <ZSetPython>              │       3 │
        │           │ ╒═══════════╤═══════════╕ │         │
        │           │ │   _count_ │ _value_   │ │         │
        │           │ ╞═══════════╪═══════════╡ │         │
        │           │ │         1 │ foo       │ │         │
        │           │ ├───────────┼───────────┤ │         │
        │           │ │         1 │ bar       │ │         │
        │           │ ╘═══════════╧═══════════╛ │         │
        ├───────────┼───────────────────────────┼─────────┤
        │         1 │ <ZSetPython>              │       3 │
        │           │ ╒═══════════╤═══════════╕ │         │
        │           │ │   _count_ │ _value_   │ │         │
        │           │ ╞═══════════╪═══════════╡ │         │
        │           │ │         1 │ bar       │ │         │
        │           │ ╘═══════════╧═══════════╛ │         │
        ╘═══════════╧═══════════════════════════╧═════════╛
        """
    ).strip()
    assert set(str(output).splitlines()) == set(str(expected_str).splitlines())

    # st.write_png(graph, str(IMAGES / "test_how_it_works_g.png"))
