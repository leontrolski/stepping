import cProfile
import pathlib
from textwrap import dedent
from typing import Any

import stepping as st

SQLITE_PATH = pathlib.Path(__file__).parent / "stepping-docs-test.db"
SQLITE_PATH_LOADS = pathlib.Path(__file__).parent / "stepping-docs-test-typechecker.db"


# reference: class-class
class Class(st.Data):
    identifier: str  # eg: "one.A"
    attrs: tuple[tuple[str, str], ...]
# /reference: class-class

class Attr(st.Data):
    identifier: str  # eg: "one.A"
    key: str  # eg: "x"
    value: str  # eg: "int" or "one.A"


class A(st.Data):
    key: str
    value: "str | tuple[A, ...]"


class Resolved(st.Data):
    identifier: str  # eg: "one.A"
    attrs: tuple[A, ...]


def to_many_attrs(c: Class) -> frozenset[Attr]:
    return frozenset(
        Attr(identifier=c.identifier, key=key, value=value) for key, value in c.attrs
    )


def to_edge(a: Attr) -> st.Pair[str, str]:
    return st.Pair(a.identifier, a.value)


def zset_zero() -> st.ZSetPython[Class]:
    return st.ZSetPython[Class]()


def pick_zset(p: st.Pair[st.Pair[str, str], Class]) -> st.ZSetPython[Class]:
    return st.ZSetPython[Class]({p.right: 1})


def resolve(
    p: st.Pair[Class, st.Pair[st.ZSetPython[Class], str] | st.Empty]
) -> Resolved:
    from_class = p.left
    identifier_to_attrs = {
        to_class.identifier: to_class.attrs
        for to_class, _ in (
            [] if isinstance(p.right, st.Empty) else p.right.left.iter()
        )
    }

    def f(key_values: tuple[tuple[str, str], ...]) -> tuple[A, ...]:
        out = tuple[A, ...]()
        for [key, value] in key_values:
            if value in identifier_to_attrs:
                out += (A(key=key, value=f(identifier_to_attrs[value])),)
            else:
                out += (A(key=key, value=value),)
        return out

    return Resolved(identifier=from_class.identifier, attrs=f(from_class.attrs))


output_cache = st.Cache[Resolved]()

# reference: query
def link_attrs(classes: st.ZSet[Class]) -> st.ZSet[Resolved]:
    attrs = st.map_many(classes, f=to_many_attrs)
    edges = st.map(attrs, f=to_edge)
    all_edges = st.transitive_closure(edges)

    from_to = st.join(
        all_edges,
        classes,
        on_left=st.pick_index(st.Pair[str, str], lambda p: p.right),
        on_right=st.pick_index(Class, lambda a: a.identifier),
    )
    grouped_by_from_identifier = st.group_reduce_flatten(
        from_to,
        by=st.pick_index(st.Pair[st.Pair[str, str], Class], lambda p: p.left.left),
        zero=zset_zero,
        pick_value=pick_zset,
    )
    from_joined_to_relevant = st.outer_join(
        classes,
        grouped_by_from_identifier,
        on_left=st.pick_index(Class, lambda a: a.identifier),
        on_right=st.pick_index(st.Pair[st.ZSetPython[Class], str], lambda p: p.right),
    )
    resolved = st.map(from_joined_to_relevant, f=resolve)
    _ = output_cache[resolved](lambda r:  st.integrate(r))
    return resolved
# /reference: query


graph = st.compile_lazy(link_attrs)

# attrs: st.ZSet[Attr]
# edges: st.ZSet[st.Pair[str, str]]
# all_edges: st.ZSet[st.Pair[str, str]]
# from_to: st.ZSet[st.Pair[st.Pair[str, str], Class]]
# grouped_by_from_identifier: st.ZSet[st.Pair[st.ZSetPython[Class], str]]
# from_joined_to_relevant: st.ZSet[st.Pair[Class, st.Pair[st.ZSetPython[Class], str] | st.Empty]]
# resolved: st.ZSet[Resolved]


def test_typechecker(request: Any) -> None:
    SQLITE_PATH.unlink(missing_ok=True)
    if request.config.getoption("--write-graphs"):
        st.write_png(graph(), "graphs/test_typechecker.png")

    with st.connection_sqlite(SQLITE_PATH) as conn:
        store = st.StoreSQLite.from_graph(conn, graph(), create_tables=True)
        (action,) = st.actions(store, graph())
        (actual,) = action.insert(*input_data)
        assert output_cache.zset(store) == st.ZSetPython(
            {v: 1 for v in expected_output}
        )
        assert actual == st.ZSetPython({v: 1 for v in expected_output})

        expected = dedent(
        """
            <ZSetPython>
            ╒═══════════╤══════════════════════════════════════════════════════════════════════════════════════════════════╕
            │   _count_ │ _value_                                                                                          │
            ╞═══════════╪══════════════════════════════════════════════════════════════════════════════════════════════════╡
            │         1 │ identifier='one.C' attrs=(A(key='z', value=(A(key='y', value=(A(key='x', value='str'),)),)),)    │
            ├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
            │         1 │ identifier='one.B' attrs=(A(key='y', value=(A(key='x', value='str'),)),)                         │
            ├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
            │         1 │ identifier='one.A' attrs=(A(key='x', value='str'),)                                              │
            ├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
            │         1 │ identifier='two.E' attrs=(A(key='x', value='float'),)                                            │
            ├───────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
            │         1 │ identifier='two.D' attrs=(A(key='y', value=(A(key='x', value='str'),)), A(key='z', value='int')) │
            ╘═══════════╧══════════════════════════════════════════════════════════════════════════════════════════════════╛
            """
        ).strip()
        assert set(str(actual).splitlines()) == set(expected.splitlines())

        to_remove = Class(identifier="two.E", attrs=(("x", "float"),))
        expected_removed = st.ZSetPython(
            {Resolved(identifier="two.E", attrs=(A(key="x", value="float"),)): -1}
        )
        (actual,) = action.remove(to_remove)
        assert actual == expected_removed

        to_remove = Class(identifier="one.B", attrs=(("y", "one.A"),))
        expected_removed = st.ZSetPython(
            {
                Resolved(
                    identifier="one.B",
                    attrs=(A(key="y", value=(A(key="x", value="str"),)),),
                ): -1,
                Resolved(
                    identifier="one.C",
                    attrs=(
                        A(
                            key="z",
                            value=(A(key="y", value=(A(key="x", value="str"),)),),
                        ),
                    ),
                ): -1,
                Resolved(identifier="one.C", attrs=(A(key="z", value="one.B"),)): 1,
            }
        )
        (actual,) = action.remove(to_remove)
        assert actual == expected_removed

        expected = dedent(
        """
            <ZSetPython>
            ╒═══════════╤═══════════════════════════════════════════════════════════════════════════════════════════════╕
            │   _count_ │ _value_                                                                                       │
            ╞═══════════╪═══════════════════════════════════════════════════════════════════════════════════════════════╡
            │        -1 │ identifier='one.C' attrs=(A(key='z', value=(A(key='y', value=(A(key='x', value='str'),)),)),) │
            ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
            │         1 │ identifier='one.C' attrs=(A(key='z', value='one.B'),)                                         │
            ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
            │        -1 │ identifier='one.B' attrs=(A(key='y', value=(A(key='x', value='str'),)),)                      │
            ╘═══════════╧═══════════════════════════════════════════════════════════════════════════════════════════════╛
            """
        ).strip()
        assert set(str(actual).splitlines()) == set(expected.splitlines())


def test_read_tables_again() -> None:
    with st.connection_sqlite(SQLITE_PATH) as conn:
        store = st.StoreSQLite.from_graph(conn, graph(), create_tables=False)
        zset = output_cache.zset(store)
        assert isinstance(zset, st.ZSetSQLite)
        assert len(list(zset.to_python().iter())) == 3

    SQLITE_PATH.unlink()

# reference: input-data
input_data = [
    Class(identifier="one.A", attrs=(("x", "str"),)),
    Class(identifier="one.B", attrs=(("y", "one.A"),)),
    Class(identifier="one.C", attrs=(("z", "one.B"),)),
    Class(identifier="two.D", attrs=(("y", "one.A"), ("z", "int"))),
    Class(identifier="two.E", attrs=(("x", "float"),)),
]
# /reference: input-data

expected_output = [
    Resolved(identifier="one.A", attrs=(A(key="x", value="str"),)),
    Resolved(
        identifier="one.B",
        attrs=(A(key="y", value=(A(key="x", value="str"),)),),
    ),
    Resolved(
        identifier="one.C",
        attrs=(
            A(
                key="z",
                value=(A(key="y", value=(A(key="x", value="str"),)),),
            ),
        ),
    ),
    Resolved(
        identifier="two.D",
        attrs=(
            A(key="y", value=(A(key="x", value="str"),)),
            A(key="z", value="int"),
        ),
    ),
    Resolved(identifier="two.E", attrs=(A(key="x", value="float"),)),
]


def test_typechecker_make_loads(request: Any) -> None:
    SQLITE_PATH_LOADS.unlink(missing_ok=True)

    N = 1_000_000
    N = 10
    input_data_loads = list[Class]()
    for i in range(N):
        input_data_loads.extend(
            [
                Class(identifier=f"one.A.{i}", attrs=(("x", "str"),)),
                Class(identifier=f"one.B.{i}", attrs=(("y", f"one.A.{i}"),)),
                Class(identifier=f"one.C.{i}", attrs=(("z", f"one.B.{i}"),)),
                Class(
                    identifier=f"two.D.{i}", attrs=(("y", f"one.A.{i}"), ("z", "int"))
                ),
                Class(identifier=f"two.E.{i}", attrs=(("x", "float"),)),
            ]
        )

    with st.connection_sqlite(SQLITE_PATH_LOADS) as conn:
        store = st.StoreSQLite.from_graph(conn, graph(), create_tables=True)
        (action,) = st.actions(store, graph())
        for chunk in st.batched(input_data_loads, 100):
            action.insert(*chunk)

        zset = output_cache.zset(store)
        assert isinstance(zset, st.ZSetSQLite)
        assert len(list(zset.to_python().iter())) == 5 * N

        with cProfile.Profile() as pr:
            action.insert(*input_data)
        pr.dump_stats("test_typechecker_make_loads.prof")

    SQLITE_PATH_LOADS.unlink()
