from __future__ import annotations

import subprocess
from datetime import date, datetime, timezone
from typing import Annotated as A
from typing import Any

from steppingpack import AwareUTCDatetime, Data

from stepping.types import Index, ZSet
from stepping.zset import functions
from stepping.zset.python import ZSetPython
from stepping.zset.sql import generic, sqlite


def _flush(z: generic.ZSetSQL[Any]) -> None:
    z.upsert()
    z.changes = ()


def dump_schema(conn: generic.ConnSQLite) -> str:
    db_url = conn.execute("PRAGMA database_list").fetchone()[2]
    return subprocess.check_output(f"sqlite3 {db_url} .schema", shell=True).decode()


def test_create_table(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    ix = Index.identity(int)
    sqlite.ZSetSQLite[int](cur, int, "foo", (ix,))


class Animal(Data):
    name: A[str, 1]
    sound: A[str, 2]
    age: A[int, 3]
    created: A[AwareUTCDatetime, 4]


def test_typing(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()

    animal = Animal(
        name="fido",
        sound="woof",
        age=38,
        created=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    z: ZSet[int] = sqlite.ZSetSQLite[int](cur, int, "foo", ())


def test_write_simple_int(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, int, "foo", ())
    z.create_data_table()
    changes = ZSetPython(int, {42: 1, 56: 2, 78: -1})
    z += changes

    actual = list(z.iter(((42, 78))))
    assert actual == [(42, 1), (78, -1)]

    _flush(z)
    assert z.to_python() == changes

    actual = list(z.iter(((42, 78))))
    assert actual == [(42, 1), (78, -1)]


def test_write_simple_int_with_index(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, int, "foo", (Index.identity(int),))
    z.create_data_table()
    changes = ZSetPython(int, {42: 1, 56: 2})
    z += changes
    _flush(z)
    assert z.to_python() == changes

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)
    assert "ixd__identity__identity INTEGER NOT NULL" in schema
    assert "CREATE INDEX ix__foo__identity ON foo(ixd__identity__identity)" in schema


def test_write_simple_date(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, date, "foo", ())
    z.create_data_table()
    changes = ZSetPython(date, {date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    _flush(z)
    assert z.to_python() == changes


def test_write_simple_date_with_index(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(
        cur,
        date,
        "foo",
        (Index.identity(date),),
    )
    z.create_data_table()
    changes = ZSetPython(date, {date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    _flush(z)
    assert z.to_python() == changes

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)
    assert "ixd__identity__identity TEXT NOT NULL" in schema
    assert "CREATE INDEX ix__foo__identity ON foo(ixd__identity__identity)" in schema


def test_write_complex(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, Animal, "foo", ())
    z.create_data_table()

    animal = Animal(
        name="fido",
        sound="woof",
        age=38,
        created=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    changes = ZSetPython(Animal, [(animal, 2)])
    z += changes
    _flush(z)
    assert z.to_python() == changes


def test_write_complex_update(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, Animal, "foo", ())
    z.create_data_table()

    animal = Animal(
        name="fido",
        sound="woof",
        age=38,
        created=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    animal_new = Animal(
        name="fido",
        sound="woofff",
        age=38,
        created=datetime(2022, 6, 1, tzinfo=timezone.utc),
    )

    changes = ZSetPython(Animal, [(animal, 1)])
    z += changes
    _flush(z)
    assert z.to_python() == changes

    changes = ZSetPython(Animal, [(animal, -1), (animal_new, 1)])
    z += changes
    _flush(z)

    expected = ZSetPython(Animal, [(animal_new, 1)])
    assert z.to_python() == expected


class Bar(Data):
    bingo: A[str, 1]


class Foo(Data):
    name: A[str | None, 1]
    created: A[date, 2]
    age: A[int, 3]
    parent: A[Bar, 4]


def test_schema_made_and_used(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    ix_namely = Index.pick(Foo, lambda f: f.name)
    ix_name_and_age = Index.pick(Foo, lambda f: (f.name, f.age))
    ix_parent_bingo = Index.pick(
        Foo, lambda f: (f.name, f.parent.bingo), ascending=(False, True)
    )
    ix_created = Index.pick(Foo, lambda f: f.created)

    z = sqlite.ZSetSQLite(
        cur,
        Foo,
        "foo",
        (ix_namely, ix_name_and_age, ix_parent_bingo, ix_created),
    )
    z.create_data_table()

    first_foo = Foo(
        name="a-name", created=date(2023, 1, 4), age=23, parent=Bar(bingo="asd")
    )
    changes = ZSetPython(
        Foo,
        [(first_foo, 1)]
        + [
            (
                Foo(
                    name=f"some-name-{i % 3}",
                    created=date(2023, 1, 20),
                    age=i % 10,
                    parent=Bar(bingo=f"pi-{i}"),
                ),
                2,
            )
            for i in range(33)
        ],
    )
    z += changes
    _flush(z)

    actual = list(z.iter(((first_foo,))))
    assert actual == [(first_foo, 1)]

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)

    # fmt:off
    assert "ixd__name_age__name TEXT NOT NULL" in schema
    assert "ixd__name_age__age INTEGER NOT NULL" in schema
    assert "ixd__name_parent_bingo__name TEXT NOT NULL" in schema
    assert "ixd__name_parent_bingo__parent_bingo TEXT NOT NULL" in schema
    assert "ixd__created__created TEXT NOT NULL" in schema
    assert "CREATE INDEX ix__foo__name ON foo(ixd__name__name)" in schema
    assert "CREATE INDEX ix__foo__name_age ON foo(ixd__name_age__name, ixd__name_age__age)" in schema
    assert "CREATE INDEX ix__foo__name_parent_bingo ON foo(ixd__name_parent_bingo__name DESC, ixd__name_parent_bingo__parent_bingo)" in schema
    assert "CREATE INDEX ix__foo__created ON foo(ixd__created__created)" in schema
    # fmt:on

    # plan = postgres.explain(
    #     sqlite_conn,
    #     "SELECT * FROM foo WHERE (data #>> '{name}') = 'some-name' AND (data #>> '{age}')::integer = 3",
    # )
    # assert "Index Scan" in plan

    # plan = postgres.explain(sqlite_conn, "SELECT * FROM foo ORDER BY (data #>> '{name}')")
    # assert "Index Scan" in plan

    z_generic: ZSet[Foo] = z

    all_keys = [k for k, _ in functions.iter_by_index_grouped(z_generic, ix_created)]
    assert all_keys == [date(2023, 1, 4), date(2023, 1, 20)]

    first_keys = [
        k for k, _ in functions.iter_by_index_grouped(z_generic, ix_name_and_age)
    ][:5]
    assert first_keys == [
        ("a-name", 23),
        ("some-name-0", 0),
        ("some-name-0", 1),
        ("some-name-0", 2),
        ("some-name-0", 3),
    ]

    first_inner = [
        set(inner)
        for _, inner in functions.iter_by_index_grouped(z_generic, ix_name_and_age)
    ][0]
    expected = {
        (
            Foo(
                name="a-name",
                created=date(2023, 1, 4),
                age=23,
                parent=Bar(bingo="asd"),
            ),
            1,
        )
    }
    assert first_inner == expected

    second_inner = [
        set(inner)
        for _, inner in functions.iter_by_index_grouped(z_generic, ix_name_and_age)
    ][1]
    expected = {
        (
            Foo(
                name="some-name-0",
                created=date(2023, 1, 20),
                age=0,
                parent=Bar(bingo="pi-30"),
            ),
            2,
        ),
        (
            Foo(
                name="some-name-0",
                created=date(2023, 1, 20),
                age=0,
                parent=Bar(bingo="pi-0"),
            ),
            2,
        ),
    }
    assert second_inner == expected

    first_keys_bingo = [
        k for k, _ in functions.iter_by_index_grouped(z_generic, ix_parent_bingo)
    ][:5]
    assert first_keys_bingo == [
        ("some-name-2", "pi-11"),
        ("some-name-2", "pi-14"),
        ("some-name-2", "pi-17"),
        ("some-name-2", "pi-2"),
        ("some-name-2", "pi-20"),
    ]


def _make_dt(day: int) -> AwareUTCDatetime:
    return datetime(2022, 1, day, tzinfo=timezone.utc)


def _make_animal(age: int, day: int) -> Animal:
    return Animal(
        name="fido",
        sound="woof",
        age=age,
        created=_make_dt(day),
    )


def test_iter_is_sorted(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    index = Index.pick(Animal, lambda a: (a.age, a.created))
    assert index.k == tuple[int, AwareUTCDatetime]
    z = sqlite.ZSetSQLite(cur, Animal, "foo", (index,))
    z.create_data_table()

    z += ZSetPython(Animal, [(_make_animal(1, 1), 1)])
    z += ZSetPython(Animal, [(_make_animal(2, 2), 1)])
    z += ZSetPython(Animal, [(_make_animal(2, 1), 1)])
    _flush(z)
    z += ZSetPython(Animal, [(_make_animal(3, 2), 1)])
    z += ZSetPython(Animal, [(_make_animal(1, 2), 1)])
    z += ZSetPython(Animal, [(_make_animal(3, 1), 1)])

    actual = list(z.iter_by_index(index))
    expected = [
        ((1, _make_dt(1)), _make_animal(1, 1), 1),
        ((1, _make_dt(2)), _make_animal(1, 2), 1),
        ((2, _make_dt(1)), _make_animal(2, 1), 1),
        ((2, _make_dt(2)), _make_animal(2, 2), 1),
        ((3, _make_dt(1)), _make_animal(3, 1), 1),
        ((3, _make_dt(2)), _make_animal(3, 2), 1),
    ]
    assert actual == expected
