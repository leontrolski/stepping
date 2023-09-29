from __future__ import annotations

import pathlib
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pytest

from stepping.types import Data, SerializableObject, ZSet, pick_identity, pick_index
from stepping.zset import functions
from stepping.zset.python import ZSetPython
from stepping.zset.sql import generic, sqlite


def dump_schema(conn: generic.ConnSQLite) -> str:
    db_url = conn.execute("PRAGMA database_list").fetchone()[2]
    return subprocess.check_output(f"sqlite3 {db_url} .schema", shell=True).decode()


def test_create_table(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    ix = pick_identity(int)
    sqlite.ZSetSQLite[int](cur, int, generic.Table("foo"), (ix,))


class Animal(Data):
    name: str
    sound: str
    age: int
    created: datetime


def test_typing(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()

    animal: SerializableObject = Animal(
        name="fido",
        sound="woof",
        age=38,
        created=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    z: ZSet[int] = sqlite.ZSetSQLite[int](cur, int, generic.Table("foo"), ())


def test_write_simple_int(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, int, generic.Table("foo"), ())
    z.create_data_table()
    changes = ZSetPython({42: 1, 56: 2, 78: -1})
    z += changes

    actual = list(z.iter(frozenset((42, 78))))
    assert actual == [(42, 1), (78, -1)]

    z.flush_changes()
    assert z.to_python() == changes

    actual = list(z.iter(frozenset((42, 78))))
    assert actual == [(42, 1), (78, -1)]


def test_write_simple_int_with_index(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, int, generic.Table("foo"), (pick_identity(int),))
    z.create_data_table()
    changes = ZSetPython({42: 1, 56: 2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)
    ix_str = "CREATE INDEX ix__foo__identity ON foo(CAST(data AS INTEGER))"
    assert ix_str in schema


def test_write_simple_date(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, date, generic.Table("foo"), ())
    z.create_data_table()
    changes = ZSetPython({date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes


def test_write_simple_date_with_index(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(
        cur,
        date,
        generic.Table("foo"),
        (pick_identity(date),),
    )
    z.create_data_table()
    changes = ZSetPython({date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)
    ix_str = "CREATE INDEX ix__foo__identity ON foo(CAST(data AS TEXT))"
    assert ix_str in schema


def test_write_complex(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, Animal, generic.Table("foo"), ())
    z.create_data_table()

    animal = Animal(
        name="fido",
        sound="woof",
        age=38,
        created=datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    changes = ZSetPython({animal: 2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes


def test_write_complex_update(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    z = sqlite.ZSetSQLite(cur, Animal, generic.Table("foo"), ())
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

    changes = ZSetPython({animal: 1})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    changes = ZSetPython({animal: -1, animal_new: 1})
    z += changes
    z.flush_changes()

    expected = ZSetPython({animal_new: 1})
    assert z.to_python() == expected


class Bar(Data):
    bingo: str


class Foo(Data):
    name: str | None
    created: date
    age: int
    parent: Bar


def test_schema_made_and_used(sqlite_conn: generic.ConnSQLite) -> None:
    cur = sqlite_conn.cursor()
    ix_namely = pick_index(Foo, lambda f: f.name)
    ix_name_and_age = pick_index(Foo, lambda f: (f.name, f.age))
    ix_parent_bingo = pick_index(
        Foo, lambda f: (f.name, f.parent.bingo), ascending=(False, True)
    )
    ix_created = pick_index(Foo, lambda f: f.created)

    z = sqlite.ZSetSQLite(
        cur,
        Foo,
        generic.Table("foo"),
        (ix_namely, ix_name_and_age, ix_parent_bingo, ix_created),
    )
    z.create_data_table()

    first_foo = Foo(
        name="a-name", created=date(2023, 1, 4), age=23, parent=Bar(bingo="asd")
    )
    changes = ZSetPython(
        {first_foo: 1}
        | {
            Foo(
                name=f"some-name-{i % 3}",
                created=date(2023, 1, 20),
                age=i % 10,
                parent=Bar(bingo=f"pi-{i}"),
            ): 2
            for i in range(33)
        }
    )
    z += changes
    z.flush_changes()

    actual = list(z.iter(frozenset((first_foo,))))
    assert actual == [(first_foo, 1)]

    sqlite_conn.commit()
    schema = dump_schema(sqlite_conn)
    ix_str = "CREATE INDEX ix__foo__name ON foo(CAST((data ->> '$.name') AS TEXT))"
    assert ix_str in schema
    ix_str = "CREATE INDEX ix__foo__name__age ON foo(CAST((data ->> '$.name') AS TEXT), CAST((data ->> '$.age') AS INTEGER))"
    assert ix_str in schema
    ix_str = "CREATE INDEX ix__foo__name__parent_bingo ON foo(CAST((data ->> '$.name') AS TEXT) DESC, CAST((data ->> '$.parent.bingo') AS TEXT))"
    assert ix_str in schema

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


def _make_dt(day: int) -> datetime:
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
    index = pick_index(Animal, lambda a: (a.age, a.created))
    z = sqlite.ZSetSQLite(cur, Animal, generic.Table("foo"), (index,))
    z.create_data_table()

    z += ZSetPython({_make_animal(1, 1): 1})
    z += ZSetPython({_make_animal(2, 2): 1})
    z += ZSetPython({_make_animal(2, 1): 1})
    z.flush_changes()
    z += ZSetPython({_make_animal(3, 2): 1})
    z += ZSetPython({_make_animal(1, 2): 1})
    z += ZSetPython({_make_animal(3, 1): 1})

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
