from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pytest

from stepping import config
from stepping.types import Data, SerializableObject, ZSet, pick_identity, pick_index
from stepping.zset import iter_by_index_grouped, postgres
from stepping.zset.python import ZSetPython


def dump_schema() -> str:
    return subprocess.check_output(
        f"pg_dump {config.get_config().DB_URL} --schema-only", shell=True
    ).decode()


def test_create_table(conn: postgres.Conn) -> None:
    ix = pick_identity(int)
    z = postgres.ZSetPostgres[int](conn, int, postgres.Table("foo"), (ix,))
    with conn.transaction():
        postgres.create_data_table(z)


@dataclass(frozen=True)
class Animal(Data):
    def identity(self) -> str:
        return self.name

    name: str
    sound: str
    age: int
    created: datetime


def test_typing(conn: postgres.Conn) -> None:
    animal: SerializableObject = Animal(
        "fido", "woof", 38, datetime(2022, 1, 1, tzinfo=timezone.utc)
    )
    z: ZSet[int] = postgres.ZSetPostgres[int](conn, int, postgres.Table("foo"), ())


def test_write_simple_int(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, int, postgres.Table("foo"), ())
    postgres.create_data_table(z)
    changes = ZSetPython({42: 1, 56: 2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes


def test_write_simple_int_with_index(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, int, postgres.Table("foo"), (pick_identity(int),))
    postgres.create_data_table(z)
    changes = ZSetPython({42: 1, 56: 2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    conn.commit()
    schema = dump_schema()
    ix_str = (
        "CREATE INDEX ix__foo__identity ON public.foo USING btree (((data)::integer))"
    )
    assert ix_str in schema


def test_write_simple_date(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, date, postgres.Table("foo"), ())
    postgres.create_data_table(z)
    changes = ZSetPython({date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes


def test_write_simple_date_with_index(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(
        conn,
        date,
        postgres.Table("foo"),
        (pick_identity(date),),
    )
    postgres.create_data_table(z)
    changes = ZSetPython({date(2021, 1, 3): 1, date(2021, 1, 4): -2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    conn.commit()
    schema = dump_schema()
    ix_str = "CREATE INDEX ix__foo__identity ON public.foo USING btree (((data)::text))"
    assert ix_str in schema


def test_write_complex(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, Animal, postgres.Table("foo"), ())
    postgres.create_data_table(z)

    animal = Animal("fido", "woof", 38, datetime(2022, 1, 1, tzinfo=timezone.utc))
    changes = ZSetPython({animal: 2})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes


def test_write_complex_update(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, Animal, postgres.Table("foo"), ())
    postgres.create_data_table(z)

    animal = Animal("fido", "woof", 38, datetime(2022, 1, 1, tzinfo=timezone.utc))
    animal_new = Animal("fido", "woofff", 38, datetime(2022, 6, 1, tzinfo=timezone.utc))

    changes = ZSetPython({animal: 1})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    changes = ZSetPython({animal: -1, animal_new: 1})
    z += changes
    z.flush_changes()

    expected = ZSetPython({animal_new: 1})
    assert z.to_python() == expected


def test_write_complex_update_error(conn: postgres.Conn) -> None:
    z = postgres.ZSetPostgres(conn, Animal, postgres.Table("foo"), ())
    postgres.create_data_table(z)

    animal = Animal("fido", "woof", 38, datetime(2022, 1, 1, tzinfo=timezone.utc))
    animal_new = Animal("fido", "woofff", 38, datetime(2022, 6, 1, tzinfo=timezone.utc))

    changes = ZSetPython({animal: 1})
    z += changes
    z.flush_changes()
    assert z.to_python() == changes

    changes = ZSetPython({animal: -1, animal_new: 2})
    z += changes

    with pytest.raises(RuntimeError):
        z.flush_changes()


@dataclass(frozen=True)
class Bar(Data):
    bingo: str


@dataclass(frozen=True)
class Foo(Data):
    name: str | None
    created: date
    age: int
    parent: Bar


def test_schema_made_and_used(conn: postgres.Conn) -> None:
    ix_namely = pick_index(Foo, lambda f: f.name)
    ix_name_and_age = pick_index(Foo, lambda f: (f.name, f.age))
    ix_parent_bingo = pick_index(
        Foo, lambda f: (f.name, f.parent.bingo), ascending=(False, True)
    )
    ix_created = pick_index(Foo, lambda f: f.created)

    z = postgres.ZSetPostgres(
        conn,
        Foo,
        postgres.Table("foo"),
        (ix_namely, ix_name_and_age, ix_parent_bingo, ix_created),
    )
    postgres.create_data_table(z)

    changes = ZSetPython(
        {Foo(name="a-name", created=date(2023, 1, 4), age=23, parent=Bar("asd")): 1}
        | {
            Foo(
                name=f"some-name-{i % 3}",
                created=date(2023, 1, 20),
                age=i % 10,
                parent=Bar(f"pi-{i}"),
            ): 2
            for i in range(33)
        }
    )
    z += changes
    z.flush_changes()

    conn.commit()
    schema = dump_schema()
    ix_str = "CREATE INDEX ix__foo__name ON public.foo USING btree (((data #>> '{name}'::text[])))"
    assert ix_str in schema
    ix_str = "CREATE INDEX ix__foo__name__age ON public.foo USING btree (((data #>> '{name}'::text[])), (((data #>> '{age}'::text[]))::integer))"
    assert ix_str in schema
    ix_str = "CREATE INDEX ix__foo__name__parent_bingo ON public.foo USING btree (((data #>> '{name}'::text[])) DESC, ((data #>> '{parent,bingo}'::text[])))"
    assert ix_str in schema

    plan = postgres.explain(
        conn,
        "SELECT * FROM foo WHERE (data #>> '{name}') = 'some-name' AND (data #>> '{age}')::integer = 3",
    )
    assert "Index Scan" in plan

    plan = postgres.explain(conn, "SELECT * FROM foo ORDER BY (data #>> '{name}')")
    assert "Index Scan" in plan

    postgres.MAKE_TEST_ASSERTIONS = True
    z_generic: ZSet[Foo] = z

    all_keys = [k for k, _ in iter_by_index_grouped(z_generic, ix_created)]
    assert all_keys == [date(2023, 1, 4), date(2023, 1, 20)]

    first_keys = [k for k, _ in iter_by_index_grouped(z_generic, ix_name_and_age)][:5]
    assert first_keys == [
        ("a-name", 23),
        ("some-name-0", 0),
        ("some-name-0", 1),
        ("some-name-0", 2),
        ("some-name-0", 3),
    ]

    first_inner = [
        set(inner) for _, inner in iter_by_index_grouped(z_generic, ix_name_and_age)
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
        set(inner) for _, inner in iter_by_index_grouped(z_generic, ix_name_and_age)
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
        k for k, _ in iter_by_index_grouped(z_generic, ix_parent_bingo)
    ][:5]
    assert first_keys_bingo == [
        ("some-name-2", "pi-11"),
        ("some-name-2", "pi-14"),
        ("some-name-2", "pi-17"),
        ("some-name-2", "pi-2"),
        ("some-name-2", "pi-20"),
    ]
