import pathlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

import icdiff
import pytest
import testing.postgresql
from prettyprinter import install_extras, pformat

import stepping as st
from stepping.zset.sql import generic, sqlite

install_extras(warn_on_error=False)

DB_URL = "postgresql://postgres@127.0.0.1:8421/test"


@pytest.fixture(scope="session")
def postgres_db() -> Iterator[str]:
    with testing.postgresql.Postgresql(port=8421) as postgresql:
        yield postgresql.url()


@pytest.fixture
def postgres_conn(postgres_db: None) -> Iterator[st.ConnPostgres]:
    with st.connection_postgres(DB_URL) as conn:
        yield conn
        with conn.transaction():
            clean_postgres_tables(conn)


@pytest.fixture()
def sqlite_conn() -> Iterator[generic.ConnSQLite]:
    p = pathlib.Path(__file__).parent / "stepping-test.db"
    with sqlite.connection(p) as conn:
        yield conn
    p.unlink()


@dataclass
class Conns:
    postgres: generic.ConnPostgres
    sqlite: generic.ConnSQLite


@pytest.fixture()
def conns(
    postgres_conn: generic.ConnPostgres, sqlite_conn: generic.ConnSQLite
) -> Iterator[Conns]:
    yield Conns(postgres_conn, sqlite_conn)


def clean_postgres_tables(conn: generic.ConnPostgres) -> None:
    qry = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """
    for (table_name,) in conn.execute(qry):
        conn.execute(f"DROP TABLE {table_name}")
        # conn.execute(f"DELETE FROM {table_name}")

    qry = """
        SELECT sequencename FROM pg_sequences
        WHERE schemaname IN (SELECT current_schema())
    """
    for (sequence,) in conn.execute(qry):
        conn.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--n-profile",
        action="store",
        default="10",
        help="number of iterations to profile",
    )
    parser.addoption("--write-graphs", help="write graphs to png", action="store_true")


def pytest_assertrepr_compare(
    config: Any, op: Any, left: Any, right: Any
) -> list[str] | None:
    return pretty_compare(config, op, left, right)


def pretty_compare(config: Any, op: str, left: Any, right: Any) -> list[str] | None:
    """Heavily influenced by https://github.com/hjwp/pytest-icdiff."""
    very_verbose = config.option.verbose >= 2
    if not very_verbose:
        return None

    if op != "==":
        return None

    try:
        if abs(left + right) < 100:
            return None
    except TypeError:
        pass

    try:
        if hasattr(left, "pformat") and hasattr(right, "pformat"):
            pretty_left = left.pformat().splitlines()
            pretty_right = right.pformat().splitlines()
        else:
            pretty_left = pformat(
                left, indent=4, width=79, sort_dict_keys=True
            ).splitlines()
            pretty_right = pformat(
                right, indent=4, width=79, sort_dict_keys=True
            ).splitlines()
        differ = icdiff.ConsoleDiff(cols=160, tabsize=4)
        icdiff_lines = list(differ.make_table(pretty_left, pretty_right, context=False))

        return (
            ["equals failed"]
            + ["<left>".center(79) + "|" + "<right>".center(80)]
            + ["-" * 160]
            + [icdiff.color_codes["none"] + l for l in icdiff_lines]
        )
    except Exception as e:
        return None
