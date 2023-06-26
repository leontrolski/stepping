from typing import Any, Iterator

import icdiff
import pytest
import testing.postgresql
from _pytest.monkeypatch import MonkeyPatch
from prettyprinter import install_extras, pformat

from stepping import config, operators
from stepping.zset import postgres

install_extras(warn_on_error=False)


@pytest.fixture(autouse=True)
def reset_operator_i() -> Iterator[None]:
    yield
    operators.reset_vertex_counter()


@pytest.fixture(scope="session")
def dummy_config() -> Iterator[None]:
    # class scoped monkeypatch
    mpatch = MonkeyPatch()
    mpatch.setattr(
        config,
        "get_config",
        lambda: config.Config(
            DB_URL="postgresql://postgres@127.0.0.1:8421/test",
        ),
    )
    yield
    mpatch.undo()


@pytest.fixture(scope="session")
def db(dummy_config: None) -> Iterator[str]:
    with testing.postgresql.Postgresql(port=8421) as postgresql:
        yield postgresql.url()


@pytest.fixture
def conn(db: None) -> Iterator[postgres.Conn]:
    with postgres.connection() as conn:
        yield conn
        with conn.transaction():
            clean_tables(conn)


def clean_tables(conn: postgres.Conn) -> None:
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
