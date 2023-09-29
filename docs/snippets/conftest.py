# reference: fixtures
import pytest
from typing import Iterator
import testing.postgresql

import stepping as st


@pytest.fixture(scope="session")
def postgres_db() -> Iterator[str]:
    with testing.postgresql.Postgresql(port=8421) as postgresql:
        yield postgresql.url()


@pytest.fixture
def postgres_conn(postgres_db: str) -> Iterator[st.ConnPostgres]:
    with st.connection_postgres(postgres_db) as conn:
        yield conn
        with conn.transaction():
            clean_postgres_tables(conn)


def clean_postgres_tables(conn: st.ConnPostgres) -> None:
    conn.execute(f"DROP SCHEMA public CASCADE")
    conn.execute(f"CREATE SCHEMA public")
# /reference: fixtures


from typing import Any


def pytest_addoption(parser: Any) -> None:
    try:
        parser.addoption(
            "--n-profile",
            action="store",
            default="10",
            help="number of iterations to profile",
        )
        parser.addoption(
            "--write-graphs", help="write graphs to png", action="store_true"
        )
    except:
        pass
