import pytest
from typing import Iterator
import testing.postgresql
import psycopg


# reference: fixtures
@pytest.fixture(scope="session")
def db() -> Iterator[str]:
    with testing.postgresql.Postgresql(port=8421) as postgresql:
        yield postgresql.url()


@pytest.fixture
def conn(db: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(db) as conn:
        yield conn
        clean_tables(conn)


def clean_tables(conn: psycopg.Connection) -> None:
    qry = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """
    for (table_name,) in conn.execute(qry):
        conn.execute(f"DROP TABLE {table_name}")

    qry = """
        SELECT sequencename FROM pg_sequences
        WHERE schemaname IN (SELECT current_schema())
    """
    for (sequence,) in conn.execute(qry):
        conn.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")
# /reference: fixtures
