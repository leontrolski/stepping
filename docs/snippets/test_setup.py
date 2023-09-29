import psycopg

def test_foo(postgres_conn: psycopg.Connection) -> None:  # type: ignore
    rows = postgres_conn.execute("SELECT 1")
    assert list(rows) == [(1, )]
