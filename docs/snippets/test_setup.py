import pytest
import psycopg

def test_foo(conn: psycopg.Connection) -> int:
    rows = conn.execute("SELECT 1")
    assert list(rows) == [(1, )]
