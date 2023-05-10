---
title: "Installation"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 102
toc: true
---

Installation should be as simple as:

```bash
pip install stepping
```

If you want to run the examples with `pytest`, I recommend using [testing.postgresql](https://github.com/tk0miya/testing.postgresql#usage) and setting up fixtures in your `conftest.py` similar to:

```python
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
```
