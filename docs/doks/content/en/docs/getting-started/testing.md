---
title: "Testing"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 105
toc: true
---

If you want to run test queries against a Postgres db with `pytest`, I recommend using [testing.postgresql](https://github.com/tk0miya/testing.postgresql#usage) (this requires having Postgres' `initdb` in your PATH) and setting up fixtures in your `conftest.py` similar to:

```python [/docs/snippets/conftest.py::fixtures]
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
```

Example usage can be seen in the `stepping` [tests](https://github.com/leontrolski/stepping/blob/main/tests/run).
