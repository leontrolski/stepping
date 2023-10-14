---
title: "Performance"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "in-production"
weight: 604
toc: true
---

In general, it's worth benchmarking before commiting to `stepping`, see for example: [test_profile.py](https://github.com/leontrolski/stepping/blob/main/tests/run/test_profile.py).

Insert speeds are far slower than plain SQL, but once you cross a million rows or so, retrieving data with `JOIN`s and `GROUP BY`s can become orders of magnitude quicker. When weighing up, also consider the developer experience, you have the whole of Python to play with, not just SQL. Insert time should be linear (give or take some writing to indexes).

## Rough Benchmarks

All on my M1 Macbook Air.

- For a fairly standard query (with a couple of joins, a group by, an integrate into a cache, 6 delay vertices), we can write **10,000 rows per second** to Postgres and **15,000 rows per second** to SQLite.
- For a more complex query (with >10 delay vertices and a recursive operator), we can write **2000 rows per second** to SQLite, up to **4000 rows per second** if we enable (transactionally dangerous) parallelism. Writing 100,000 rows (fairly small rows, but with indexes, nested `ZSet`s etc.) takes up **120MB** of space.
- Retrieving data from an indexed cache should take in the low ms.


## Future

- _Profile with loads of data, how do insert times grow over time?_
- _How does performance look with read replica(s)?_
- _Look into doing something like [pydantic-core](https://github.com/pydantic/pydantic-core) and rewriting hot code in Rust. The main blocker for writing "ZSets in Rust" are probably:_
  - _Adding `.st_bytes`, `.st_hash`, `.st_identifier` to `ZSetPython` and `Pair`. Can we do some crazy hashing with bitmaps or something where we update the hash as we increment/decrement counts?_
  - _Should the underlying `immutabledict[K, T]` _also_ be an immutable btree._
  - _Some deep thought required with index ordering of `int`s, `float`s datetime (see `_btree.py`). Maybe we just split `K`s between those that can be ordered lexicographically and those that can't._
- _Build some entirely different storage layer, eg. using something new and trendy like [sled](https://github.com/spacejam/sled), or less trendy, like `lmdb`._
- _How quick is it in pypy?_
- _Use a different SQLite adaptor, eg. [apsw](https://rogerbinns.github.io/apsw/pysqlite.html), or maybe even wrap a rust SQLite library._
