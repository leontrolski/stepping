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

_More to follow..._

## Benchmarks

- Running a pretty basic test (1 million reads, two joins, group by date), `stepping`'s per-iteration insert time is slow, but querying the integrated data set takes `0.0003s` as opposed to `0.5s`. (Details in [test_profile.py](https://github.com/leontrolski/stepping/blob/main/tests/run/test_profile.py)).

## Future Ideas

- Profile with loads of data, how do insert times grow over time?
- How does performance look with read replica(s)?
- Look into doing something like [pydantic-core](https://github.com/pydantic/pydantic-core) and rewriting hot code in Rust. The main blocker for writing "ZSets in Rust" are probably:
  - Adding `.st_bytes`, `.st_hash`, `.st_identifier` to `ZSetPython` and `Pair`. Can we do some crazy hashing with bitmaps or something where we update the hash as we increment/decrement counts?
  - Should the underlying `immutabledict[K, T]` _also_ be an immutable btree.
  - Some deep thought required with index ordering of `int`s, `float`s datetime (see `_btree.py`). Maybe we just split `K`s between those that can be ordered lexicographically and those that can't.
- Build some entirely different storage layer, eg. using something new and trendy like [sled](https://github.com/spacejam/sled), or less trendy, like `lmdb`.
- How quick is it in pypy?
- Use a different SQLite adaptor, eg. [apsw](https://rogerbinns.github.io/apsw/pysqlite.html), or maybe even wrap a rust SQLite library.
