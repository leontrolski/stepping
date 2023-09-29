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
- **Parallelise inserts**, getting this right with transactional behaviour seems hard, where is the literature?
- Other serialization options.
  - Pickle.
  - Custom Postgres types like [sqlski](https://github.com/leontrolski/sqlski/blob/master/sqlski/composite.py).
  - Some JSON binary format (ideally with separate schema).
  - Store as JSON Arrays not Objects and use the schema for keys.
  - Compress the JSON. In SQLite, it is possible to do `conn.create_function("f", 1, f, deterministic=True)`, compression with `zstandard.ZstdCompressionDict(json.dumps(Resolved.model_json_schema()).encode())` compresses raw JSON by around half.
- Note that non-JSON options would require writing extra columns for indexing.
- Look into doing something like [pydantic-core](https://github.com/pydantic/pydantic-core) and rewriting hot code in Rust.
- Build some entirely different storage layer, eg. using something new and trendy like [sled](https://github.com/spacejam/sled).
