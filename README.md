# `stepping`

Based on the paper: [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://github.com/vmware/database-stream-processor/blob/e6cdbb538bbce8adb90018ff75f8ae8251b3e206/doc/theory/main.pdf).

# [📚 Documentation 📚](https://stepping.site)

## Installation

```bash
pip install stepping
```

### Development installation

```bash
git clone git@github.com:leontrolski/stepping.git
python -m venv .env
source .env/bin/activate
pip install -e '.[dev]'
pytest
mypy src tests
```

## Todos

- See suggestions in `performance.md`.
    - Can we `SELECT value FROM json_array_elements_text(...)` with bytes.
        - Special class for compound indexes.
    - Make SQL ZSets work with JSON bytes.
    - Can `pick_index` become simpler/get removed, can we remove `WithLen` and friends?
    - With `steppingpack` does `TIndexable`/`K` become meaningless.
    - Can SQLite and postgres actually index bytes? Can we save space only putting the data in the index.
- Write up parallelize docs.
    - Talk a lot about early commit mode and pitfalls.
    - `time` as a `SEQUENCE?` - would come as part of stepping manager.
    - Future? Need to think hard about how one could allow parallelism where you can guaranteee no phantom rows.
- Make the graph a simple introspectable thing.
    - Use paths as names.
    - Make the Path contain the module names.
    - Make vertexes immutable, run `mypy`, fix.
    - Remember to update docs.
- Replace operator kind with `normal`.
- Instead of `create_tables=True` can we emit SQL and write it.
- In `iter_by_index`, use indexed `ZSetPython`.
- Can `run.iteration` make nicer error messages?
- Implement `isjsonschemasubset` for msgpack.
- Write everything up, email the dbsp people from the original paper. In particular, ask question about why the incremental recursive stuff is different from the paper.
- Python 3.12. Use built in `batched`. Can we use the new `Unpack` syntax for nicer action types?
- Look at 11.8 "Window aggregates"
- Replace `integrate_delay` with a nice transform. Similarly, transform shared delays.
- Test arbitrary depth grouped nesting and joining in a grouped setting (Does this _need_ doing?).
- Replace `annotate_zset` with `__get_pydantic_core_schema__`.
- Revisit `st.compile(...)`.
- Decide whether to make `...SQL` classes protocols.

# Uploading to Pypi

```bash
# bump version
python -m pip install build twine
python -m build
twine check dist/*
twine upload dist/*
```

# Deploy docs

```bash
flyctl launch
flyctl deploy
flyctl ips list -a stepping-docs
# set A record to @, IPv4
# set AAAA record to @, IPv6
flyctl certs create -a stepping-docs stepping.site

cd docs/doks
npm install
npm run start
npm run build
cd ..; flyctl deploy; cd -

cd docs
python ../scripts/md.py ../ $(find -L ../docs-md -name '**.md')
```
