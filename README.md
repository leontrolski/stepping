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

- Make the graph a simple introspectable thing.
    - Use paths as names.
    - Make the Path contain the module names.
    - Make vertexes immutable, run `mypy`, fix.
    - Remember to update docs.
- Docs - Write up parallelize docs, update index docs (esp. API page), add `steppingpack` docs.
    - Talk a lot about early commit mode and pitfalls.
    - `time` as a `SEQUENCE?` - would come as part of stepping manager.
    - Write some more performance numbers.
    - Future? Need to think hard about how one could allow parallelism where you can guaranteee no phantom rows.
    - Quick example on the homepage? Something better to explain things.
    - Mention adjusting `MAX_SLEEP_SECS`.
    - Mention how failures impact the other commit mode.
    - Go through every page and make sure nothing's changed.
- Instead of `create_tables=True` can we emit SQL and write it.
- In `steppingpack`, use class name not class itself as key, or something, maybe index each of the schemas.
- Write everything up, email the dbsp people from the original paper. In particular, ask question about why the incremental recursive stuff is different from the paper.

## Nice-to-haves

- In `interleave_changes`, use indexed `ZSetPython` under the hood.
- Skip out the middle man with `Grouped` and just use an indexed `ZSet`? Does this lead to performance benefits in `st.group`?
- Revisit `st.compile(...)`.
- Can `run.iteration` make nicer error messages?
- Support some more types with `steppingpack`, notably `time`.
- Implement `isjsonschemasubset` for `steppingpack` as part of steppingmanager.
- See suggestions in `performance.md`.
- Python 3.12. Use built in `batched`. Can we use the new `Unpack` syntax for nicer action types?
- Look at 11.8 "Window aggregates"
- Replace `integrate_delay` with a nice transform. Similarly, transform shared delays.
- Decide whether to make `...SQL` classes protocols.
- Test arbitrary depth grouped nesting and joining in a grouped setting (Does this even make sense to do?).

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
