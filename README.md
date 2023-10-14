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

## Nice-to-haves

- In `interleave_changes`, use indexed `ZSetPython` under the hood.
- Skip out the middle man with `Grouped` and just use an indexed `ZSet`? Does this lead to performance benefits in `st.group`?
- Instead of `create_tables=True` can we emit SQL and write it.
- Revisit `st.compile(...)`.
- Can `run.iteration` make nicer error messages?
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
