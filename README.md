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

- Docs.
- Work out how to parallelize.
- Write everything up, email the dbsp people from the original paper. In particular, ask question about why the incremental recursive stuff is different from the paper.
- See suggestions in `performance.md`.
- Look at 11.8 "Window aggregates"
- Replace `integrate_delay` with a nice transform. Similarly, transform shared delays.
- Test arbitrary depth grouped nesting and joining in a grouped setting (Does this _need_ doing?).
- Replace `annotate_zset` with `__get_pydantic_core_schema__`.
- Revisit `st.compile(...)`

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
