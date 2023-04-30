# `stepping`

Based on the paper: [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://github.com/vmware/database-stream-processor/blob/e6cdbb538bbce8adb90018ff75f8ae8251b3e206/doc/theory/main.pdf).

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

# Internals

## Todos

### Ergonomics

- Think of a nice way to implement nested collections with efficient operations.
- Wrap `run.iteration` with further nice interface.
- Is there anything funky we can do like the `immerframe` lib.
- Add a commit timestamp for the tables for a future API.
- Write everything up, email the dbsp people from the original paper.

### Operator level

- Allow for (and test) arbitrary depth grouped nesting and joining in a grouped setting - is this necessary, or can it always just be achieved outside the group?
- Look at 11.8 "Window aggregates"
- Change `name: str` everywhere to be `provenance: Provenance`
- Change `transform.finalize` to be like `with freshly_numbered_vertices():` and namespace tables. (There is `reset_vertex_counter(...)` now if that helps).
- Simplify `haitch`.

### Types level

- `s/T/TSerializable/`
- Add a `maybe` function that allows for `pick_index(Left, maybe(left.a).foo)`.
- `s/[T, K]/[K, T]/` everywhere.

## Performance

- Running a pretty basic test (1 million reads, two joins, group by date), stepping's insert time is a lot slower, but querying the integrated data set takes `0.0003s` as opposed to `0.5s`. (Details in `test_profile_cute.py`).
