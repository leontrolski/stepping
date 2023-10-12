import inspect
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Iterator

TOTALS: dict[str, float] = defaultdict(float)
STACKS: dict[str, int] = defaultdict(int)
STACK_TOTALS: dict[str, float] = defaultdict(float)


@contextmanager
def timeit(name: str) -> Iterator[None]:
    before = time.time()
    yield
    TOTALS[name] += time.time() - before


def pp_totals() -> None:
    for k, v in sorted(TOTALS.items(), key=lambda kv: kv[1]):
        print(v, k)


@contextmanager
def register_called_from() -> Iterator[None]:
    stacks = [
        f"{n.filename.partition('/src/stepping/')[2]}:{n.positions.lineno}"  # type: ignore[union-attr]
        for n in inspect.stack()
    ]
    stack = " ".join(reversed([n for n in stacks[2:] if not n.startswith("venv/")]))
    STACKS[stack] += 1

    before = time.time()
    yield
    STACK_TOTALS[stack] += time.time() - before


def pp_stacks() -> None:
    for stack, count in sorted(STACKS.items(), key=lambda kv: STACK_TOTALS[kv[0]]):
        print(count, f"{STACK_TOTALS[stack]:.2f}s", stack)
        print()
