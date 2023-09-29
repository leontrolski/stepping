from itertools import product
from typing import Iterator


def y(s: str, l: int = 2) -> Iterator[list[str]]:
    for n in range(1, l + 1):
        yield [f"{s}{m}" for m in range(1, n + 1)]


def a(ns: list[str]) -> str:
    return f"A{len(ns)}[{', '.join(ns)}]"


print()
for ts, us, xs, ys in product(y("T"), y("U"), y("X"), y("Y")):
    print("@overload")
    print(
        f"def stack(a: Graph[{a(ts)}, {a(us)}], b: Graph[{a(xs)}, {a(ys)}]) -> Graph[{a(ts + xs)}, {a(us + ys)}]:  ..."
    )


# for run.py


def a2(ns: list[str]) -> str:
    ns_str = ", ".join(f"{n}" for n in ns)
    return f"A{len(ns)}[{ns_str}]"


def tuple_(ns: list[str]) -> str:
    ns_str = ", ".join(f"{n}" for n in ns)
    return f"tuple[{ns_str}]"


print()
for ts, us in product(y("T", 4), y("U", 4)):
    print("@overload")
    print(
        f"def iteration(store: Store, g: Graph[{a2(ts)}, {a2(us)}], inputs: {tuple_(ts)}) -> {tuple_(us)}: ..."
    )


def az(ns: list[str]) -> str:
    ns_str = ", ".join(f"ZSet[{n}]" for n in ns)
    return f"A{len(ns)}[{ns_str}]"


def callable_(ts: list[str], us: list[str]) -> str:
    callables_str = ", ".join(f"Action[{t}, tuple[{', '.join(us)}]]" for t in ts)
    return f"tuple[{callables_str}]"


print()
for ts, us in product(y("T", 4), y("U", 4)):
    print("@overload")
    print(
        f"def actions(store: Store, g: Graph[{az(ts)}, {a2(us)}]) -> {callable_(ts, us)}: ..."
    )


def az2(ns: list[str]) -> str:
    return ", ".join(f"ZSet[{n}]" for n in ns)

def az2_tuple(ns: list[str]) -> str:
    if len(ns) > 1:
        foo = ", ".join(f"ZSet[{n}]" for n in ns)
        return f"tuple[{foo}]"
    return ", ".join(f"ZSet[{n}]" for n in ns)


print()
for ts, us in product(y("T", 4), y("U", 4)):
    print("@overload")
    print(
        f"def compile(func: Callable[[{az2(ts)}], {az2_tuple(us)}]) -> Graph[{az(ts)}, {az(us)}]: ..."
    )

# @overload
# def compile(func: Callable[[ZSet[T1]], ZSet[U1]]) -> Graph[A1[ZSet[T1]], A1[ZSet[U1]]]: ...
# @overload
# def compile(func: Callable[[ZSet[T1]], ZSet[U1]]) -> Graph[A1[ZSet[T1]], A1[ZSet[U1]]]: ...
# @overload
# def compile(func: Callable[[ZSet[T1], ZSet[T2]], ZSet[U1]]) -> Graph[A2[ZSet[T1], ZSet[T2]], A1[ZSet[U1]]]: ...
