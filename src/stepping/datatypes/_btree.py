# This is a version of https://gist.github.com/natekupp/1763661 without
# using mutation and with some other simplifications.
from __future__ import annotations

from typing import Final, Generic, Iterator

from stepping.types import K, MatchAll, TSerializable

MAX_KEYS: Final = 15
J: Final = MAX_KEYS // 2  # the index of the middle element


class Node(Generic[TSerializable, K]):
    __slots__ = ("self", "keys", "children")

    def __init__(
        self,
        keys: tuple[tuple[TSerializable, K], ...],
        children: tuple[Node[TSerializable, K], ...],
    ) -> None:
        self.keys: tuple[tuple[TSerializable, K], ...] = keys
        self.children: tuple[Node[TSerializable, K], ...] = children


def _lt_atom(key_a: K, key_b: K) -> bool:
    if key_a is None and key_b is None:
        return True
    if key_a is None:
        return True
    if key_b is None:
        return False
    return key_a < key_b  # type: ignore


def lt(key_a: K, key_b: K, ascending: tuple[bool, ...]) -> bool:
    if key_a == key_b:
        return False

    if isinstance(key_a, tuple):
        assert isinstance(key_b, tuple)
        assert isinstance(ascending, tuple)
        assert len(key_a) == len(key_b)
        for inner_a, inner_b, inner_ascending in zip(key_a, key_b, ascending):
            if inner_a == inner_b:
                continue
            return _lt_atom(inner_a, inner_b) ^ (not inner_ascending)
        return False

    (asc,) = ascending
    return _lt_atom(key_a, key_b) ^ (not asc)


def _gt(key_a: K, key_b: K, ascending: tuple[bool, ...]) -> bool:
    return key_a != key_b and not lt(key_a, key_b, ascending)


def _split(node: Node[TSerializable, K], i: int) -> Node[TSerializable, K]:
    child = node.children[i]
    keys_before, key, keys_after = child.keys[:J], child.keys[J], child.keys[J + 1 :]
    children_before, children_after = child.children[: J + 1], child.children[J + 1 :]

    return Node(
        node.keys[:i] + (key,) + node.keys[i:],
        node.children[:i]
        + (
            Node(keys_before, children_before),
            Node(keys_after, children_after),
        )
        + node.children[i + 1 :],
    )


def add(
    node: Node[TSerializable, K],
    value: TSerializable,
    key: K,
    ascending: tuple[bool, ...],
) -> Node[TSerializable, K]:
    if len(node.keys) == MAX_KEYS:
        node = _split(Node((), (node,)), 0)
    return _insert(node, value, key, ascending)


def _find_i(node: Node[TSerializable, K], key: K, ascending: tuple[bool, ...]) -> int:
    i = len(node.keys)
    for inner_i, [_, inner_key] in enumerate(node.keys):
        if lt(key, inner_key, ascending):
            i = inner_i
            break
    return i


def _insert(
    node: Node[TSerializable, K],
    value: TSerializable,
    key: K,
    ascending: tuple[bool, ...],
) -> Node[TSerializable, K]:
    i = _find_i(node, key, ascending)

    if not node.children:  # i.e. is a leaf
        return Node(node.keys[:i] + ((value, key),) + node.keys[i:], node.children)

    if len(node.children[i].keys) == MAX_KEYS:
        node = _split(node, i)
        _, inner_key = node.keys[i]
        i = i + 1 if _gt(key, inner_key, ascending) else i

    new_child = _insert(node.children[i], value, key, ascending)
    return Node(node.keys, node.children[:i] + (new_child,) + node.children[i + 1 :])


def yield_sorted_matching(
    node: Node[TSerializable, K], match_key: K | MatchAll, ascending: tuple[bool, ...]
) -> Iterator[TSerializable]:
    ma = isinstance(match_key, MatchAll)
    if not node.keys:
        return

    if not node.children:
        for [value, key] in node.keys:
            if ma or key == match_key:
                yield value
        return

    for child, [value, key] in zip(node.children, node.keys):
        # if key < match_key, skip
        if not ma and lt(key, match_key, ascending):  # type: ignore
            continue

        yield from yield_sorted_matching(child, match_key, ascending)
        if ma or key == match_key:
            yield value

        # if key < match_key, return
        if not ma and _gt(key, match_key, ascending):  # type: ignore
            return

    yield from yield_sorted_matching(node.children[-1], match_key, ascending)
