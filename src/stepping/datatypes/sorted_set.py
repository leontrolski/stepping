from __future__ import annotations

from typing import Generic, Iterator

import immutables

from stepping.datatypes._btree import Node, add
from stepping.datatypes._btree import lt as lt
from stepping.datatypes._btree import yield_sorted_matching
from stepping.types import MATCH_ALL, Index, K, MatchAll, TSerializable


class SortedSet(Generic[TSerializable, K]):
    __slots__ = ("added", "removed", "btree", "index")

    def __init__(self, index: Index[TSerializable, K]) -> None:
        self.added = immutables.Map[TSerializable, None]()
        self.removed = immutables.Map[TSerializable, None]()
        self.btree = Node[TSerializable, K]((), ())
        self.index = index

    def add(self, other: TSerializable) -> SortedSet[TSerializable, K]:
        btree = self.btree
        added = self.added
        removed = self.removed

        if other not in self.added:
            btree = add(
                self.btree,
                other,
                self.index.f(other),
                self.index.ascending,
            )
            added = self.added.set(other, None)
        if other in self.removed:
            removed = self.removed.delete(other)

        out = SortedSet[TSerializable, K](self.index)
        out.added = added
        out.removed = removed
        out.btree = btree
        return out

    def remove(self, other: TSerializable) -> SortedSet[TSerializable, K]:
        out = SortedSet[TSerializable, K](self.index)
        out.added = self.added
        out.removed = self.removed.set(other, None)
        out.btree = self.btree
        return out

    def __iter__(self) -> Iterator[TSerializable]:
        for n in yield_sorted_matching(self.btree, MATCH_ALL, self.index.ascending):
            if n in self.added and n not in self.removed:
                yield n

    def iter_matching(
        self, match_keys: frozenset[K] | MatchAll
    ) -> Iterator[TSerializable]:
        if isinstance(match_keys, MatchAll):
            yield from self
            return
        # We sort the keys here so as to match Postgres' behaviour
        for s in sorted(SortableKey(k, self.index.ascending) for k in match_keys):
            for n in yield_sorted_matching(self.btree, s.key, self.index.ascending):
                if n in self.added and n not in self.removed:
                    yield n

    def __repr__(self) -> str:
        more_than_10 = " ..." if len(self.added) - len(self.removed) > 10 else ""
        inner = ", ".join(repr(n) for n, _ in zip(self, range(10)))
        return "{" + inner + more_than_10 + "}"


class SortableKey(Generic[K]):
    def __init__(self, key: K, ascending: tuple[bool, ...]) -> None:
        self.key = key
        self.ascending = ascending

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SortableKey):
            return False
        return self.key == other.key  # type: ignore

    def __lt__(self, other: SortableKey[K]) -> bool:
        return lt(self.key, other.key, self.ascending)


# def pp(node: Node[TSerializable, K], indent: int = 0) -> str:
#     return "\n".join(
#         "  " * indent + line
#         for line in [
#             f"keys: {node.keys} {'children:' if node.children else ''}",
#             *[f"  {pp(child, indent + 1)}" for child in node.children],
#         ]
#     )
