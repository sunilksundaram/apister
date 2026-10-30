"""Topic parsing and a wildcard subscription trie.

Patterns follow MQTT rules, with ``/`` as the separator:

* ``+`` matches exactly one segment
* ``#`` matches the rest of the topic, including zero segments, and must be last
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from apister.types import InvalidTopic

if TYPE_CHECKING:
    from apister.bus import Subscription

_MAX_TOPIC_BYTES = 1024


def parse_topic(topic: str, *, pattern: bool) -> tuple[str, ...]:
    """Split a topic or pattern into segments, rejecting malformed input."""
    if not isinstance(topic, str) or not topic:
        raise InvalidTopic("topic must be a non-empty string")
    if len(topic.encode("utf-8")) > _MAX_TOPIC_BYTES:
        raise InvalidTopic(f"topic exceeds {_MAX_TOPIC_BYTES} bytes")
    if topic.startswith("/") or topic.endswith("/") or "//" in topic:
        raise InvalidTopic("topic must not have empty segments")
    parts = tuple(topic.split("/"))
    last = len(parts) - 1
    for index, part in enumerate(parts):
        if part == "#":
            if not pattern or index != last:
                raise InvalidTopic("'#' is only valid as the final pattern segment")
            continue
        if part == "+":
            if not pattern:
                raise InvalidTopic("'+' is only valid in subscription patterns")
            continue
        if "+" in part or "#" in part:
            raise InvalidTopic("wildcards must occupy a whole segment")
    return parts


def topic_matches(pattern: str, topic: str) -> bool:
    """Return whether ``topic`` matches an MQTT-style ``pattern``."""
    return _match(parse_topic(pattern, pattern=True), parse_topic(topic, pattern=False), 0, 0)


def _match(pattern: tuple[str, ...], topic: tuple[str, ...], i: int, j: int) -> bool:
    if i == len(pattern):
        return j == len(topic)
    segment = pattern[i]
    if segment == "#":
        return True
    if j == len(topic):
        return False
    if segment == "+" or segment == topic[j]:
        return _match(pattern, topic, i + 1, j + 1)
    return False


class _Node:
    __slots__ = ("children", "subscribers")

    def __init__(self) -> None:
        self.children: dict[str, _Node] = {}
        self.subscribers: set[Subscription] = set()


class TopicTrie:
    """Subscription index. Match cost follows topic depth, not subscriber count."""

    def __init__(self) -> None:
        self._root = _Node()

    def add(self, pattern: str, subscription: Subscription) -> None:
        node = self._root
        for segment in parse_topic(pattern, pattern=True):
            child = node.children.get(segment)
            if child is None:
                child = _Node()
                node.children[segment] = child
            node = child
        node.subscribers.add(subscription)

    def discard(self, pattern: str, subscription: Subscription) -> None:
        parts = parse_topic(pattern, pattern=True)
        nodes = [self._root]
        node = self._root
        for segment in parts:
            child = node.children.get(segment)
            if child is None:
                return
            nodes.append(child)
            node = child
        node.subscribers.discard(subscription)
        for index in range(len(parts), 0, -1):
            parent = nodes[index - 1]
            child = nodes[index]
            segment = parts[index - 1]
            if child.subscribers or child.children:
                break
            del parent.children[segment]

    def match(self, topic: str) -> list[Subscription]:
        found: list[Subscription] = []
        segments = parse_topic(topic, pattern=False)

        def walk(node: _Node, index: int) -> None:
            if index == len(segments):
                found.extend(node.subscribers)
                rest = node.children.get("#")
                if rest is not None:
                    found.extend(rest.subscribers)
                return
            exact = node.children.get(segments[index])
            if exact is not None:
                walk(exact, index + 1)
            plus = node.children.get("+")
            if plus is not None:
                walk(plus, index + 1)
            rest = node.children.get("#")
            if rest is not None:
                found.extend(rest.subscribers)

        walk(self._root, 0)
        return found

    def __len__(self) -> int:
        total = 0
        stack = [self._root]
        while stack:
            node = stack.pop()
            total += len(node.subscribers)
            stack.extend(node.children.values())
        return total
