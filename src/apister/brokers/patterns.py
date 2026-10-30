"""Map bus topics onto Redis, RabbitMQ, and Kafka naming rules.

Bus topics use ``/``. AMQP and Kafka use ``.`` as the separator, so ``.`` and
``~`` inside a segment are escaped before the topic leaves the process.
"""

from __future__ import annotations

import re


def topic_to_dotted(topic: str) -> str:
    return ".".join(_escape(part) for part in topic.split("/"))


def dotted_to_topic(dotted: str) -> str:
    if not dotted:
        return dotted
    return "/".join(_unescape(part) for part in dotted.split("."))


def to_amqp_routing(pattern: str) -> str:
    """MQTT-style pattern to an AMQP topic-exchange binding key."""
    return ".".join(_amqp_token(part) for part in pattern.split("/"))


def to_redis_globs(pattern: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return ``(exact_channels, psubscribe_globs)`` for a bus pattern.

    Redis globs are a superset of MQTT wildcards. The bus filters again locally.
    """
    parts = pattern.split("/")
    exact: list[str] = []
    globs: list[str] = []
    if parts[-1] == "#":
        prefix = parts[:-1]
        if not prefix:
            return (), ("*",)
        exact.append("/".join(prefix))
        globs.append("/".join([*prefix, "*"]))
        return tuple(exact), tuple(globs)
    globs.append("/".join("*" if part == "+" else _redis_escape(part) for part in parts))
    return (), tuple(globs)


def to_kafka_regex(patterns: list[str]) -> str:
    """One consumer regex covering every bus pattern."""
    if not patterns:
        return r"^$"
    bodies = [_kafka_body(pattern) for pattern in patterns]
    return rf"^(?:{'|'.join(bodies)})$"


def _amqp_token(part: str) -> str:
    if part == "+":
        return "*"
    if part == "#":
        return "#"
    return _escape(part)


def _kafka_body(pattern: str) -> str:
    parts = pattern.split("/")
    hash_tail = parts[-1] == "#"
    if hash_tail:
        parts = parts[:-1]
    if not parts:
        return r".*"
    tokens = ["[^.]+" if part == "+" else re.escape(_escape(part)) for part in parts]
    body = r"\.".join(tokens)
    if hash_tail:
        return rf"{body}(?:\..*)?"
    return body


def _escape(segment: str) -> str:
    return segment.replace("~", "~0").replace(".", "~1")


def _unescape(segment: str) -> str:
    return segment.replace("~1", ".").replace("~0", "~")


def _redis_escape(segment: str) -> str:
    return segment.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?").replace("[", "\\[")
