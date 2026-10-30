import unittest

from apister.topics import TopicTrie, parse_topic, topic_matches
from apister.types import InvalidTopic


class ParseTests(unittest.TestCase):
    def test_rejects_empty_and_wildcards_on_publish(self) -> None:
        with self.assertRaises(InvalidTopic):
            parse_topic("", pattern=False)
        with self.assertRaises(InvalidTopic):
            parse_topic("news/+", pattern=False)
        with self.assertRaises(InvalidTopic):
            parse_topic("news/#/late", pattern=True)
        with self.assertRaises(InvalidTopic):
            parse_topic("/news", pattern=False)

    def test_hash_matches_zero_or_more_segments(self) -> None:
        self.assertTrue(topic_matches("news/#", "news"))
        self.assertTrue(topic_matches("news/#", "news/sports/final"))
        self.assertFalse(topic_matches("news/+/final", "news/sports/extra/final"))
        self.assertTrue(topic_matches("news/+/final", "news/sports/final"))
        self.assertTrue(topic_matches("#", "anything/at/all"))


class TrieTests(unittest.TestCase):
    def test_match_and_discard(self) -> None:
        trie = TopicTrie()
        exact = _Sub("news/sports/final")
        wild = _Sub("news/#")
        plus = _Sub("news/+/final")
        trie.add(exact.pattern, exact)  # type: ignore[arg-type]
        trie.add(wild.pattern, wild)  # type: ignore[arg-type]
        trie.add(plus.pattern, plus)  # type: ignore[arg-type]
        matched = trie.match("news/sports/final")
        self.assertEqual({id(item) for item in matched}, {id(exact), id(wild), id(plus)})
        trie.discard(plus.pattern, plus)  # type: ignore[arg-type]
        self.assertNotIn(plus, trie.match("news/sports/final"))
        self.assertEqual(len(trie), 2)


class _Sub:
    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
