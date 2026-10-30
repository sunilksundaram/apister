import unittest

from apister.brokers.patterns import (
    dotted_to_topic,
    to_amqp_routing,
    to_kafka_regex,
    to_redis_globs,
    topic_to_dotted,
)


class PatternTests(unittest.TestCase):
    def test_round_trip_escapes_dots(self) -> None:
        topic = "sports/ten.nis/score"
        self.assertEqual(dotted_to_topic(topic_to_dotted(topic)), topic)

    def test_amqp_and_redis_and_kafka(self) -> None:
        self.assertEqual(to_amqp_routing("sports/+/score"), "sports.*.score")
        self.assertEqual(to_amqp_routing("sports/#"), "sports.#")
        exact, globs = to_redis_globs("sports/#")
        self.assertEqual(exact, ("sports",))
        self.assertEqual(globs, ("sports/*",))
        regex = to_kafka_regex(["sports/+/score", "sports/#"])
        self.assertIn("[^.]+", regex)
        self.assertTrue(regex.startswith("^"))
