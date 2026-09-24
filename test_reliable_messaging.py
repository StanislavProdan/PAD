import unittest

from reliable_messaging import Broker, Consumer, ConsumerCrash, Producer


class ReliableMessagingTests(unittest.TestCase):
    def test_crash_after_effect_redelivers_without_duplicate_effect(self):
        effects = []
        broker = Broker(max_attempts=3)
        consumer = Consumer(broker, effects.append)
        Producer(broker).send("payment-authorized", message_id="m-1")

        with self.assertRaises(ConsumerCrash):
            consumer.process_one(crash_after_effect=True)

        self.assertEqual(effects, ["payment-authorized"])
        self.assertEqual(broker.in_flight_count, 1)

        broker.recover_unacked()
        consumer.process_until_empty()

        self.assertEqual(effects, ["payment-authorized"])
        self.assertEqual(broker.dead_letters, [])

    def test_messages_are_processed_in_fifo_order(self):
        effects = []
        broker = Broker()
        consumer = Consumer(broker, effects.append)
        producer = Producer(broker)
        producer.send("first")
        producer.send("second")
        producer.send("third")

        consumer.process_until_empty()

        self.assertEqual(effects, ["first", "second", "third"])

    def test_nack_retries_then_moves_message_to_dlq(self):
        broker = Broker(max_attempts=2)
        message = Producer(broker).send("invalid", message_id="bad-1")

        delivered = broker.deliver()
        self.assertEqual(delivered.message_id, message.message_id)
        broker.nack(message.message_id)
        self.assertEqual(broker.pending_count, 1)

        delivered_again = broker.deliver()
        self.assertEqual(delivered_again.attempts, 2)
        broker.nack(message.message_id)

        self.assertEqual(broker.pending_count, 0)
        self.assertEqual([item.message_id for item in broker.dead_letters], ["bad-1"])

    def test_duplicate_message_id_does_not_repeat_effect(self):
        effects = []
        broker = Broker()
        consumer = Consumer(broker, effects.append)
        producer = Producer(broker)
        producer.send("shipment-created", message_id="same-id")
        producer.send("shipment-created", message_id="same-id")

        consumer.process_until_empty()

        self.assertEqual(effects, ["shipment-created"])

    def test_effect_failure_is_nacked_for_retry(self):
        attempts = []
        broker = Broker(max_attempts=2)

        def failing_effect(payload):
            attempts.append(payload)
            raise ValueError("temporary failure")

        consumer = Consumer(broker, failing_effect)
        Producer(broker).send("retry-me", message_id="retry-1")

        with self.assertRaises(ValueError):
            consumer.process_one()

        self.assertEqual(broker.pending_count, 1)
        with self.assertRaises(ValueError):
            consumer.process_one()

        self.assertEqual(attempts, ["retry-me", "retry-me"])
        self.assertEqual([item.message_id for item in broker.dead_letters], ["retry-1"])


if __name__ == "__main__":
    unittest.main()
