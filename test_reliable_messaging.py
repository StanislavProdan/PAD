import unittest
from threading import Thread
import time

from reliable_messaging import (
    Broker,
    Consumer,
    ConsumerCrash,
    Producer,
    TCPBrokerServer,
    TCPForwardingBrokerServer,
    TCPReceiverServer,
    send_tcp_request,
    send_message,
)


class ReliableMessagingTests(unittest.TestCase):
    """Testeaza brokerul, consumerul si transportul TCP pentru mesaje fiabile."""

    def test_crash_after_effect_redelivers_without_duplicate_effect(self):
        # Verifica faptul ca un mesaj ramas neconfirmat dupa un crash este
        # redelivrat, dar efectul local nu este aplicat din nou.
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
        # Verifica pastrarea ordinii FIFO pentru mesajele publicate in broker.
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
        # Verifica retry-ul dupa NACK si mutarea mesajului in dead-letter queue
        # dupa atingerea numarului maxim de incercari.
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
        # Verifica deduplicarea: doua mesaje cu acelasi message_id produc un
        # singur efect local.
        effects = []
        broker = Broker()
        consumer = Consumer(broker, effects.append)
        producer = Producer(broker)
        producer.send("shipment-created", message_id="same-id")
        producer.send("shipment-created", message_id="same-id")

        consumer.process_until_empty()

        self.assertEqual(effects, ["shipment-created"])

    def test_effect_failure_is_nacked_for_retry(self):
        # Verifica faptul ca esecul efectului produce NACK, permite un retry
        # si ajunge in DLQ dupa esecuri repetate.
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

    def test_event_history_records_redelivery_and_deduplication(self):
        # Verifica istoricul complet pentru publicare, crash, redeliverare,
        # deduplicare si confirmarea finala prin ACK.
        broker = Broker()
        consumer = Consumer(broker, lambda payload: None)
        Producer(broker).send("payment", message_id="payment-1")

        with self.assertRaises(ConsumerCrash):
            consumer.process_one(crash_after_effect=True)

        broker.recover_unacked()
        consumer.process_until_empty()

        self.assertEqual(
            broker.events,
            [
                "PUBLISH message_id=payment-1 payload='payment'",
                "DELIVER message_id=payment-1 attempt=1",
                "EFFECT_APPLIED message_id=payment-1",
                "CRASH_BEFORE_ACK message_id=payment-1",
                "RECOVER message_id=payment-1 attempt=1 action=RETRY",
                "DELIVER message_id=payment-1 attempt=2",
                "DUPLICATE_IGNORED message_id=payment-1",
                "ACK message_id=payment-1",
            ],
        )

    def test_tcp_client_publishes_to_server_owned_broker(self):
        # Verifica faptul ca un client TCP publica in brokerul detinut de
        # server si primeste statusul corect dupa procesarea mesajului.
        server = TCPBrokerServer(("127.0.0.1", 0))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            response = send_tcp_request(
                host,
                port,
                {
                    "action": "publish",
                    "payload": "tcp-message",
                    "message_id": "tcp-1",
                },
            )
            self.assertEqual(response["status"], "acked")
            self.assertEqual(server.effects, ["tcp-message"])

            status = send_tcp_request(host, port, {"action": "status"})
            self.assertEqual(status["processed"], 1)
            self.assertEqual(status["pending"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_socket_sender_broker_receiver_routes_json_message(self):
        received = []
        receiver = TCPReceiverServer(("127.0.0.1", 0), received.append)
        receiver_thread = Thread(target=receiver.serve_forever, daemon=True)
        receiver_thread.start()
        broker = TCPForwardingBrokerServer(
            ("127.0.0.1", 0), receiver.server_address
        )
        broker_thread = Thread(target=broker.serve_forever, daemon=True)
        broker_thread.start()
        try:
            response = send_message(
                "127.0.0.1",
                broker.server_address[1],
                {"order_id": "order-1"},
                "order.created",
                message_id="socket-1",
            )
            self.assertEqual(response, {"ok": True, "status": "forwarded"})
            self.assertEqual(received, [{"order_id": "order-1"}])
            self.assertEqual(receiver.messages[0]["message_type"], "order.created")
        finally:
            broker.shutdown()
            broker.server_close()
            receiver.shutdown()
            receiver.server_close()
            broker_thread.join(timeout=2)
            receiver_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
