

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import sys
from typing import Any, Callable
from uuid import uuid4


@dataclass(frozen=True)
class Message:
    message_id: str
    sequence: int
    payload: Any
    attempts: int = 0


class Broker:
    """FIFO broker with explicit ACK/NACK, redelivery and a dead-letter queue."""

    def __init__(self, max_attempts: int = 3) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.max_attempts = max_attempts
        self._pending: deque[Message] = deque()
        self._in_flight: dict[str, Message] = {}
        self.dead_letters: list[Message] = []
        self.events: list[str] = []
        self._next_sequence = 1

    def record_event(self, event: str) -> None:
        self.events.append(event)

    def publish(self, payload: Any, message_id: str | None = None) -> Message:
        message = Message(
            message_id=message_id or str(uuid4()),
            sequence=self._next_sequence,
            payload=payload,
        )
        self._next_sequence += 1
        self._pending.append(message)
        self.record_event(f"PUBLISH message_id={message.message_id} payload={message.payload!r}")
        return message

    def deliver(self) -> Message | None:
        if not self._pending:
            return None
        message = self._pending.popleft()
        leased = Message(
            message_id=message.message_id,
            sequence=message.sequence,
            payload=message.payload,
            attempts=message.attempts + 1,
        )
        self._in_flight[leased.message_id] = leased
        self.record_event(
            f"DELIVER message_id={leased.message_id} attempt={leased.attempts}"
        )
        return leased

    def ack(self, message_id: str) -> None:
        if message_id not in self._in_flight:
            raise KeyError(f"message {message_id!r} is not in flight")
        del self._in_flight[message_id]
        self.record_event(f"ACK message_id={message_id}")

    def nack(self, message_id: str) -> None:
        message = self._in_flight.pop(message_id)
        if message.attempts >= self.max_attempts:
            self.dead_letters.append(message)
            self.record_event(
                f"NACK message_id={message_id} attempt={message.attempts} action=DLQ"
            )
        else:
            self._pending.appendleft(message)
            self.record_event(
                f"NACK message_id={message_id} attempt={message.attempts} action=RETRY"
            )

    def recover_unacked(self) -> None:
        """Redeliver leases left behind by a crashed consumer."""
        for message_id in reversed(list(self._in_flight)):
            message = self._in_flight.pop(message_id)
            if message.attempts >= self.max_attempts:
                self.dead_letters.append(message)
                self.record_event(
                    f"RECOVER message_id={message_id} attempt={message.attempts} action=DLQ"
                )
            else:
                self._pending.appendleft(message)
                self.record_event(
                    f"RECOVER message_id={message_id} attempt={message.attempts} action=RETRY"
                )

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)


class ConsumerCrash(RuntimeError):
    """Represents a process crash after the local effect was applied."""


class Consumer:
    """Applies each message once and ACKs only after the local effect succeeds."""

    def __init__(self, broker: Broker, effect: Callable[[Any], None]) -> None:
        self.broker = broker
        self.effect = effect
        self.processed_ids: set[str] = set()

    def process_one(self, crash_after_effect: bool = False) -> Message | None:
        message = self.broker.deliver()
        if message is None:
            return None

        if message.message_id not in self.processed_ids:
            try:
                self.effect(message.payload)
            except Exception:
                self.broker.record_event(
                    f"EFFECT_FAILED message_id={message.message_id}"
                )
                self.broker.nack(message.message_id)
                raise
            self.processed_ids.add(message.message_id)
            self.broker.record_event(f"EFFECT_APPLIED message_id={message.message_id}")
        else:
            self.broker.record_event(f"DUPLICATE_IGNORED message_id={message.message_id}")

        if crash_after_effect:
            self.broker.record_event(f"CRASH_BEFORE_ACK message_id={message.message_id}")
            raise ConsumerCrash(
                f"consumer crashed before ACK for {message.message_id}"
            )

        self.broker.ack(message.message_id)
        return message

    def process_until_empty(self) -> None:
        while self.broker.pending_count:
            self.process_one()

    def reject_current(self, message_id: str) -> None:
        self.broker.nack(message_id)


class Producer:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker

    def send(self, payload: Any, message_id: str | None = None) -> Message:
        return self.broker.publish(payload, message_id)


def demo() -> list[Any]:
    effects: list[Any] = []
    broker = Broker(max_attempts=3)
    producer = Producer(broker)
    consumer = Consumer(broker, effects.append)
    producer.send("order-created", message_id="order-1")

    try:
        consumer.process_one(crash_after_effect=True)
    except ConsumerCrash:
        broker.recover_unacked()

    consumer.process_until_empty()
    return effects


def interactive_demo() -> None:
    """Read messages from the console and process them through the broker."""
    effects: list[str] = []
    broker = Broker()
    consumer = Consumer(broker, effects.append)
    producer = Producer(broker)

    print("Scrie mesaje. Apasă Enter pe o linie goală pentru terminare.")
    while True:
        payload = input("Mesaj: ")
        if not payload:
            break
        producer.send(payload)

    consumer.process_until_empty()
    print(f"Mesaje procesate: {effects}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_demo()
    else:
        print(demo())
