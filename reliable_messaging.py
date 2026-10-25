

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import json
import socket
import socketserver
import sys
from threading import RLock
from typing import Any, Callable
from uuid import uuid4


@dataclass(frozen=True)
class Message:
    message_id: str
    sequence: int
    payload: Any
    attempts: int = 0
    message_type: str = "default"


class Broker:
    """Broker FIFO cu ACK/NACK explicit, redelivrare si coada de mesaje moarte."""

    def __init__(self, max_attempts: int = 3) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.max_attempts = max_attempts
        self._pending: deque[Message] = deque()
        self._in_flight: dict[str, Message] = {}
        self.dead_letters: list[Message] = []
        self.events: list[str] = []
        self._next_sequence = 1
        self._lock = RLock()

    def record_event(self, event: str) -> None:
        with self._lock:
            self.events.append(event)

    def publish(
        self,
        payload: Any,
        message_id: str | None = None,
        message_type: str = "default",
    ) -> Message:
        if not isinstance(message_type, str) or not message_type:
            raise ValueError("message_type must be a non-empty string")
        with self._lock:
            message = Message(
                message_id=message_id or str(uuid4()),
                sequence=self._next_sequence,
                payload=payload,
                message_type=message_type,
            )
            self._next_sequence += 1
            self._pending.append(message)
            self.events.append(
                f"PUBLISH message_id={message.message_id} payload={message.payload!r}"
            )
            return message

    def deliver(self) -> Message | None:
        with self._lock:
            if not self._pending:
                return None
            message = self._pending.popleft()
            leased = Message(
                message_id=message.message_id,
                sequence=message.sequence,
                payload=message.payload,
                attempts=message.attempts + 1,
                message_type=message.message_type,
            )
            self._in_flight[leased.message_id] = leased
            self.events.append(
                f"DELIVER message_id={leased.message_id} attempt={leased.attempts}"
            )
            return leased

    def ack(self, message_id: str) -> None:
        with self._lock:
            if message_id not in self._in_flight:
                raise KeyError(f"message {message_id!r} is not in flight")
            del self._in_flight[message_id]
            self.events.append(f"ACK message_id={message_id}")

    def nack(self, message_id: str) -> None:
        with self._lock:
            message = self._in_flight.pop(message_id)
            if message.attempts >= self.max_attempts:
                self.dead_letters.append(message)
                self.events.append(
                    f"NACK message_id={message_id} attempt={message.attempts} action=DLQ"
                )
            else:
                self._pending.appendleft(message)
                self.events.append(
                    f"NACK message_id={message_id} attempt={message.attempts} action=RETRY"
                )

    def recover_unacked(self) -> None:
        """Retrimite mesajele ramase in asteptare dupa caderea consumerului."""
        with self._lock:
            for message_id in reversed(list(self._in_flight)):
                message = self._in_flight.pop(message_id)
                if message.attempts >= self.max_attempts:
                    self.dead_letters.append(message)
                    self.events.append(
                        f"RECOVER message_id={message_id} attempt={message.attempts} action=DLQ"
                    )
                else:
                    self._pending.appendleft(message)
                    self.events.append(
                        f"RECOVER message_id={message_id} attempt={message.attempts} action=RETRY"
                    )

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    @property
    def in_flight_count(self) -> int:
        with self._lock:
            return len(self._in_flight)


class ConsumerCrash(RuntimeError):
    """Reprezinta caderea procesului dupa aplicarea efectului local."""


class Consumer:
    """Aplica fiecare mesaj o singura data si confirma doar dupa succesul efectului."""

    def __init__(self, broker: Broker, effect: Callable[[Any], None]) -> None:
        self.broker = broker
        self.effect = effect
        self.processed_ids: set[str] = set()
        self._lock = RLock()

    def process_one(self, crash_after_effect: bool = False) -> Message | None:
        with self._lock:
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
                self.broker.record_event(
                    f"DUPLICATE_IGNORED message_id={message.message_id}"
                )

            if crash_after_effect:
                self.broker.record_event(
                    f"CRASH_BEFORE_ACK message_id={message.message_id}"
                )
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

    def send(
        self,
        payload: Any,
        message_id: str | None = None,
        message_type: str = "default",
    ) -> Message:
        return self.broker.publish(payload, message_id, message_type)


class TCPBrokerRequestHandler(socketserver.StreamRequestHandler):
    """Proceseaza comenzi JSON delimitate prin linie pentru serverul TCP al brokerului."""

    def handle(self) -> None:
        server = self.server
        if not isinstance(server, TCPBrokerServer):
            raise RuntimeError("invalid TCP broker server")

        for raw_request in self.rfile:
            try:
                request = json.loads(raw_request.decode("utf-8"))
                response = server.handle_request(request)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, KeyError) as error:
                response = {"ok": False, "error": str(error)}
            except Exception as error:
                response = {"ok": False, "error": f"internal server error: {error}"}

            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()


class TCPBrokerServer(socketserver.ThreadingTCPServer):
    """Expune un broker din memorie printr-un protocol TCP/JSON simplu."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        max_attempts: int = 3,
    ) -> None:
        super().__init__(server_address, TCPBrokerRequestHandler)
        self.broker = Broker(max_attempts=max_attempts)
        self.effects: list[Any] = []
        self.consumer = Consumer(self.broker, self.effects.append)
        self._state_lock = RLock()

    def handle_request(self, request: Any) -> dict[str, Any]:
        with self._state_lock:
            return self._handle_request(request)

    def _handle_request(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, dict):
            raise ValueError("request must be a JSON object")

        action = request.get("action")
        if action == "publish":
            if "payload" not in request:
                raise ValueError("publish requires a payload")
            message = Producer(self.broker).send(
                request["payload"],
                message_id=request.get("message_id"),
            )
            self.consumer.process_until_empty()
            return {
                "ok": True,
                "message_id": message.message_id,
                "sequence": message.sequence,
                "status": "acked",
            }
        if action == "status":
            return {
                "ok": True,
                "pending": self.broker.pending_count,
                "in_flight": self.broker.in_flight_count,
                "dead_letters": len(self.broker.dead_letters),
                "processed": len(self.effects),
            }
        if action == "events":
            return {"ok": True, "events": list(self.broker.events)}
        if action == "shutdown":
            self.shutdown()
            return {"ok": True, "status": "shutting-down"}
        raise ValueError(f"unknown action: {action!r}")


def run_tcp_server(host: str = "127.0.0.1", port: int = 9000) -> None:
    """Porneste serverul TCP si il mentine activ pana la intrerupere."""
    with TCPBrokerServer((host, port)) as server:
        print(f"Server TCP pornit pe {host}:{server.server_address[1]}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer oprit.")
        finally:
            server.shutdown()


def send_tcp_request(
    host: str,
    port: int,
    request: dict[str, Any],
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Trimite o cerere JSON si returneaza raspunsul serverului."""
    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.sendall((json.dumps(request) + "\n").encode("utf-8"))
        with connection.makefile("rb") as response_stream:
            response = response_stream.readline()
    if not response:
        raise ConnectionError("server closed the connection without a response")
    decoded = json.loads(response.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("server response must be a JSON object")
    return decoded


class TCPReceiverRequestHandler(socketserver.StreamRequestHandler):
    """Primeste mesaje JSON validate de la un broker de redirectionare."""

    def handle(self) -> None:
        server = self.server
        if not isinstance(server, TCPReceiverServer):
            raise RuntimeError("invalid TCP receiver server")
        for raw_message in self.rfile:
            try:
                message = json.loads(raw_message.decode("utf-8"))
                if not isinstance(message, dict):
                    raise ValueError("message must be a JSON object")
                if not isinstance(message.get("message_type"), str):
                    raise ValueError("message_type is required")
                if "payload" not in message:
                    raise ValueError("payload is required")
                server.receive(message)
                response = {"ok": True}
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                response = {"ok": False, "error": str(error)}
            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()


class TCPReceiverServer(socketserver.ThreadingTCPServer):
    """Punct de intrare pentru receiverul din fluxul sender-broker-receiver."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        effect: Callable[[Any], None] | None = None,
    ) -> None:
        super().__init__(server_address, TCPReceiverRequestHandler)
        self.effect = effect or (lambda payload: None)
        self.messages: list[dict[str, Any]] = []
        self._state_lock = RLock()

    def receive(self, message: dict[str, Any]) -> None:
        with self._state_lock:
            self.messages.append(dict(message))
            self.effect(message["payload"])


class TCPForwardingBrokerRequestHandler(socketserver.StreamRequestHandler):
    """Accepta mesaje de la senderi si le redirectioneaza catre receiverul configurat."""

    def handle(self) -> None:
        server = self.server
        if not isinstance(server, TCPForwardingBrokerServer):
            raise RuntimeError("invalid TCP forwarding broker server")
        for raw_request in self.rfile:
            try:
                request = json.loads(raw_request.decode("utf-8"))
                response = server.forward(request)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                response = {"ok": False, "error": str(error)}
            except (ConnectionError, OSError) as error:
                response = {"ok": False, "error": f"receiver unavailable: {error}"}
            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()


class TCPForwardingBrokerServer(socketserver.ThreadingTCPServer):
    """Broker socket minimal care directioneaza mesaje JSON catre receiveri cunoscuti."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        receiver_address: tuple[str, int],
        routes: dict[str, tuple[str, int]] | None = None,
    ) -> None:
        super().__init__(server_address, TCPForwardingBrokerRequestHandler)
        self.receiver_address = receiver_address
        self.routes = dict(routes or {})

    def forward(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, dict):
            raise ValueError("message must be a JSON object")
        message_type = request.get("message_type")
        if not isinstance(message_type, str) or not message_type:
            raise ValueError("message_type must be a non-empty string")
        if "payload" not in request:
            raise ValueError("payload is required")
        receiver_address = self.routes.get(message_type, self.receiver_address)
        response = send_tcp_request(
            receiver_address[0],
            receiver_address[1],
            {
                "message_id": request.get("message_id", str(uuid4())),
                "message_type": message_type,
                "payload": request["payload"],
            },
        )
        if not response.get("ok"):
            raise ConnectionError(response.get("error", "receiver rejected message"))
        return {"ok": True, "status": "forwarded"}


def send_message(
    host: str,
    port: int,
    payload: Any,
    message_type: str,
    message_id: str | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Trimite un mesaj JSON catre un broker socket."""
    if not message_type:
        raise ValueError("message_type must be a non-empty string")
    return send_tcp_request(
        host,
        port,
        {
            "message_id": message_id or str(uuid4()),
            "message_type": message_type,
            "payload": payload,
        },
        timeout=timeout,
    )


def interactive_tcp_client(host: str, port: int) -> None:
    """Citeste mesaje din consola si le publica prin serverul TCP."""
    print(f"Client conectat la {host}:{port}. Linie goala pentru terminare.")
    while True:
        payload = input("Mesaj: ")
        if not payload:
            break
        response = send_tcp_request(host, port, {"action": "publish", "payload": payload})
        if not response.get("ok"):
            print(f"Eroare server: {response.get('error', 'necunoscuta')}")
        else:
            print(f"ACK: message_id={response['message_id']}")


def tcp_menu() -> None:
    """Alege din consola un endpoint TCP salvat si operatia dorita."""
    endpoints = {
        "1": ("Server local", "127.0.0.1", 9000),
    }

    while True:
        print("\n========== MENIU TCP ==========")
        for key, (name, host, port) in endpoints.items():
            print(f"{key}. {name} ({host}:{port})")
        print("n. Adauga server")
        print("0. Iesire")
        choice = input("Alege serverul: ").strip().lower()

        if choice == "0":
            return
        if choice == "n":
            name = input("Numele serverului: ").strip()
            host = input("Adresa serverului: ").strip()
            port_text = input("Portul serverului: ").strip()
            if not name or not host:
                print("Numele si adresa sunt obligatorii.")
                continue
            try:
                port = int(port_text)
                if not 1 <= port <= 65535:
                    raise ValueError
            except ValueError:
                print("Portul trebuie sa fie un numar intre 1 si 65535.")
                continue
            next_key = str(max((int(key) for key in endpoints), default=0) + 1)
            endpoints[next_key] = (name, host, port)
            print(f"Serverul {name!r} a fost adaugat ca optiunea {next_key}.")
            continue

        endpoint = endpoints.get(choice)
        if endpoint is None:
            print("Optiune necunoscuta.")
            continue

        name, host, port = endpoint
        print(f"\nServer selectat: {name} ({host}:{port})")
        print("1. Porneste serverul")
        print("2. Conecteaza clientul")
        print("3. Afiseaza statusul")
        print("4. Afiseaza evenimentele")
        print("b. Inapoi")
        action = input("Alege actiunea: ").strip().lower()

        if action == "1":
            run_tcp_server(host, port)
            return
        if action == "2":
            try:
                interactive_tcp_client(host, port)
            except (ConnectionError, OSError) as error:
                print(f"Conectarea a esuat: {error}")
        elif action == "3":
            try:
                print(send_tcp_request(host, port, {"action": "status"}))
            except (ConnectionError, OSError) as error:
                print(f"Conectarea a esuat: {error}")
        elif action == "4":
            try:
                print(send_tcp_request(host, port, {"action": "events"}))
            except (ConnectionError, OSError) as error:
                print(f"Conectarea a esuat: {error}")


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
    """Citeste mesaje din consola si le proceseaza prin broker."""
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
    if len(sys.argv) > 1 and sys.argv[1] == "--menu":
        tcp_menu()
    elif len(sys.argv) > 1 and sys.argv[1] == "--server":
        run_tcp_server(
            host=sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1",
            port=int(sys.argv[3]) if len(sys.argv) > 3 else 9000,
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "--client":
        interactive_tcp_client(
            host=sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1",
            port=int(sys.argv[3]) if len(sys.argv) > 3 else 9000,
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_demo()
    else:
        print(demo())
