from reliable_messaging import Broker, Consumer, ConsumerCrash, Producer


def read_message() -> str:
    return input("  Scrie mesajul: ")


def header(title: str, concept: str, action: str) -> None:
    print(f"\n--- {title} ---")
    print(f"Testeaza: {concept}")
    print(f"Actiune: {action}")


def result(ok: bool, expected: str, actual: str) -> None:
    status = "SUCCES" if ok else "ESEC"
    print(f"\n[{status}]")
    print(f"Asteptat: {expected}")
    print(f"Obtinut:  {actual}")


def run() -> None:
    broker = Broker(max_attempts=2)
    producer = Producer(broker)
    effects: list[str] = []
    consumer = Consumer(broker, effects.append)

    while True:
        print("\n================ TESTARE MANUALA ================")
        print("1. Flux normal             (publish + ACK)")
        print("2. Mesaj duplicat          (deduplicare)")
        print("3. Crash inainte de ACK    (redelivery + idempotenta)")
        print("4. Eroare procesare        (NACK + retry + DLQ)")
        print("5. Trei mesaje             (ordering FIFO)")
        print("6. Stare broker            (pending/in-flight/DLQ)")
        print("0. Iesire")
        choice = input("Alege un scenariu [0-6]: ")

        if choice == "0":
            return
        if choice == "1":
            header("Flux normal", "contract, publish si ACK", "trimite si proceseaza un mesaj")
            before = len(effects)
            producer.send(read_message())
            consumer.process_until_empty()
            result(
                len(effects) == before + 1,
                "un efect local si mesajul scos din coada",
                f"efecte totale: {len(effects)}, pending: {broker.pending_count}",
            )
        elif choice == "2":
            header("Mesaj duplicat", "deduplicare", "publica de doua ori acelasi message_id")
            before = len(effects)
            message_id = input("  ID mesaj (exemplu: comanda-1): ")
            payload = read_message()
            producer.send(payload, message_id=message_id)
            producer.send(payload, message_id=message_id)
            consumer.process_until_empty()
            result(
                len(effects) == before + 1,
                "doua livrari, dar un singur efect local",
                f"efecte noi: {len(effects) - before}",
            )
        elif choice == "3":
            header("Crash inainte de ACK", "redelivery si deduplicare", "aplica efectul, simuleaza crash, apoi recupereaza mesajul")
            before = len(effects)
            message = producer.send(read_message())
            try:
                consumer.process_one(crash_after_effect=True)
            except ConsumerCrash:
                print("  Consumerul a cazut inainte de ACK.")
                broker.recover_unacked()
            consumer.process_until_empty()
            result(
                len(effects) == before + 1 and broker.in_flight_count == 0,
                "un singur efect si ACK dupa redelivery",
                f"mesaj redelivrat: {message.message_id}, efecte noi: {len(effects) - before}",
            )
        elif choice == "4":
            header("Eroare procesare", "NACK, retry si DLQ", "forteaza doua esecuri pentru acelasi mesaj")
            failing_broker = broker

            def failing_effect(payload: str) -> None:
                raise ValueError(f"Efect esuat pentru {payload}")

            failing_consumer = Consumer(failing_broker, failing_effect)
            failing_message = Producer(failing_broker).send(read_message())
            for _ in range(2):
                try:
                    failing_consumer.process_one()
                except ValueError as error:
                    print(f"  {error}")
            in_dlq = [item.message_id for item in failing_broker.dead_letters]
            result(
                in_dlq == [failing_message.message_id],
                "doua incercari si mesajul mutat in DLQ",
                f"incercari: 2, DLQ: {in_dlq}",
            )
        elif choice == "5":
            header("Ordering FIFO", "sequence si ordering", "publica trei mesaje si compara ordinea efectelor")
            before = len(effects)
            for _ in range(3):
                producer.send(read_message())
            consumer.process_until_empty()
            processed = effects[before:]
            result(
                len(processed) == 3,
                "mesajele procesate in ordinea introducerii",
                f"ordinea efectelor: {processed}",
            )
        elif choice == "6":
            header("Stare broker", "observabilitate locala", "afiseaza cozile si mesajele blocate")
            print(f"In asteptare: {broker.pending_count}")
            print(f"In procesare: {broker.in_flight_count}")
            print(f"DLQ: {len(broker.dead_letters)}")
        else:
            print("Optiune necunoscuta. Alege un numar intre 0 si 6.")


if __name__ == "__main__":
    run()
