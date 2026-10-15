# Dovada scenariului de esec

## Scenariu: crash dupa aplicarea efectului si inainte de ACK

### Starea initiala

- brokerul are un mesaj cu `message_id="payment-1"` in coada;
- consumerul nu are mesajul in `processed_ids`;
- nu exista mesaje in dead-letter queue.

### Injectarea esecului

Testul automat
`test_crash_after_effect_redelivers_without_duplicate_effect` apeleaza:

```python
consumer.process_one(crash_after_effect=True)
```

Flag-ul simuleaza oprirea procesului dupa aplicarea efectului, dar inainte de
confirmarea ACK.

### Comportamentul observat

1. brokerul livreaza mesajul si il pune in `in_flight`;
2. consumerul aplica efectul o singura data;
3. consumerul se opreste inainte de ACK;
4. mesajul ramane neconfirmat;
5. `recover_unacked()` il pune din nou in coada;
6. la redelivery, `message_id` este gasit in `processed_ids`;
7. efectul nu este repetat, iar mesajul primeste ACK.

Istoricul asteptat este:

```text
PUBLISH message_id=payment-1
DELIVER message_id=payment-1 attempt=1
EFFECT_APPLIED message_id=payment-1
CRASH_BEFORE_ACK message_id=payment-1
RECOVER message_id=payment-1 attempt=1 action=RETRY
DELIVER message_id=payment-1 attempt=2
DUPLICATE_IGNORED message_id=payment-1
ACK message_id=payment-1
```

### Starea finala

- efectul local apare o singura data;
- `pending_count == 0`;
- `in_flight_count == 0`;
- mesajul nu ajunge in DLQ;
- redelivery-ul este confirmat prin ACK.

### Garantia obtinuta

Scenariul demonstreaza livrare at-least-once si deduplicare pentru un consumer
activ, atunci cand procesul poate recupera mesajele neconfirmate.

### Ce nu acopera

Starea de deduplicare si efectul sunt in memorie. Oprirea completa a procesului
poate pierde aceste informatii. Pentru productie este necesara persistenta
durabila si o tranzactie sau o strategie de deduplicare intre efect si ACK.
