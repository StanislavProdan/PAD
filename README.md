# Mesagerie fiabila in Python

Acesta este primul increment al proiectului: flux producer-broker-consumer cu garantii explicite si limite documentate.

## Rulare

```powershell
python -m unittest -v
python reliable_messaging.py
python reliable_messaging.py --interactive
python manual_test.py
```

Cu `--interactive`, scrii mesajele tale în consolă. O linie goală încheie introducerea și consumerul procesează mesajele prin broker.


În meniul manual poți alege mesaj normal, mesaj duplicat, crash după efect înainte de ACK, eroare cu retry și DLQ, ordering pentru trei mesaje, afișarea stării brokerului sau istoricul complet al evenimentelor din sesiune.


### ACK și scenariul de crash


Verifică faptul că efectul este aplicat o singură dată, mesajul este redelivrat și apoi confirmat cu ACK.

### NACK, retry și dead-letter queue



Verifică faptul că mesajul este retrimis, iar după `max_attempts` ajunge în `broker.dead_letters`.

### Deduplicare



Verifică faptul că două mesaje cu același `message_id` produc un singur efect local.

### Ordering



Verifică procesarea în ordinea `first`, `second`, `third`.

### Eșecul efectului local



Verifică faptul că o excepție a consumerului produce NACK, retry și în final DLQ.

## Conceptele implementate

### 1. Contract de mesaj

Este definit de clasa `Message` din `reliable_messaging.py`:

- `message_id` identifica unic mesajul si este folosit pentru deduplicare;
- `sequence` indica ordinea publicarii;
- `payload` contine datele efective ale mesajului;
- `attempts` retine numarul de livrari incercate.

`Producer.send()` construieste mesajul prin `Broker.publish()`. Astfel producerul si consumerul folosesc aceeasi structura, nu transfera valori anonime.

### 2. ACK

ACK inseamna ca mesajul a fost procesat cu succes. In `Consumer.process_one()`, efectul local este executat mai intai, iar apoi se apeleaza `broker.ack(message.message_id)`.

Brokerul elimina mesajul din dictionarul `in_flight` numai dupa ACK. Daca procesul se opreste inainte de acest punct, mesajul poate fi livrat din nou.

### 3. NACK

NACK inseamna ca mesajul nu a putut fi procesat. La o exceptie a efectului local, consumerul apeleaza automat `broker.nack()`. NACK poate fi solicitat si explicit prin `Consumer.reject_current()`.

Brokerul scoate mesajul din `in_flight` si il pune inapoi in coada pentru o noua incercare. Daca a fost atins `max_attempts`, nu il mai retrimite si il muta in DLQ.

### 4. Retry

Retry-ul este implementat in `Broker.nack()`. Mesajul este reintrodus cu `appendleft()`, iar la urmatoarea livrare campul `attempts` este incrementat.

Exemplu: cu `max_attempts=3`, acelasi mesaj poate fi livrat de cel mult trei ori. Dupa a treia esuare ajunge in dead-letter queue.

### 5. Deduplicare

Consumerul pastreaza identificatorii mesajelor procesate in `processed_ids`. La redelivery, daca `message_id` exista deja in aceasta colectie, efectul local nu mai este executat, dar mesajul este confirmat cu ACK.

Aceasta rezolva scenariul in care efectul a reusit, dar ACK-ul s-a pierdut. In implementarea actuala setul este in memorie; pentru productie ar trebui persistat impreuna cu efectul local.

### 6. Ordering

Brokerul foloseste `deque`, iar mesajele noi sunt adaugate la coada cu `append()`. `sequence` este incrementata la fiecare publicare, iar un consumer proceseaza mesajele in ordinea FIFO.

Un retry este pus in fata cozii cu `appendleft()`, pentru ca mesajul esuat sa fie rezolvat inaintea urmatorului mesaj. Garantia este valabila pentru un consumer activ; mai multi workeri ar necesita reguli suplimentare de partitionare si ordering.

### 7. Dead-letter queue

DLQ este lista `broker.dead_letters`. Cand un mesaj esueaza dupa `max_attempts`, `Broker.nack()` sau `Broker.recover_unacked()` il muta acolo in loc sa il mai livreze.

DLQ permite izolarea mesajelor problematice pentru investigare, fara blocarea permanenta a cozii principale. In acest prototip este in memorie si se pierde la oprirea procesului.

## Scenariul critic: crash dupa efect, inainte de ACK

Scenariul este simulat de `consumer.process_one(crash_after_effect=True)`:

1. brokerul livreaza mesajul si il pune in `in_flight`;
2. consumerul aplica efectul local si il adauga in `processed_ids`;
3. consumerul se opreste inainte de ACK si mesajul ramane in `in_flight`;
4. `broker.recover_unacked()` il reintroduce in coada;
5. la redelivery, consumerul vede acelasi `message_id`, nu repeta efectul si trimite ACK.

Testul `test_crash_after_effect_redelivers_without_duplicate_effect` verifica exact acest flux.

## Garantii si limite

Implementarea ofera livrare **at-least-once** si ordonare FIFO pentru un consumer activ. Deduplicarea este in memoria procesului; intr-un sistem real, `processed_ids` si efectele trebuie pastrate intr-o baza de date tranzactionala sau intr-un store durabil. Fara o tranzactie comuna intre efect si ACK, nu se poate obtine exact-once end-to-end doar din broker.

`recover_unacked()` modeleaza expirarea lease-ului dupa caderea consumerului. In productie, aceasta ar fi declansata de timeout si ar necesita persistenta, autentificare, observabilitate si mai multi workeri cu reguli explicite de ordering.