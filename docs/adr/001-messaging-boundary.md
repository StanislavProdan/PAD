# ADR-001: Separarea contractului de mesagerie de suprafata de transport

- **Status:** acceptat
- **Data:** 2026-10-15

## Context

Primele incremente trebuie sa demonstreze livrare fiabila, redelivery,
deduplicare si tratarea unui rezultat ambiguu fara a introduce inca o
infrastructura persistenta. Sunt necesare atat un model local de broker, cat si
un contract RPC explicit pentru client-server.

## Decizie

Pastram mecanismele de mesagerie in `reliable_messaging.py` si expunem
contractul RPC in `reliable_messaging.proto`, cu implementarea gRPC in
`grpc_service.py`. Identificatorii `message_id` si `request_id` sunt distincti:
primul identifica mesajul, iar al doilea identifica operatia clientului.

Pentru timeout sau anulare, clientul nu repeta efectul. El foloseste
`GetOperationStatus` cu acelasi `request_id`.

## Alternative analizate

1. **Retry automat direct al apelului RPC** - respins, deoarece poate executa
   efectul de doua ori dupa pierderea raspunsului.
2. **Folosirea unui singur identificator pentru mesaj si operatie** - respins,
   deoarece o operatie poate genera un mesaj, iar responsabilitatile celor doua
   entitati sunt diferite.
3. **Introducerea unei baze de date in primul increment** - amanata pentru a
   mentine incrementul concentrat pe protocol si comportamentul de esec.

## Consecinte

Obtinem idempotency demonstrabila si o rezolvare explicita a ambiguitatii.
Contractul este versionat si poate fi verificat independent de implementare.
In schimb, starea este volatila, iar gRPC si brokerul TCP nu sunt inca
conectate printr-un serviciu de comenzi comun.

## Limita cunoscuta

Fara persistenta comuna intre efect si confirmare nu poate fi afirmata
garantia exact-once end-to-end. Implementarea documenteaza explicit aceasta
limita si ofera at-least-once cu deduplicare in memorie.
