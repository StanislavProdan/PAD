# Laboratorul 3: Web Proxy pentru Warehouse

## Arhitectura

```text
Client HTTP
    |
    v
Reverse Proxy (ThreadingHTTPServer)
    |-- cache GET, TTL
    |-- Round-Robin
    |-- ConnectionPool
    |
    +--> Warehouse 1 (HTTP + Thread per request)
    |
    +--> Warehouse 2 (HTTP + Thread per request)
```

## Warehouse

`ThreadSafeWarehouse` păstrează angajații într-un dicționar protejat prin
`RLock`. Serverul folosește `ThreadingHTTPServer`, deci cererile concurente
sunt distribuite către thread-uri separate. În demonstrație, cele două
servere folosesc aceeași instanță `ThreadSafeWarehouse`, astfel încât o
modificare rămâne vizibilă indiferent de backend-ul ales prin Round-Robin.

## Reverse Proxy

Proxy-ul este singurul punct de acces public al demonstrației. La fiecare GET:

1. caută un răspuns neexpirat în cache;
2. la HIT returnează răspunsul și antetul `X-Cache: HIT`;
3. la MISS alege un backend Round-Robin;
4. memoriza răspunsul 200 pentru TTL;
5. returnează `X-Cache: MISS` și `X-Backend`.

Conexiunile către warehouse sunt reutilizate printr-un pool thread-safe.
`PUT` și `POST` invalidează cache-ul.

## Demonstrare

```powershell
python demo_web_proxy.py
```

Testele acoperă JSON și XML, operații PUT/POST, pull de modificări, cache,
invalidarea cache-ului și Round-Robin:

```powershell
python -m unittest test_web_proxy -v
```

## Limite

- starea este in-memory și se pierde la repornire;
- pool-ul nu elimină automat backend-urile inactive;
- cache-ul este local procesului proxy;
- Redis și Cassandra pot fi adăugate pentru persistență și distribuție reală.
