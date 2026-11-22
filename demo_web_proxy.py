from threading import Thread
from time import sleep

from web_proxy import (
    ReverseProxyServer,
    ThreadSafeWarehouse,
    WarehouseHTTPServer,
)


def start(server):
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def main() -> None:
    shared_store = ThreadSafeWarehouse(
        [
            {"id": 1, "name": "Ada Lovelace", "role": "Engineer"},
            {"id": 2, "name": "Grace Hopper", "role": "Admiral"},
        ]
    )
    warehouse_one = WarehouseHTTPServer(
        ("127.0.0.1", 0),
        shared_store,
    )
    warehouse_two = WarehouseHTTPServer(
        ("127.0.0.1", 0),
        shared_store,
    )
    thread_one = start(warehouse_one)
    thread_two = start(warehouse_two)

    proxy = ReverseProxyServer(
        ("127.0.0.1", 8080),
        [
            warehouse_one.server_address,
            warehouse_two.server_address,
        ],
        cache_ttl_seconds=5,
    )
    proxy_thread = start(proxy)

    print("Warehouse 1:", warehouse_one.server_address)
    print("Warehouse 2:", warehouse_two.server_address)
    print("Reverse proxy: http://127.0.0.1:8080")
    print("GET /employees")
    print("GET /employee?id=1")
    print("PUT /employees cu un corp JSON")
    print("Ctrl+C pentru oprire")

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        proxy.shutdown()
        proxy.server_close()
        warehouse_one.shutdown()
        warehouse_one.server_close()
        warehouse_two.shutdown()
        warehouse_two.server_close()
        proxy_thread.join(timeout=2)
        thread_one.join(timeout=2)
        thread_two.join(timeout=2)


if __name__ == "__main__":
    main()
