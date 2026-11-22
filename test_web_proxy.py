from http.client import HTTPConnection
from threading import Thread
import json
import unittest

from web_proxy import (
    ReverseProxyServer,
    ThreadSafeWarehouse,
    WarehouseHTTPServer,
)


def start_server(server):
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def request(host, port, method, path, body=None, headers=None):
    connection = HTTPConnection(host, port, timeout=5)
    request_headers = dict(headers or {})
    if body is not None:
        request_headers["Content-Type"] = "application/json"
        request_body = json.dumps(body).encode("utf-8")
    else:
        request_body = None
    connection.request(method, path, body=request_body, headers=request_headers)
    response = connection.getresponse()
    response_body = response.read()
    response_headers = dict(response.getheaders())
    connection.close()
    return response.status, response_headers, response_body


class WebProxyTests(unittest.TestCase):
    def setUp(self):
        self.warehouses = []
        self.warehouse_threads = []
        shared_store = ThreadSafeWarehouse(
            [
                {"id": 1, "name": "Employee 1"},
                {"id": 2, "name": "Employee 2"},
            ]
        )
        for employee_id in (1, 2):
            server = WarehouseHTTPServer(("127.0.0.1", 0), shared_store)
            thread = start_server(server)
            self.warehouses.append(server)
            self.warehouse_threads.append(thread)

        self.proxy = ReverseProxyServer(
            ("127.0.0.1", 0),
            [
                (
                    warehouse.server_address[0],
                    warehouse.server_address[1],
                )
                for warehouse in self.warehouses
            ],
            cache_ttl_seconds=30,
        )
        self.proxy_thread = start_server(self.proxy)
        self.proxy_host, self.proxy_port = self.proxy.server_address

    def tearDown(self):
        self.proxy.shutdown()
        self.proxy.server_close()
        self.proxy_thread.join(timeout=2)
        for warehouse in self.warehouses:
            warehouse.shutdown()
            warehouse.server_close()
        for thread in self.warehouse_threads:
            thread.join(timeout=2)

    def test_concurrent_warehouse_accepts_put_and_get_json(self):
        host, port = self.warehouses[0].server_address
        status, _, body = request(
            host,
            port,
            "PUT",
            "/employees",
            {"id": 10, "name": "Ada", "role": "Engineer"},
        )
        self.assertEqual(status, 200)
        status, headers, body = request(
            host,
            port,
            "GET",
            "/employee?id=10&format=json",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(status, 200)
        self.assertIn("application/json", headers["Content-Type"])
        self.assertEqual(json.loads(body)["employee"]["name"], "Ada")

    def test_warehouse_can_return_xml_when_requested_by_accept(self):
        host, port = self.warehouses[0].server_address
        status, headers, body = request(
            host,
            port,
            "GET",
            "/employees",
            headers={"Accept": "application/xml"},
        )
        self.assertEqual(status, 200)
        self.assertIn("application/xml", headers["Content-Type"])
        self.assertIn(b"<response>", body)

    def test_proxy_round_robin_routes_different_requests(self):
        status1, headers1, _ = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employee?id=1",
        )
        status2, headers2, _ = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employee?id=2",
        )
        self.assertEqual(status1, 200)
        self.assertEqual(status2, 200)
        self.assertIn("1", headers1["X-Backend"])
        self.assertIn("2", headers2["X-Backend"])

    def test_proxy_caches_get_and_invalidates_after_put(self):
        status1, headers1, body1 = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employees?offset=0&limit=100",
        )
        status2, headers2, body2 = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employees?offset=0&limit=100",
        )
        self.assertEqual(status1, 200)
        self.assertEqual(status2, 200)
        self.assertEqual(headers1["X-Cache"], "MISS")
        self.assertEqual(headers2["X-Cache"], "HIT")

        status3, _, _ = request(
            self.proxy_host,
            self.proxy_port,
            "PUT",
            "/employees",
            {"id": 11, "name": "Grace"},
        )
        self.assertEqual(status3, 200)

        status4, headers4, body4 = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employees?offset=0&limit=100",
        )
        self.assertEqual(status4, 200)
        self.assertEqual(headers4["X-Cache"], "MISS")
        self.assertEqual(body1, body2)
        self.assertNotEqual(body4, body2)

    def test_proxy_cache_keeps_json_and_xml_variants_separate(self):
        status_json, headers_json, _ = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employees",
            headers={"Accept": "application/json"},
        )
        status_xml, headers_xml, body_xml = request(
            self.proxy_host,
            self.proxy_port,
            "GET",
            "/employees",
            headers={"Accept": "application/xml"},
        )
        self.assertEqual(status_json, 200)
        self.assertEqual(status_xml, 200)
        self.assertEqual(headers_json["X-Cache"], "MISS")
        self.assertEqual(headers_xml["X-Cache"], "MISS")
        self.assertIn("application/xml", headers_xml["Content-Type"])
        self.assertIn(b"<response>", body_xml)

    def test_post_pull_update_queue(self):
        host, port = self.warehouses[0].server_address
        status, _, _ = request(
            host,
            port,
            "POST",
            "/update/employees",
            {"id": 12, "name": "Linus"},
        )
        self.assertEqual(status, 202)

        status, _, body = request(
            host,
            port,
            "GET",
            "/update/employees",
        )
        self.assertEqual(status, 200)
        updates = json.loads(body)["updates"]
        self.assertEqual(updates[0]["operation"], "upsert")
        self.assertEqual(updates[0]["employee"]["name"], "Linus")


if __name__ == "__main__":
    unittest.main()
