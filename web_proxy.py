from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, RLock
from time import monotonic
from urllib.parse import parse_qs, urlparse
from xml.etree import ElementTree as ET
import json
from typing import Iterable


class ThreadSafeWarehouse:
    """Stocheaza angajatii si modificarile in asteptare in mod thread-safe."""

    def __init__(self, employees: Iterable[dict] | None = None) -> None:
        self._lock = RLock()
        self._employees: dict[int, dict] = {}
        self._updates: deque[dict] = deque()
        for employee in employees or []:
            employee_id = int(employee["id"])
            normalized = dict(employee)
            normalized["id"] = employee_id
            self._employees[employee_id] = normalized

    def upsert(self, employee: dict) -> dict:
        employee_id = int(employee["id"])
        if employee_id < 1:
            raise ValueError("id must be a positive integer")
        normalized = dict(employee)
        normalized["id"] = employee_id
        with self._lock:
            self._employees[employee_id] = normalized
            self._updates.append({"operation": "upsert", "employee": normalized})
        return normalized

    def delete(self, employee_id: int) -> dict | None:
        with self._lock:
            employee = self._employees.pop(employee_id, None)
            if employee is not None:
                self._updates.append(
                    {
                        "operation": "delete",
                        "employee": {"id": employee_id},
                    }
                )
                return dict(employee)
        return None

    def get(self, employee_id: int) -> dict | None:
        with self._lock:
            employee = self._employees.get(employee_id)
            return dict(employee) if employee is not None else None

    def list(self, offset: int = 0, limit: int = 100) -> list[dict]:
        with self._lock:
            employees = [
                dict(employee)
                for _, employee in sorted(self._employees.items())
            ]
        return employees[offset : offset + limit]

    def count(self) -> int:
        with self._lock:
            return len(self._employees)

    def pull_updates(self) -> list[dict]:
        with self._lock:
            updates = list(self._updates)
            self._updates.clear()
            return updates


class WarehouseRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PAD-Warehouse/1.0"

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            path = parsed.path.rstrip("/") or "/"
            if path in ("/employees/updates", "/update/employees"):
                data = {"updates": self.server.store.pull_updates()}
                self._send_json_or_xml(data, 200)
                return

            if path == "/employee" and "id" in query:
                employee_id = int(query["id"][0])
                employee = self.server.store.get(employee_id)
                if employee is None:
                    self._send_json_or_xml({"error": "Employee not found"}, 404)
                else:
                    self._send_json_or_xml({"employee": employee}, 200)
                return

            if path.startswith("/employees/"):
                employee_id = int(path.rsplit("/", 1)[1])
                employee = self.server.store.get(employee_id)
                if employee is None:
                    self._send_json_or_xml({"error": "Employee not found"}, 404)
                else:
                    self._send_json_or_xml({"employee": employee}, 200)
                return

            if path == "/employees" or path == "/employee":
                offset = int(query.get("offset", ["0"])[0])
                limit = int(query.get("limit", ["100"])[0])
                if offset < 0 or limit < 1:
                    raise ValueError("offset and limit must be valid")
                employees = self.server.store.list(offset, limit)
                self._send_json_or_xml(
                    {
                        "employees": employees,
                        "offset": offset,
                        "limit": limit,
                        "count": len(employees),
                    },
                    200,
                )
                return
            self._send_json_or_xml({"error": "Not found"}, 404)
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            self._send_json_or_xml({"error": str(error)}, 400)

    def do_PUT(self) -> None:
        try:
            employee = self._read_json()
            path = urlparse(self.path).path.rstrip("/")
            if path.startswith("/employees/"):
                employee_id = int(path.rsplit("/", 1)[1])
                if "id" in employee and int(employee["id"]) != employee_id:
                    raise ValueError("ID in body must match ID in path")
                employee["id"] = employee_id
                stored = self.server.store.upsert(employee)
            else:
                stored = self.server.store.upsert(employee)
            self._send_json_or_xml({"employee": stored}, 200)
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            self._send_json_or_xml({"error": str(error)}, 400)

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            body = self._read_json()
            path = parsed.path.rstrip("/")
            if path in ("/employees/updates", "/update/employees"):
                stored = self.server.store.upsert(body)
                self._send_json_or_xml(
                    {"status": "accepted", "employee": stored}, 202
                )
            elif path == "/employees":
                stored = self.server.store.upsert(body)
                self._send_json_or_xml({"employee": stored}, 201)
            else:
                self._send_json_or_xml({"error": "Not found"}, 404)
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            self._send_json_or_xml({"error": str(error)}, 400)

    def do_DELETE(self) -> None:
        try:
            path = urlparse(self.path).path.rstrip("/")
            if not path.startswith("/employees/"):
                self._send_json_or_xml({"error": "Not found"}, 404)
                return
            employee_id = int(path.rsplit("/", 1)[1])
            deleted = self.server.store.delete(employee_id)
            if deleted is None:
                self._send_json_or_xml({"error": "Employee not found"}, 404)
            else:
                self._send_json_or_xml({"employee": deleted}, 200)
        except (ValueError, KeyError) as error:
            self._send_json_or_xml({"error": str(error)}, 400)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length < 1:
            raise ValueError("JSON body is required")
        body = self.rfile.read(length)
        value = json.loads(body.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        if "id" not in value:
            raise KeyError("id")
        return value

    def _send_json_or_xml(self, payload: dict, status: int) -> None:
        output_format = "json"
        parsed = urlparse(self.path)
        query_format = parse_qs(parsed.query).get("format", [""])[0].lower()
        if query_format in ("xml", "application/xml"):
            output_format = "xml"
        elif "xml" in self.headers.get("Accept", "").lower():
            output_format = "xml"

        if output_format == "xml":
            body = ET.tostring(
                _dict_to_xml("response", payload),
                encoding="utf-8",
                xml_declaration=True,
            )
            content_type = "application/xml; charset=utf-8"
        else:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            content_type = "application/json; charset=utf-8"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


class WarehouseHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: ThreadSafeWarehouse,
    ) -> None:
        super().__init__(address, WarehouseRequestHandler)
        self.store = store


def _dict_to_xml(name: str, value: object) -> ET.Element:
    element = ET.Element(name)
    if isinstance(value, dict):
        for key, item in value.items():
            element.append(_dict_to_xml(str(key), item))
    elif isinstance(value, list):
        for item in value:
            element.append(_dict_to_xml("item", item))
    elif value is not None:
        element.text = str(value).lower() if isinstance(value, bool) else str(value)
    return element


WEB_UI = r"""<!doctype html>
<html lang="ro">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Warehouse Web Proxy</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #f4f6f8; color: #172033; }
    main { max-width: 1000px; margin: auto; }
    .card { background: white; border-radius: 10px; padding: 18px; margin: 14px 0; box-shadow: 0 2px 10px #0001; }
    h1, h2 { margin-top: 0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; }
    input, button, select { font-size: 15px; padding: 9px; }
    button { cursor: pointer; border: 0; border-radius: 6px; background: #2563eb; color: white; }
    button.secondary { background: #475569; }
    button.danger { background: #dc2626; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; border-bottom: 1px solid #ddd; padding: 8px; }
    pre { background: #111827; color: #d1fae5; padding: 12px; overflow: auto; min-height: 80px; }
  </style>
</head>
<body>
<main>
  <h1>Warehouse Web Proxy</h1>
  <div class="card">
    <b>Arhitectura:</b> Client -> Reverse Proxy -> Warehouse(s)<br>
    <b>Load balancing:</b> Round-Robin &nbsp; <b>Caching:</b> TTL 5 secunde &nbsp; <b>Format:</b> JSON/XML
  </div>
  <div class="card">
    <h2>CRUD angajati</h2>
    <div class="grid">
      <input id="id" type="number" placeholder="ID">
      <input id="name" placeholder="Nume">
      <input id="role" placeholder="Rol">
      <button onclick="createEmployee()">Creare</button>
      <button class="secondary" onclick="loadEmployees()">Reincarcare</button>
      <button class="secondary" onclick="loadXml()">Afisare XML</button>
    </div>
    <table>
      <thead><tr><th>ID</th><th>Nume</th><th>Rol</th><th>Actiuni</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>
  <div class="card">
    <h2>Raspuns HTTP</h2>
    <pre id="output">Ready</pre>
  </div>
</main>
<script>
async function api(path, options = {}) {
  const response = await fetch(path, options);
  const text = await response.text();
  document.getElementById("output").textContent =
    "HTTP " + response.status + "\n" +
    "X-Cache: " + (response.headers.get("X-Cache") || "-") + "\n" +
    "X-Backend: " + (response.headers.get("X-Backend") || "-") + "\n" +
    text;
  if (!response.ok) throw new Error(text);
  return response.headers.get("Content-Type")?.includes("json") ? JSON.parse(text) : text;
}
async function loadEmployees() {
  try {
    const data = await api("/employees?offset=0&limit=100");
    document.getElementById("rows").innerHTML = data.employees.map(e => `
      <tr>
        <td>${e.id}</td><td>${escapeHtml(e.name)}</td><td>${escapeHtml(e.role || "")}</td>
        <td>
          <button class="secondary" onclick="editEmployee(${e.id})">Modificare</button>
          <button class="danger" onclick="deleteEmployee(${e.id})">Stergere</button>
        </td>
      </tr>`).join("");
  } catch (error) { document.getElementById("output").textContent = error.message; }
}
async function createEmployee() {
  const id = Number(document.getElementById("id").value);
  const name = document.getElementById("name").value;
  const role = document.getElementById("role").value;
  if (!Number.isInteger(id) || id < 1 || !name.trim()) {
    document.getElementById("output").textContent =
      "Completeaza un ID pozitiv si numele angajatului.";
    return;
  }
  try {
    await api("/employees", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({id, name: name.trim(), role: role.trim()})
    });
    document.getElementById("id").value = "";
    document.getElementById("name").value = "";
    document.getElementById("role").value = "";
    await loadEmployees();
  } catch (error) {
    document.getElementById("output").textContent = error.message;
  }
}
async function editEmployee(id) {
  const name = prompt("Nume:", document.getElementById("name").value);
  if (name === null) return;
  const role = prompt("Rol:", document.getElementById("role").value);
  if (role === null) return;
  await api("/employees/" + id, {method: "PUT", headers: {"Content-Type":"application/json"}, body: JSON.stringify({id, name, role})});
  await loadEmployees();
}
async function deleteEmployee(id) {
  if (!confirm("Stergi angajatul " + id + "?")) return;
  await api("/employees/" + id, {method: "DELETE"});
  await loadEmployees();
}
async function loadXml() {
  await api("/employees?format=xml");
}
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#039;" })[c]);
}
loadEmployees();
</script>
</body>
</html>
"""


@dataclass(frozen=True)
class CacheEntry:
    body: bytes
    content_type: str
    expires_at: float


class RoundRobinPool:
    def __init__(self, backends: list[tuple[str, int]]) -> None:
        if not backends:
            raise ValueError("at least one backend is required")
        self.backends = list(backends)
        self._next = 0
        self._lock = Lock()

    def choose(self) -> tuple[str, int]:
        with self._lock:
            backend = self.backends[self._next]
            self._next = (self._next + 1) % len(self.backends)
            return backend


class ConnectionPool:
    """Grupare simpla de conexiuni HTTP reuse pentru proxy -> warehouse."""

    def __init__(self) -> None:
        self._connections: dict[
            tuple[str, int], deque[HTTPConnection]
        ] = {}
        self._lock = Lock()

    def acquire(self, host: str, port: int) -> HTTPConnection:
        key = (host, port)
        with self._lock:
            available = self._connections.setdefault(key, deque())
            if available:
                return available.popleft()
        return HTTPConnection(host, port, timeout=5)

    def release(self, host: str, port: int, connection: HTTPConnection) -> None:
        with self._lock:
            self._connections.setdefault((host, port), deque()).append(connection)

    def close(self) -> None:
        with self._lock:
            for connections in self._connections.values():
                for connection in connections:
                    connection.close()
            self._connections.clear()


class ResponseCache:
    def __init__(self, ttl_seconds: float = 5.0) -> None:
        self.ttl_seconds = ttl_seconds
        self._entries: dict[str, CacheEntry] = {}
        self._lock = Lock()

    def get(self, key: str) -> CacheEntry | None:
        now = monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[key]
                return None
            return entry

    def put(self, key: str, body: bytes, content_type: str) -> None:
        with self._lock:
            self._entries[key] = CacheEntry(
                body=body,
                content_type=content_type,
                expires_at=monotonic() + self.ttl_seconds,
            )

    def invalidate(self) -> None:
        with self._lock:
            self._entries.clear()


class ReverseProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PAD-ReverseProxy/1.0"

    def do_GET(self) -> None:
        if urlparse(self.path).path.rstrip("/") == "":
            body = WEB_UI.encode("utf-8")
            self._send_response(
                200,
                body,
                "text/html; charset=utf-8",
                cache_status="-",
                backend=None,
            )
            return
        accept = self.headers.get("Accept", "application/json")
        cache_key = f"GET {self.path} Accept={accept}"
        cached = self.server.cache.get(cache_key)
        if cached is not None:
            self._send_response(
                200,
                cached.body,
                cached.content_type,
                cache_status="HIT",
                backend=None,
            )
            return
        self._forward(cache_key=cache_key)

    def do_PUT(self) -> None:
        self.server.cache.invalidate()
        self._forward()

    def do_POST(self) -> None:
        self.server.cache.invalidate()
        self._forward()

    def do_DELETE(self) -> None:
        self.server.cache.invalidate()
        self._forward()

    def log_message(self, format: str, *args: object) -> None:
        return

    def _forward(self, cache_key: str | None = None) -> None:
        host, port = self.server.backends.choose()
        connection = self.server.connections.acquire(host, port)
        body = None
        if self.command in ("POST", "PUT"):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""

        headers = {"Accept": self.headers.get("Accept", "application/json")}
        content_type = self.headers.get("Content-Type")
        if content_type:
            headers["Content-Type"] = content_type
        if body is not None:
            headers["Content-Length"] = str(len(body))

        try:
            connection.request(
                self.command,
                self.path,
                body=body,
                headers=headers,
            )
            response = connection.getresponse()
            response_body = response.read()
            response_content_type = response.getheader(
                "Content-Type", "application/json"
            )
            status = response.status
            if cache_key is not None and status == 200:
                self.server.cache.put(
                    cache_key,
                    response_body,
                    response_content_type,
                )
            self.server.connections.release(host, port, connection)
            self._send_response(
                status,
                response_body,
                response_content_type,
                cache_status="MISS" if cache_key is not None else "-",
                backend=f"{host}:{port}",
            )
        except Exception as error:
            connection.close()
            self._send_json_error(
                f"Warehouse unavailable: {host}:{port}",
                502,
                str(error),
            )

    def _send_response(
        self,
        status: int,
        body: bytes,
        content_type: str,
        cache_status: str,
        backend: str | None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Cache", cache_status)
        if backend:
            self.send_header("X-Backend", backend)
        self.end_headers()
        self.wfile.write(body)

    def _send_json_error(
        self,
        message: str,
        status: int,
        detail: str,
    ) -> None:
        body = json.dumps(
            {"error": message, "detail": detail},
            ensure_ascii=False,
        ).encode("utf-8")
        self._send_response(
            status,
            body,
            "application/json; charset=utf-8",
            cache_status="-",
            backend=None,
        )


class ReverseProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        backends: list[tuple[str, int]],
        cache_ttl_seconds: float = 5.0,
    ) -> None:
        super().__init__(address, ReverseProxyHandler)
        self.backends = RoundRobinPool(backends)
        self.connections = ConnectionPool()
        self.cache = ResponseCache(cache_ttl_seconds)

    def server_close(self) -> None:
        self.connections.close()
        super().server_close()
