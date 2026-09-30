"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .hub_service import HubService
from .service import DomainService
from .storage import Database


def _hub(service: DomainService) -> HubService:
    return HubService(service.database, service.clock)


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            query = parse_qs(parsed.query)
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}

        hub = _hub(service)
        query = parse_qs(parsed.query)

        def q(name: str, default: str = "") -> str:
            return query.get(name, [default])[0]

        if method == "POST" and parsed.path == "/hub/resources":
            result = hub.register_resource(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/contracts":
            result = hub.register_contract(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/batches":
            result = hub.register_batch(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/shipments":
            result = hub.register_shipment(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/blockades":
            result = hub.register_blockade(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/events":
            result = hub.ingest_event(actor_id=actor_id, site_id=body.pop("site_id", ""),
                                      event=body.pop("event", body),
                                      request_id=body.pop("request_id", ""))
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/plans":
            result = hub.create_plan(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/plans/confirm":
            result = hub.confirm_plan(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 200, result
        if method == "POST" and parsed.path == "/hub/plans/commit":
            result = hub.commit_plan(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/hub/rate-freezes":
            result = hub.freeze_rate(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/hub/plans":
            return 200, hub.get_plan(q("plan_id"))
        if method == "GET" and parsed.path == "/hub/shipments":
            return 200, {"items": hub.list_shipments(q("site_id"))}
        if method == "GET" and parsed.path == "/hub/shipment":
            return 200, hub.get_shipment(q("site_id"), q("shipment_id"))
        if method == "GET" and parsed.path == "/hub/timeline":
            return 200, {"items": hub.timeline(q("site_id"))}
        if method == "GET" and parsed.path == "/hub/conservation":
            return 200, hub.conservation(q("site_id"))
        if method == "GET" and parsed.path == "/hub/rate":
            return 200, hub.compute_rate(q("site_id"), q("as_of") or None)
        if method == "GET" and parsed.path == "/hub/rate-freezes":
            return 200, {"items": hub.list_freezes(q("site_id"))}
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动技能赛训协作基础服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
