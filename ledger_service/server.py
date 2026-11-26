"""财务本地 HTTP/JSON 入口。

仅使用 Python 标准库。默认绑定 ``127.0.0.1:8787``，用 ``ThreadingHTTPServer``
并行接收请求；真正的账务串行化由 SQLite ``BEGIN IMMEDIATE`` 完成。
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import LedgerError
from .ledger import Ledger


def http_status_for_result(result: dict[str, Any]) -> int:
    if result.get("ok"):
        return 200
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    code = error.get("code") or result.get("code") or ""
    if code == "unauthorized_close":
        return 403
    if code == "not_found":
        return 404
    if code in {"validation_error", "currency_mismatch", "voucher_not_balanced"}:
        return 422
    if code in {
        "conflict",
        "idempotency_conflict",
        "period_conflict",
        "close_not_ready",
        "period_already_closed",
        "period_not_closed",
        "refund_limit_exceeded",
    }:
        return 409
    return 400


class _LedgerHandler(BaseHTTPRequestHandler):
    ledger: Ledger

    server_version = "ServiceCouponLedger/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        # 本地服务保留简洁访问日志；错误仍会由异常响应解释。
        super().log_message("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LedgerError(
                "请求体不是合法 JSON",
                code="invalid_json",
                http_status=400,
                details={"error": str(exc)},
            ) from exc
        if not isinstance(data, dict):
            raise LedgerError(
                "请求体必须是 JSON 对象",
                code="invalid_request",
                http_status=400,
            )
        return data

    def _handle_domain(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        try:
            result = fn(*args, **kwargs)
            if isinstance(result, dict) and "ok" not in result:
                result = {"ok": True, **result}
            self._send_json(http_status_for_result(result), result)
        except LedgerError as exc:
            self._send_json(exc.http_status, exc.to_dict())

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        parts = [part for part in path.split("/") if part]

        if path in {"/", "/v1", "/v1/"}:
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "service-coupon-ledger",
                    "endpoints": [
                        "GET  /health",
                        "POST /v1/actions",
                        "POST /v1/clearing-requests",
                        "POST /v1/clearing-requests/{request_id}/confirm",
                        "POST /v1/clearing-reports",
                        "POST /v1/refunds",
                        "POST /v1/refunds/{refund_id}/retry",
                        "POST /v1/reversals",
                        "POST /v1/close",
                        "POST /v1/periods/{org}/{month}/close",
                        "GET  /v1/periods/{org}/{month}/net",
                        "GET  /v1/periods/{org}/{month}/certificate",
                        "GET  /v1/services/{ref}",
                        "GET  /v1/vouchers/{voucher_id}",
                        "GET  /v1/recovery",
                    ],
                },
            )
            return
        if path == "/health":
            self._send_json(200, {"ok": True, "service": "ledger", "database": self.ledger.database})
            return
        if path == "/v1/recovery":
            self._handle_domain(self.ledger.recovery)
            return

        try:
            if len(parts) >= 4 and parts[0] == "v1" and parts[1] == "periods":
                org, month = parts[2], parts[3]
                if len(parts) == 4:
                    self._handle_domain(self.ledger.get_period, org, month)
                    return
                if len(parts) == 5 and parts[4] == "net":
                    include_chain = query.get("chain", ["1"])[0] not in {"0", "false", "False"}
                    self._handle_domain(
                        self.ledger.period_net, org, month, include_chain=include_chain
                    )
                    return
                if len(parts) == 5 and parts[4] in {"certificate", "closed", "credential"}:
                    self._handle_domain(self.ledger.read_closed_certificate, org, month)
                    return
            if len(parts) == 3 and parts[0] == "v1" and parts[1] == "services":
                self._handle_domain(self.ledger.service_chain, parts[2])
                return
            if len(parts) == 3 and parts[0] == "v1" and parts[1] == "vouchers":
                self._handle_domain(self.ledger.get_voucher, parts[2])
                return
            if len(parts) == 3 and parts[0] == "v1" and parts[1] == "requests":
                self._handle_domain(self.ledger.get_request, parts[2])
                return
        except LedgerError as exc:
            self._send_json(exc.http_status, exc.to_dict())
            return

        self._send_json(404, {"ok": False, "error": {"code": "route_not_found", "path": path}})

    def do_POST(self) -> None:  # noqa: N802
        try:
            data = self._read_json()
        except LedgerError as exc:
            self._send_json(exc.http_status, exc.to_dict())
            return

        parsed = urlparse(self.path)
        parts = [part for part in parsed.path.split("/") if part]

        if parsed.path == "/v1/actions":
            action = data.get("action")
            args = data.get("args", [])
            kwargs = data.get("kwargs", {})
            result = self.ledger.handle_action(action, args, **(kwargs if isinstance(kwargs, dict) else {}))
            self._send_json(http_status_for_result(result), result)
            return

        if parsed.path == "/v1/clearing-requests":
            self._handle_domain(
                self.ledger.receive_clearing,
                data.get("source"),
                data.get("source_seq", data.get("seq")),
                data.get("request_id", data.get("id")),
                data.get("month"),
                data.get("currency"),
                data.get("allocations"),
                auto_confirm=bool(data.get("auto_confirm", False)),
                confirm_options=data.get("confirm_options"),
            )
            return

        if (
            len(parts) == 4
            and parts[:2] == ["v1", "clearing-requests"]
            and parts[3] == "confirm"
        ):
            self._handle_domain(
                self.ledger.confirm_clearing,
                parts[2],
                report_id=data.get("report_id"),
                outcome=data.get("outcome", "cleared"),
                report=data.get("report"),
                period_policy=data.get("period_policy", "error"),
                target_month=data.get("target_month"),
            )
            return

        if parsed.path == "/v1/clearing-reports":
            request_id = data.get("request_id")
            self._handle_domain(
                self.ledger.confirm_clearing,
                request_id,
                report_id=data.get("report_id"),
                outcome=data.get("outcome", "cleared"),
                report=data,
                period_policy=data.get("period_policy", "error"),
                target_month=data.get("target_month"),
            )
            return

        if parsed.path == "/v1/refunds":
            self._handle_domain(
                self.ledger.create_refund,
                data.get("refund_id", data.get("id")),
                data.get("original_voucher_id", data.get("original")),
                data.get("amount"),
                data.get("month"),
                currency=data.get("currency"),
                lines=data.get("lines"),
                period_policy=data.get("period_policy", "error"),
                target_month=data.get("target_month"),
            )
            return

        if parsed.path == "/v1/reversals":
            amount = data.get("amount")
            self._handle_domain(
                self.ledger.reverse_voucher,
                data.get("reversal_id", data.get("id")),
                data.get("original_voucher_id", data.get("original")),
                data.get("month"),
                amount=amount,
                currency=data.get("currency"),
                lines=data.get("lines"),
                period_policy=data.get("period_policy", "error"),
                target_month=data.get("target_month"),
            )
            return

        if len(parts) == 4 and parts[:2] == ["v1", "refunds"] and parts[3] == "retry":
            overrides = {
                key: data[key]
                for key in ("period_policy", "target_month")
                if key in data
            }
            self._handle_domain(self.ledger.retry_refund, parts[2], **overrides)
            return

        if parsed.path == "/v1/close":
            self._handle_domain(
                self.ledger.close_period,
                data.get("actor"),
                data.get("org"),
                data.get("month"),
                data.get("cuts"),
                idempotency_key=data.get("idempotency_key"),
                phase=data.get("phase", "close"),
            )
            return

        if len(parts) == 5 and parts[:2] == ["v1", "periods"] and parts[4] == "close":
            self._handle_domain(
                self.ledger.close_period,
                data.get("actor"),
                parts[2],
                parts[3],
                data.get("cuts"),
                idempotency_key=data.get("idempotency_key"),
                phase=data.get("phase", "close"),
            )
            return

        if parsed.path in {"/v1/recovery", "/v1/restart"}:
            self._handle_domain(self.ledger.recovery, component=data.get("component"))
            return

        self._send_json(404, {"ok": False, "error": {"code": "route_not_found", "path": parsed.path}})


def create_server(
    database: str = "ledger.db",
    host: str = "127.0.0.1",
    port: int = 8787,
) -> ThreadingHTTPServer:
    ledger = Ledger(database)
    handler_class = type("ConfiguredLedgerHandler", (_LedgerHandler,), {"ledger": ledger})
    server = ThreadingHTTPServer((host, int(port)), handler_class)
    server.daemon_threads = True
    return server


def serve(database: str = "ledger.db", host: str = "127.0.0.1", port: int = 8787) -> None:
    server = create_server(database, host, port)
    print(f"ledger API listening on http://{host}:{port} (database={database})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
