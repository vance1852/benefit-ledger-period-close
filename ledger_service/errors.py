"""账本服务的领域异常。

异常都带稳定的 ``code`` 和可 JSON 序列化的 ``details``，HTTP 层据此设置状态码，
命令/重放层则把它们转成财务能读懂的拒绝原因。
"""

from __future__ import annotations

from typing import Any, Mapping


class LedgerError(Exception):
    """所有可预期账本错误的基类。"""

    code = "ledger_error"
    http_status = 400

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        code: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details: dict[str, Any] = dict(details or {})
        if code:
            self.code = code
        if http_status is not None:
            self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


class ValidationError(LedgerError):
    code = "validation_error"
    http_status = 422


class NotFoundError(LedgerError):
    code = "not_found"
    http_status = 404


class ConflictError(LedgerError):
    code = "conflict"
    http_status = 409


class UnauthorizedCloseError(LedgerError):
    code = "unauthorized_close"
    http_status = 403


class IdempotencyConflictError(ConflictError):
    code = "idempotency_conflict"


class BalanceError(ValidationError):
    code = "voucher_not_balanced"


class CurrencyMismatchError(ValidationError):
    code = "currency_mismatch"


class PeriodConflictError(ConflictError):
    code = "period_conflict"


class CloseNotReadyError(ConflictError):
    code = "close_not_ready"


class AlreadyClosedError(ConflictError):
    code = "period_already_closed"


class RefundLimitError(ConflictError):
    code = "refund_limit_exceeded"
