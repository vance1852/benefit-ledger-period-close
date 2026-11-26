"""SQLite 账本核心：申请收件、清算确认、过账、关账、红冲和查询。

设计要点：

* 所有写事务使用 ``BEGIN IMMEDIATE``，把“检查余额/期间 + 写入”串行化；
* 一张凭证的全部分录在同一事务内写入，外部不可能读到半截双边账；
* 已过账凭证和已发结账凭据只追加、不重写；更正通过新的红冲/调整凭证表达；
* 方法通常返回普通 ``dict``，可直接 JSON 序列化；可预期领域错误抛
  :class:`~ledger_service.errors.LedgerError` 子类。
"""

from __future__ import annotations

from contextlib import contextmanager
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterator, Mapping, Sequence

from .errors import (
    AlreadyClosedError,
    BalanceError,
    CloseNotReadyError,
    ConflictError,
    CurrencyMismatchError,
    IdempotencyConflictError,
    LedgerError,
    NotFoundError,
    PeriodConflictError,
    RefundLimitError,
    UnauthorizedCloseError,
    ValidationError,
)
from .schema import LEDGER_SCHEMA, SCHEMA_VERSION
from .utils import canonical_json, fingerprint, largest_remainder, token

GENESIS_HASH = "0" * 64


def connect_database(database: str | Path, **kwargs: Any) -> sqlite3.Connection:
    """打开一个配置好 WAL、忙等待和外键的 SQLite 连接。"""

    target_text = str(database)
    is_uri = target_text.startswith("file:")
    if target_text == ":memory:":
        target = ":memory:"
    elif is_uri:
        target = target_text
    else:
        path = Path(database)
        path.parent.mkdir(parents=True, exist_ok=True)
        target = str(path)

    db = sqlite3.connect(
        target,
        timeout=float(kwargs.pop("timeout", 30.0)),
        isolation_level=None,
        uri=is_uri,
        **kwargs,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA foreign_keys=ON")
    try:
        # WAL 是数据库级持久属性；内存库不支持时忽略。
        db.execute("PRAGMA journal_mode=WAL")
    except sqlite3.DatabaseError:
        pass
    db.execute("PRAGMA synchronous=NORMAL")
    return db


class Ledger:
    """账本服务的进程内门面。

    对文件数据库，每个操作使用独立短连接，因此同一个 ``Ledger`` 可被多线程共享；
    SQLite 的写锁加上 ``BEGIN IMMEDIATE`` 负责并发正确性。
    """

    def __init__(self, database: str | Path = "ledger.db") -> None:
        requested = str(database)
        if requested == ":memory:":
            # 单一内存连接 + 可重入锁，避免共享缓存内存库的表锁差异；
            # 文件库仍走每操作独立连接和 SQLite WAL 并行读写。
            self.database = ":memory:"
            self._memory_db: sqlite3.Connection | None = connect_database(
                ":memory:", check_same_thread=False
            )
            self._memory_lock = threading.RLock()
        else:
            self.database = requested
            self._memory_db = None
            self._memory_lock = None
        self.initialize()

    # ------------------------------------------------------------------
    # 连接和基础结构
    # ------------------------------------------------------------------

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """提供 SQLite 连接；文件库短连接在结束后关闭。"""

        if self._memory_db is not None and self._memory_lock is not None:
            with self._memory_lock:
                yield self._memory_db
            return
        db = connect_database(self.database)
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def immediate(self, db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        """运行由 SQLite 写锁串行化的写事务。"""

        db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            db.execute("COMMIT")
        except BaseException:
            try:
                db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def initialize(self) -> None:
        with self.session() as db:
            db.executescript(LEDGER_SCHEMA)
            db.execute(
                "INSERT OR IGNORE INTO le_meta(key, value) VALUES('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            self._migrate(db)
            self._import_opening_entries(db)

    @staticmethod
    def _migrate(db: sqlite3.Connection) -> None:
        """对早期开发中的本地库做最轻量的追加列迁移。"""

        columns = {
            row[1]
            for row in db.execute("PRAGMA table_info(le_refund)").fetchall()
        }
        if columns and "kind" not in columns:
            db.execute(
                "ALTER TABLE le_refund ADD COLUMN kind TEXT NOT NULL DEFAULT 'refund'"
            )

    def upsert_account(self, account_id: str, org: str, kind: str, currency: str) -> dict[str, Any]:
        with self.session() as db, self.immediate(db):
            db.execute(
                """INSERT INTO account(id, org, kind, currency)
                   VALUES(?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     org=excluded.org, kind=excluded.kind, currency=excluded.currency""",
                (account_id, org, kind, currency.upper()),
            )
        return self.get_account(account_id)

    def get_account(self, account_id: str) -> dict[str, Any]:
        with self.session() as db:
            row = db.execute("SELECT * FROM account WHERE id=?", (account_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"账户不存在：{account_id}", details={"account": account_id})
            return dict(row)

    def upsert_period(
        self, org: str, month: str, state: str = "open", actor: str | None = None
    ) -> dict[str, Any]:
        if state not in {"open", "closing", "closed"}:
            raise ValidationError("期间状态只能是 open/closing/closed")
        with self.session() as db, self.immediate(db):
            db.execute(
                """INSERT INTO period(org, month, state, actor)
                   VALUES(?, ?, ?, ?)
                   ON CONFLICT(org, month) DO UPDATE SET
                     state=excluded.state, actor=COALESCE(excluded.actor, period.actor)""",
                (org, month, state, actor),
            )
        return self.get_period(org, month)

    def get_period(self, org: str, month: str) -> dict[str, Any]:
        with self.session() as db:
            row = db.execute(
                "SELECT * FROM period WHERE org=? AND month=?", (org, month)
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"期间未配置：{org} {month}",
                    details={"org": org, "month": month},
                )
            return dict(row)

    def set_source_cut(self, source: str, final_through: int) -> dict[str, Any]:
        if not isinstance(final_through, int) or isinstance(final_through, bool) or final_through < 0:
            raise ValidationError("final_through 必须是非负整数")
        with self.session() as db, self.immediate(db):
            db.execute(
                """INSERT INTO source_cuts(source, final_through)
                   VALUES(?, ?)
                   ON CONFLICT(source) DO UPDATE SET final_through=excluded.final_through""",
                (source, final_through),
            )
        return {"source": source, "final_through": final_through}

    def _import_opening_entries(self, db: sqlite3.Connection) -> None:
        # 原子导入，避免并发首次初始化或坏数据留下半截期初凭证。
        with self.immediate(db):
            self._import_opening_entries_tx(db)

    def _import_opening_entries_tx(self, db: sqlite3.Connection) -> None:
        done = db.execute(
            "SELECT value FROM le_meta WHERE key='openings_imported'"
        ).fetchone()
        if done is not None:
            return

        exists = db.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='opening_entries'"
        ).fetchone()
        if exists is None:
            db.execute(
                "INSERT OR IGNORE INTO le_meta(key, value) VALUES('openings_imported', '1')"
            )
            return

        rows = db.execute(
            "SELECT rowid AS rid, * FROM opening_entries ORDER BY rowid"
        ).fetchall()
        groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            groups[row["voucher"]].append(row)

        # 按凭证号排序使哈希链初始化确定。
        for voucher_id in sorted(groups):
            raw_rows = groups[voucher_id]
            lines: list[dict[str, Any]] = []
            totals: dict[str, int] = defaultdict(int)
            months: set[str] = set()
            for row in sorted(raw_rows, key=lambda r: r["rid"]):
                account = db.execute(
                    "SELECT * FROM account WHERE id=?", (row["account"],)
                ).fetchone()
                currency = row["currency"] or (account["currency"] if account else None)
                org = account["org"] if account else None
                amount = row["amount"]
                if not isinstance(amount, int):
                    raise ValidationError(f"期初分录金额不是整数：{voucher_id}")
                lines.append(
                    {
                        "account": row["account"],
                        "org": org,
                        "month": row["month"],
                        "currency": currency,
                        "amount": amount,
                    }
                )
                totals[currency] += amount
                months.add(row["month"])
            unbalanced = {c: v for c, v in totals.items() if v != 0}
            if unbalanced:
                raise BalanceError(
                    f"期初凭证 {voucher_id} 未按币种平衡",
                    details={"voucher_id": voucher_id, "currency_totals": dict(totals)},
                )
            currencies = set(totals)
            header_currency = next(iter(currencies)) if len(currencies) == 1 else "MULTI"
            effective_month = next(iter(months)) if len(months) == 1 else "MULTIPLE"
            self._insert_voucher(
                db,
                voucher_id=voucher_id,
                kind="opening",
                lines=lines,
                currency=header_currency,
                source="opening",
                source_seq=None,
                requested_month=effective_month,
                effective_month=effective_month,
                original_voucher_id=None,
                period_policy="import",
                idempotency_key=f"opening:{voucher_id}",
                payload={"opening_entries": [dict(r) for r in raw_rows]},
            )

        db.execute(
            "INSERT OR IGNORE INTO le_meta(key, value) VALUES('openings_imported', '1')"
        )

    # ------------------------------------------------------------------
    # 序列化和凭证哈希链
    # ------------------------------------------------------------------

    @staticmethod
    def _loads(value: str | None, default: Any) -> Any:
        if value is None or value == "":
            return default
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return default

    def _voucher_dict(
        self, db: sqlite3.Connection, row: sqlite3.Row | str
    ) -> dict[str, Any]:
        if isinstance(row, str):
            fetched = db.execute(
                "SELECT * FROM le_voucher WHERE voucher_id=?", (row,)
            ).fetchone()
            if fetched is None:
                raise NotFoundError(f"凭证不存在：{row}", details={"voucher_id": row})
            row = fetched

        result = dict(row)
        result["payload"] = self._loads(result.pop("payload_json", None), {})
        entries = db.execute(
            "SELECT * FROM le_entry WHERE voucher_id=? ORDER BY line_no, id",
            (result["voucher_id"],),
        ).fetchall()
        result["entries"] = [dict(entry) for entry in entries]
        totals: dict[str, int] = defaultdict(int)
        for entry in result["entries"]:
            totals[entry["currency"]] += entry["amount"]
        result["currency_totals"] = dict(totals)
        result["balanced"] = all(value == 0 for value in totals.values())
        return result

    def get_voucher(self, voucher_id: str) -> dict[str, Any]:
        with self.session() as db:
            return self._voucher_dict(db, voucher_id)

    def verify_chain(self) -> dict[str, Any]:
        """重算凭证哈希链，返回是否完整以及链尖。"""

        with self.session() as db:
            rows = db.execute(
                "SELECT * FROM le_voucher ORDER BY chain_seq"
            ).fetchall()
            previous = GENESIS_HASH
            for row in rows:
                if row["prev_hash"] != previous:
                    return {
                        "ok": False,
                        "broken_at": row["voucher_id"],
                        "reason": "prev_hash_mismatch",
                    }
                body = {
                    key: row[key]
                    for key in (
                        "voucher_id",
                        "kind",
                        "source",
                        "source_seq",
                        "requested_month",
                        "effective_month",
                        "currency",
                        "original_voucher_id",
                    )
                }
                entries = [
                    dict(entry)
                    for entry in db.execute(
                        "SELECT account, line_no, org, month, currency, amount FROM le_entry "
                        "WHERE voucher_id=? ORDER BY line_no",
                        (row["voucher_id"],),
                    )
                ]
                expected_hash = fingerprint(
                    {"chain_seq": row["chain_seq"], "prev_hash": previous, "voucher": body, "entries": entries}
                )
                if expected_hash != row["voucher_hash"]:
                    return {
                        "ok": False,
                        "broken_at": row["voucher_id"],
                        "reason": "voucher_hash_mismatch",
                    }
                previous = row["voucher_hash"]
            return {
                "ok": True,
                "voucher_count": len(rows),
                "chain_tip": previous,
            }

    def _insert_voucher(
        self,
        db: sqlite3.Connection,
        *,
        voucher_id: str,
        kind: str,
        lines: Sequence[Mapping[str, Any]],
        currency: str,
        source: str | None = None,
        source_seq: int | None = None,
        fingerprint_value: str | None = None,
        requested_month: str | None = None,
        effective_month: str | None = None,
        original_voucher_id: str | None = None,
        period_policy: str = "error",
        idempotency_key: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        existing = db.execute(
            "SELECT voucher_id FROM le_voucher WHERE voucher_id=?", (voucher_id,)
        ).fetchone()
        if existing is not None:
            raise IdempotencyConflictError(
                f"凭证号已经存在：{voucher_id}",
                details={"voucher_id": voucher_id},
            )

        chain_row = db.execute(
            """SELECT COALESCE(MAX(chain_seq), 0) AS max_seq,
                      (SELECT voucher_hash FROM le_voucher ORDER BY chain_seq DESC LIMIT 1) AS tip
               FROM le_voucher"""
        ).fetchone()
        chain_seq = int(chain_row["max_seq"]) + 1
        previous_hash = chain_row["tip"] or GENESIS_HASH

        ordered_lines = sorted(
            (dict(line) for line in lines),
            key=lambda line: (str(line.get("account")), int(line.get("line_no", 0))),
        )
        for index, line in enumerate(ordered_lines, start=1):
            line["line_no"] = index

        hash_body = {
            "chain_seq": chain_seq,
            "prev_hash": previous_hash,
            "voucher": {
                "voucher_id": voucher_id,
                "kind": kind,
                "source": source,
                "source_seq": source_seq,
                "currency": currency,
                "requested_month": requested_month,
                "effective_month": effective_month,
                "original_voucher_id": original_voucher_id,
            },
            "entries": [
                {
                    "line_no": line["line_no"],
                    "account": line["account"],
                    "org": line.get("org"),
                    "month": line["month"],
                    "currency": line["currency"],
                    "amount": line["amount"],
                }
                for line in ordered_lines
            ],
        }
        voucher_hash = fingerprint(hash_body)
        effective_month = effective_month or requested_month or "MULTIPLE"
        fp = fingerprint_value or fingerprint(payload or hash_body["voucher"])

        db.execute(
            """INSERT INTO le_voucher(
                   voucher_id, kind, source, source_seq, idempotency_key, fingerprint,
                   requested_month, effective_month, currency, original_voucher_id,
                   period_policy, status, chain_seq, prev_hash, voucher_hash, payload_json
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'posted', ?, ?, ?, ?)""",
            (
                voucher_id,
                kind,
                source,
                source_seq,
                idempotency_key,
                fp,
                requested_month,
                effective_month,
                currency,
                original_voucher_id,
                period_policy,
                chain_seq,
                previous_hash,
                voucher_hash,
                canonical_json(payload or {}),
            ),
        )
        for line in ordered_lines:
            db.execute(
                """INSERT INTO le_entry(
                       voucher_id, line_no, account, org, month, currency, amount
                   ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                (
                    voucher_id,
                    line["line_no"],
                    line["account"],
                    line.get("org"),
                    line["month"],
                    line["currency"],
                    int(line["amount"]),
                ),
            )
        return voucher_id

    # ------------------------------------------------------------------
    # 校验和期间冲突
    # ------------------------------------------------------------------

    def _allocation_lines(
        self,
        db: sqlite3.Connection,
        allocations: Mapping[str, Any],
        currency: str,
        month: str,
    ) -> list[dict[str, Any]]:
        if not isinstance(allocations, Mapping) or len(allocations) == 0:
            raise ValidationError("分摊必须是非空对象：账户 -> 整数分金额")

        lines: list[dict[str, Any]] = []
        totals: dict[str, int] = defaultdict(int)
        problems: list[dict[str, Any]] = []

        for raw_account, raw_amount in allocations.items():
            account_id = str(raw_account)
            try:
                amount = raw_amount
                if not isinstance(amount, int) or isinstance(amount, bool):
                    raise ValueError("必须是整数分")
            except ValueError:
                problems.append(
                    {"account": account_id, "code": "invalid_amount", "value": raw_amount}
                )
                continue

            account = db.execute(
                "SELECT * FROM account WHERE id=?", (account_id,)
            ).fetchone()
            if account is None:
                problems.append(
                    {"account": account_id, "code": "unknown_account", "amount": amount}
                )
                continue

            line_currency = account["currency"]
            if line_currency != currency:
                problems.append(
                    {
                        "account": account_id,
                        "code": "account_currency_mismatch",
                        "account_currency": line_currency,
                        "voucher_currency": currency,
                        "amount": amount,
                    }
                )
            totals[line_currency] += amount
            lines.append(
                {
                    "account": account_id,
                    "org": account["org"],
                    "kind": account["kind"],
                    "month": month,
                    "currency": line_currency,
                    "amount": amount,
                }
            )

        if problems:
            raise CurrencyMismatchError(
                "分摊账户、金额或币种不合法",
                details={"problems": problems, "currency_totals": dict(totals)},
            )

        unbalanced = {cur: total for cur, total in totals.items() if total != 0}
        if unbalanced:
            raise BalanceError(
                "凭证未按币种保持借贷平衡",
                details={
                    "currency_totals": dict(totals),
                    "unbalanced": unbalanced,
                    "hint": "正数为借、负数为贷；每个币种的合计必须为 0",
                },
            )

        if all(line["amount"] == 0 for line in lines):
            raise ValidationError("凭证金额不能全部为 0", details={"currency": currency})
        return lines

    def _cut_covers_source_position(
        self,
        db: sqlite3.Connection,
        org: str,
        month: str,
        source: str | None,
        source_seq: int | None,
    ) -> bool:
        if not source or source_seq is None:
            return False
        job = db.execute(
            "SELECT cuts_json FROM le_close_job WHERE org=? AND month=?",
            (org, month),
        ).fetchone()
        if job is None:
            return False
        cuts = self._loads(job["cuts_json"], {})
        through = cuts.get(source)
        return isinstance(through, int) and not isinstance(through, bool) and source_seq <= through

    def _common_open_month(
        self, db: sqlite3.Connection, orgs: Sequence[str], after_month: str
    ) -> str | None:
        valid_orgs = sorted({org for org in orgs if org})
        if not valid_orgs:
            return None
        placeholders = ",".join("?" for _ in valid_orgs)
        rows = db.execute(
            f"""SELECT org, month FROM period
                WHERE state='open' AND month > ?
                  AND org IN ({placeholders})
                ORDER BY month""",
            (after_month, *valid_orgs),
        ).fetchall()
        months: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            months[row["month"]].add(row["org"])
        org_set = set(valid_orgs)
        for candidate in sorted(months):
            if months[candidate] == org_set:
                return candidate
        return None

    def _resolve_effective_month(
        self,
        db: sqlite3.Connection,
        lines: list[dict[str, Any]],
        requested_month: str,
        *,
        policy: str = "error",
        target_month: str | None = None,
        source: str | None = None,
        source_seq: int | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        orgs = sorted({line["org"] for line in lines if line.get("org")})

        def inspect(month: str, allow_cutoff: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            states: list[dict[str, Any]] = []
            conflicts: list[dict[str, Any]] = []
            for org in orgs:
                row = db.execute(
                    "SELECT org, month, state, actor FROM period WHERE org=? AND month=?",
                    (org, month),
                ).fetchone()
                if row is None:
                    state = "missing"
                    actor = None
                    covered = False
                else:
                    state = row["state"]
                    actor = row["actor"]
                    covered = (
                        allow_cutoff
                        and state == "closing"
                        and self._cut_covers_source_position(
                            db, org, month, source, source_seq
                        )
                    )
                item = {
                    "org": org,
                    "month": month,
                    "state": state,
                    "actor": actor,
                    "close_cutoff_allows_this_request": covered,
                }
                states.append(item)
                if state != "open" and not covered:
                    if state == "closed":
                        code = "period_closed"
                    elif state == "closing":
                        code = "period_closing_without_matching_cut"
                    else:
                        code = "period_not_configured"
                    conflicts.append({**item, "code": code})
            return conflicts, states

        candidate = target_month or requested_month
        conflicts, states = inspect(candidate, allow_cutoff=(target_month is None))
        if not conflicts:
            for line in lines:
                line["month"] = candidate
            return candidate, states

        suggestion = self._common_open_month(db, orgs, requested_month)
        details = {
            "requested_month": requested_month,
            "candidate_month": candidate,
            "periods": states,
            "conflicts": conflicts,
            "next_common_open_period": suggestion,
            "remediation": (
                "整笔凭证必须一起改到所有机构共用的 open 期间；可使用 "
                "period_policy='next_common_open' 补办，不能只过其中一边。"
            ),
        }

        if policy == "next_common_open" and suggestion:
            second_conflicts, second_states = inspect(suggestion, allow_cutoff=False)
            if not second_conflicts:
                for line in lines:
                    line["month"] = suggestion
                return suggestion, second_states
            details["next_period_conflicts"] = second_conflicts

        raise PeriodConflictError(
            f"凭证不能完整计入 {candidate}：存在正在关、已关或未配置的机构期间",
            details=details,
        )

    # ------------------------------------------------------------------
    # 清算申请收件和清算回报确认
    # ------------------------------------------------------------------

    def receive_clearing(
        self,
        source: str,
        source_seq: int,
        request_id: str,
        month: str,
        currency: str,
        allocations: Mapping[str, int],
        *,
        auto_confirm: bool = False,
        confirm_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(source_seq, int) or isinstance(source_seq, bool) or source_seq < 0:
            raise ValidationError("流内序号 source_seq 必须是非负整数")
        source = str(source)
        request_id = str(request_id)
        month = str(month)
        currency = str(currency).upper()
        business_payload = {
            "source": source,
            "source_seq": source_seq,
            "request_id": request_id,
            "month": month,
            "currency": currency,
            "allocations": allocations,
        }
        fp = fingerprint(business_payload)

        with self.session() as db:
            validation_error: ValidationError | None = None
            try:
                self._allocation_lines(db, allocations, currency, month)
            except ValidationError as exc:
                validation_error = exc

            outcome: dict[str, Any] | None = None
            error_to_raise: LedgerError | None = None
            with self.immediate(db):
                existing = db.execute(
                    "SELECT * FROM le_request WHERE source=? AND source_seq=?",
                    (source, source_seq),
                ).fetchone()
                if existing is not None:
                    if existing["fingerprint"] != fp:
                        error_to_raise = IdempotencyConflictError(
                            "相同清算流位置收到了金额或分摊不同的另一笔请求；旧回执不能遮盖新载荷",
                            details={
                                "source": source,
                                "source_seq": source_seq,
                                "existing_request_id": existing["request_id"],
                                "incoming_request_id": request_id,
                                "existing_fingerprint": existing["fingerprint"],
                                "incoming_fingerprint": fp,
                                "existing_status": existing["status"],
                                "required_action": (
                                    "若为更正，请使用新的请求编号并显式关联/冲正；"
                                    "不得把它当作原请求的幂等重发。"
                                ),
                            },
                        )
                    else:
                        db.execute(
                            """UPDATE le_request
                               SET received_count=received_count+1,
                                   updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                               WHERE source=? AND source_seq=?""",
                            (source, source_seq),
                        )
                        if existing["status"] == "rejected":
                            # 终态拒绝的幂等重发必须返回同一份失败，不能用
                            # ok:true/status:rejected 伪装成已受理成功。
                            error_to_raise = self._error_from_request(
                                existing, idempotent_replay=True
                            )
                        else:
                            outcome = self._request_result(
                                db, existing["request_id"], idempotent_replay=True
                            )
                            if existing["status"] == "blocked":
                                error = self._error_from_request(
                                    existing, idempotent_replay=True
                                )
                                outcome["ok"] = False
                                outcome["error"] = error.to_dict()["error"]
                else:
                    by_request_id = db.execute(
                        "SELECT * FROM le_request WHERE request_id=?", (request_id,)
                    ).fetchone()
                    if by_request_id is not None:
                        error_to_raise = IdempotencyConflictError(
                            "业务请求编号已被其他清算流位置使用",
                            details={
                                "request_id": request_id,
                                "existing_source": by_request_id["source"],
                                "existing_seq": by_request_id["source_seq"],
                                "incoming_source": source,
                                "incoming_seq": source_seq,
                            },
                        )
                    else:
                        voucher = db.execute(
                            "SELECT voucher_id FROM le_voucher WHERE voucher_id=?",
                            (request_id,),
                        ).fetchone()
                        if voucher is not None:
                            error_to_raise = IdempotencyConflictError(
                                "业务请求编号已经是已过账凭证号",
                                details={"request_id": request_id},
                            )
                        elif validation_error is not None:
                            details = dict(validation_error.details)
                            details["request_id"] = request_id
                            db.execute(
                                """INSERT INTO le_request(
                                       source, source_seq, request_id, month, currency,
                                       fingerprint, payload_json, status,
                                       error_code, error_message, error_details_json
                                   ) VALUES(?, ?, ?, ?, ?, ?, ?, 'rejected', ?, ?, ?)""",
                                (
                                    source,
                                    source_seq,
                                    request_id,
                                    month,
                                    currency,
                                    fp,
                                    canonical_json(business_payload),
                                    validation_error.code,
                                    validation_error.message,
                                    canonical_json(details),
                                ),
                            )
                            error_to_raise = validation_error
                            validation_error.details = details
                        else:
                            db.execute(
                                """INSERT INTO le_request(
                                       source, source_seq, request_id, month, currency,
                                       fingerprint, payload_json, status
                                   ) VALUES(?, ?, ?, ?, ?, ?, ?, 'received_unverified')""",
                                (
                                    source,
                                    source_seq,
                                    request_id,
                                    month,
                                    currency,
                                    fp,
                                    canonical_json(business_payload),
                                ),
                            )
                            outcome = self._request_result(db, request_id)

            if error_to_raise is not None:
                raise error_to_raise
            assert outcome is not None

        if auto_confirm:
            options = dict(confirm_options or {})
            return self.confirm_clearing(request_id, **options)
        return outcome

    def post_clearing(
        self,
        source: str,
        source_seq: int,
        request_id: str,
        month: str,
        currency: str,
        allocations: Mapping[str, int],
        *,
        period_policy: str = "error",
        target_month: str | None = None,
        report_id: str | None = None,
    ) -> dict[str, Any]:
        """一步完成“已核实清算申请 -> 过账”，适用于申请本身即清算回报的调用方。"""

        return self.receive_clearing(
            source,
            source_seq,
            request_id,
            month,
            currency,
            allocations,
            auto_confirm=True,
            confirm_options={
                "period_policy": period_policy,
                "target_month": target_month,
                "report_id": report_id,
            },
        )

    def get_request(self, request_id: str) -> dict[str, Any]:
        with self.session() as db:
            row = db.execute(
                "SELECT * FROM le_request WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"清算申请不存在：{request_id}",
                    details={"request_id": request_id},
                )
            return self._request_result(db, row)

    @staticmethod
    def _error_from_request(
        row: sqlite3.Row, *, idempotent_replay: bool = False
    ) -> LedgerError:
        try:
            details = json.loads(row["error_details_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            details = {}
        if idempotent_replay:
            details = {**details, "idempotent_replay": True}
        message = row["error_message"] or f"清算申请当前状态为 {row['status']}"
        code = row["error_code"] or "request_not_postable"
        if code == BalanceError.code:
            return BalanceError(message, details=details)
        if code == CurrencyMismatchError.code:
            return CurrencyMismatchError(message, details=details)
        if code == PeriodConflictError.code:
            return PeriodConflictError(message, details=details)
        if code == IdempotencyConflictError.code:
            return IdempotencyConflictError(message, details=details)
        if row["status"] == "blocked" or code == PeriodConflictError.code:
            return PeriodConflictError(message, details=details)
        return ValidationError(message, details=details, code=code)

    def _request_result(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row | str,
        *,
        idempotent_replay: bool = False,
    ) -> dict[str, Any]:
        if isinstance(row, str):
            fetched = db.execute(
                "SELECT * FROM le_request WHERE request_id=?", (row,)
            ).fetchone()
            if fetched is None:
                raise NotFoundError(f"清算申请不存在：{row}")
            row = fetched

        result = dict(row)
        result["payload"] = self._loads(result.pop("payload_json", None), {})
        result["error_details"] = self._loads(result.pop("error_details_json", None), {})
        result["idempotent_replay"] = idempotent_replay
        result["verified"] = result["status"] in {"posted"}
        result["posted"] = result["status"] == "posted"
        result["final_disposition"] = result["status"] in {"posted", "rejected"}
        if result.get("voucher_id"):
            result["voucher"] = self._voucher_dict(db, result["voucher_id"])
        return result

    def confirm_clearing(
        self,
        request_id: str,
        *,
        report_id: str | None = None,
        outcome: str = "cleared",
        report: Mapping[str, Any] | None = None,
        period_policy: str = "error",
        target_month: str | None = None,
    ) -> dict[str, Any]:
        """录入清算回报并在同一事务中过账。

        ``outcome='rejected'`` 表示清算回报明确拒绝该申请；否则按申请中的分摊过账。
        """

        request_id = str(request_id)
        report_id = report_id or f"report:{request_id}"
        with self.session() as db:
            try:
                result: dict[str, Any] | None = None
                rejection: LedgerError | None = None
                with self.immediate(db):
                    req = db.execute(
                        "SELECT * FROM le_request WHERE request_id=?", (request_id,)
                    ).fetchone()
                    if req is None:
                        raise NotFoundError(
                            f"清算申请不存在：{request_id}",
                            details={"request_id": request_id},
                        )
                    if req["status"] == "posted" and req["voucher_id"]:
                        result = self._request_result(db, req, idempotent_replay=True)
                    elif req["status"] == "rejected":
                        details = self._loads(req["error_details_json"], {})
                        rejection = ValidationError(
                            req["error_message"] or "申请已被最终拒绝",
                            details={**details, "idempotent_replay": True},
                            code=req["error_code"] or "request_rejected",
                        )
                    else:
                        payload = self._loads(req["payload_json"], {})
                        report_payload = dict(report or {})
                        report_payload.setdefault("report_id", report_id)
                        report_payload.setdefault("request_id", request_id)
                        report_payload.setdefault("outcome", outcome)

                        existing_report = db.execute(
                            "SELECT * FROM le_report WHERE request_id=?", (request_id,)
                        ).fetchone()
                        if existing_report is not None:
                            # 相同回报重发是幂等的；不同结论不能覆盖旧回报。
                            if existing_report["fingerprint"] != fingerprint(report_payload):
                                raise IdempotencyConflictError(
                                    "同一申请收到结论不同的清算回报",
                                    details={"request_id": request_id},
                                )

                        if outcome == "rejected":
                            exc = ValidationError(
                                f"清算回报拒绝申请 {request_id}",
                                details={"request_id": request_id, "report": report_payload},
                                code="clearing_report_rejected",
                            )
                            db.execute(
                                """INSERT INTO le_report(report_id, request_id, outcome, fingerprint, payload_json)
                                   VALUES(?, ?, 'rejected', ?, ?)
                                   ON CONFLICT(request_id) DO NOTHING""",
                                (
                                    report_id,
                                    request_id,
                                    fingerprint(report_payload),
                                    canonical_json(report_payload),
                                ),
                            )
                            db.execute(
                                """UPDATE le_request
                                   SET status='rejected', error_code=?, error_message=?,
                                       error_details_json=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                                   WHERE request_id=?""",
                                (
                                    exc.code,
                                    exc.message,
                                    canonical_json(exc.details),
                                    request_id,
                                ),
                            )
                            rejection = exc
                        else:
                            try:
                                lines = self._allocation_lines(
                                    db,
                                    payload["allocations"],
                                    payload["currency"],
                                    payload["month"],
                                )
                                effective_month, period_states = self._resolve_effective_month(
                                    db,
                                    lines,
                                    payload["month"],
                                    policy=period_policy,
                                    target_month=target_month,
                                    source=req["source"],
                                    source_seq=req["source_seq"],
                                )
                            except ValidationError as exc:
                                rejection = exc
                            else:
                                # 若回报显式带了分摊，它必须与申请一致；差异应作为另一笔/冲正处理。
                                reported_allocations = report_payload.get("allocations")
                                if (
                                    reported_allocations is not None
                                    and fingerprint(reported_allocations)
                                    != fingerprint(payload["allocations"])
                                ):
                                    rejection = IdempotencyConflictError(
                                        "清算回报分摊与原申请不同，不能直接覆盖过账",
                                        details={
                                            "request_id": request_id,
                                            "required_action": "请识别为新请求或编制冲正凭证",
                                        },
                                    )
                                else:
                                    self._insert_voucher(
                                        db,
                                        voucher_id=request_id,
                                        kind="clearing",
                                        lines=lines,
                                        currency=payload["currency"],
                                        source=req["source"],
                                        source_seq=req["source_seq"],
                                        fingerprint_value=req["fingerprint"],
                                        requested_month=payload["month"],
                                        effective_month=effective_month,
                                        period_policy=period_policy,
                                        idempotency_key=f"request:{request_id}",
                                        payload=payload,
                                    )
                                    db.execute(
                                        """INSERT INTO le_report(report_id, request_id, outcome, fingerprint, payload_json)
                                           VALUES(?, ?, 'cleared', ?, ?)
                                           ON CONFLICT(request_id) DO NOTHING""",
                                        (
                                            report_id,
                                            request_id,
                                            fingerprint(report_payload),
                                            canonical_json(report_payload),
                                        ),
                                    )
                                    db.execute(
                                        """UPDATE le_request
                                           SET status='posted', voucher_id=?, error_code=NULL,
                                               error_message=NULL, error_details_json=NULL,
                                               updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                                           WHERE request_id=?""",
                                        (request_id, request_id),
                                    )
                                    result = self._request_result(db, request_id)
                                    result["period_states"] = period_states

                if rejection is not None:
                    raise rejection
                assert result is not None
                return result
            except PeriodConflictError as exc:
                self._persist_request_failure(db, request_id, "blocked", exc)
                raise
            except ValidationError as exc:
                # 申请载荷/平衡错误是最终不可过账；期间冲突已在上面单独处理。
                current = db.execute(
                    "SELECT status FROM le_request WHERE request_id=?", (request_id,)
                ).fetchone()
                if current and current["status"] != "rejected":
                    self._persist_request_failure(db, request_id, "rejected", exc)
                raise

    def _persist_request_failure(
        self,
        db: sqlite3.Connection,
        request_id: str,
        status: str,
        exc: LedgerError,
    ) -> None:
        with self.immediate(db):
            db.execute(
                """UPDATE le_request
                   SET status=?, error_code=?, error_message=?, error_details_json=?,
                       updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                   WHERE request_id=?""",
                (
                    status,
                    exc.code,
                    exc.message,
                    canonical_json(exc.details),
                    request_id,
                ),
            )

    def note_delivery_unknown(
        self, ref_id: str, kind: str = "ack_unknown", ref_type: str = "request"
    ) -> dict[str, Any]:
        with self.session() as db, self.immediate(db):
            db.execute(
                "INSERT INTO le_delivery_event(ref_type, ref_id, kind, detail_json) VALUES(?, ?, ?, ?)",
                (ref_type, str(ref_id), kind, canonical_json({"noted": True})),
            )
        if ref_type == "request":
            try:
                return self.get_request(str(ref_id))
            except NotFoundError:
                pass
        return self.recovery()

    # ------------------------------------------------------------------
    # 关账前来源位置核对、冻结和稳定凭据
    # ------------------------------------------------------------------

    def _build_close_plan(
        self,
        db: sqlite3.Connection,
        org: str,
        month: str,
        cuts: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(cuts, Mapping):
            raise ValidationError("cuts 必须是 {source: through_seq} 对象")

        period = db.execute(
            "SELECT * FROM period WHERE org=? AND month=?", (org, month)
        ).fetchone()
        if period is None:
            raise NotFoundError(
                f"期间未配置：{org} {month}", details={"org": org, "month": month}
            )

        source_plans: list[dict[str, Any]] = []
        all_conflicts: list[dict[str, Any]] = []
        can_close = True

        for raw_source, raw_through in cuts.items():
            source = str(raw_source)
            if (
                not isinstance(raw_through, int)
                or isinstance(raw_through, bool)
                or raw_through < 0
            ):
                raise ValidationError(
                    f"来源 {source} 的 through 必须是非负整数",
                    details={"source": source, "value": raw_through},
                )
            through = raw_through
            cut_row = db.execute(
                "SELECT * FROM source_cuts WHERE source=?", (source,)
            ).fetchone()
            source_registered = cut_row is not None
            previous = int(cut_row["final_through"]) if cut_row is not None else 0

            plan: dict[str, Any] = {
                "source": source,
                "source_registered": source_registered,
                "previous_final_through": previous,
                "requested_through": through,
                "positions": [],
                "finalized": [],
                "unverified": [],
                "blocked": [],
                "rejected": [],
                "missing": [],
                "missing_ranges": [],
            }

            if not source_registered:
                can_close = False
                conflict = {
                    "source": source,
                    "code": "source_not_registered",
                    "message": "来源不在 source_cuts 登记表中，不能证明来源位置完整",
                }
                all_conflicts.append(conflict)
                plan["conflict"] = conflict
                source_plans.append(plan)
                continue

            if through < previous:
                can_close = False
                conflict = {
                    "source": source,
                    "code": "cut_behind_final_watermark",
                    "previous_final_through": previous,
                    "requested_through": through,
                }
                all_conflicts.append(conflict)
                plan["conflict"] = conflict

            start = previous + 1
            request_rows = db.execute(
                """SELECT source_seq, request_id, status, voucher_id, error_code
                   FROM le_request
                   WHERE source=? AND source_seq >= ? AND source_seq <= ?
                   ORDER BY source_seq""",
                (source, start, through),
            ).fetchall()
            by_seq = {row["source_seq"]: row for row in request_rows}

            # 不按 through 做巨型循环；沿已有序号找缺口，同时保留小场景的 missing 列表。
            missing: list[int] = []
            missing_ranges: list[dict[str, int]] = []
            expected = start
            for seq in sorted(by_seq):
                if seq > expected:
                    gap_start = expected
                    gap_end = min(seq - 1, through)
                    missing_ranges.append({"from": gap_start, "through": gap_end})
                    if len(missing) < 1000:
                        missing.extend(range(gap_start, gap_end + 1))
                expected = max(expected, seq + 1)
            if expected <= through:
                gap_start = expected
                gap_end = through
                missing_ranges.append({"from": gap_start, "through": gap_end})
                if len(missing) < 1000:
                    remaining = list(range(gap_start, gap_end + 1))
                    missing.extend(remaining[: max(0, 1000 - len(missing))])

            for seq in range(start, min(through, start + 9999) + 1):
                # 小范围直接生成位置明细；大范围由 missing_ranges 和查询结果解释。
                row = by_seq.get(seq)
                if row is None:
                    if seq in missing:
                        position = {"source_seq": seq, "status": "missing"}
                        plan["positions"].append(position)
                    continue
                item = {
                    "source_seq": seq,
                    "request_id": row["request_id"],
                    "status": row["status"],
                    "voucher_id": row["voucher_id"],
                    "error_code": row["error_code"],
                    "final_disposition": row["status"] in {"posted", "rejected"},
                }
                plan["positions"].append(item)
                if row["status"] == "posted":
                    plan["finalized"].append(item)
                elif row["status"] == "rejected":
                    plan["rejected"].append(item)
                    plan["finalized"].append(item)
                elif row["status"] == "blocked":
                    plan["blocked"].append(item)
                    can_close = False
                else:
                    plan["unverified"].append(item)
                    can_close = False

            if missing:
                can_close = False
                plan["missing"] = missing
            if missing_ranges:
                plan["missing_ranges"] = missing_ranges

            source_plans.append(plan)

        return {
            "org": org,
            "month": month,
            "period_state": period["state"],
            "period_actor": period["actor"],
            "cuts": dict(cuts),
            "can_close": can_close,
            "sources": source_plans,
            "conflicts": all_conflicts,
            "explanation": (
                "只有 cuts 区间内每个来源序号都已过账或最终拒绝、且无缺口/收件未核实/阻塞时，"
                "才能冻结期间。"
            ),
        }

    def close_period(
        self,
        actor: str,
        org: str,
        month: str,
        cuts: Mapping[str, Any],
        *,
        idempotency_key: str | None = None,
        phase: str = "close",
    ) -> dict[str, Any]:
        actor = str(actor)
        org = str(org)
        month = str(month)
        close_key = idempotency_key or fingerprint(
            {"actor": actor, "org": org, "month": month, "cuts": cuts}
        )

        with self.session() as db:
            result: dict[str, Any]
            with self.immediate(db):
                period = db.execute(
                    "SELECT * FROM period WHERE org=? AND month=?", (org, month)
                ).fetchone()
                if period is None:
                    raise NotFoundError(
                        f"期间未配置：{org} {month}",
                        details={"org": org, "month": month},
                    )
                if period["actor"] != actor:
                    raise UnauthorizedCloseError(
                        f"结账员 {actor} 无权结转 {org} {month}；有权人是 {period['actor']}",
                        details={
                            "actor": actor,
                            "authorized_actor": period["actor"],
                            "org": org,
                            "month": month,
                        },
                    )

                certificate_row = db.execute(
                    "SELECT * FROM le_close_certificate WHERE org=? AND month=?",
                    (org, month),
                ).fetchone()
                if period["state"] == "closed" and certificate_row is not None:
                    certificate = self._certificate_dict(db, certificate_row)
                    cuts_match = fingerprint(certificate["cuts"]) == fingerprint(cuts)
                    stored_close_key = certificate_row["close_key"]
                    result = {
                        # 相同 cut 的重试是幂等成功；不同 cut 不能伪装成同一请求，
                        # 但仍返回已存在的稳定凭据和冲突标记。
                        "ok": cuts_match,
                        "closed": True,
                        "already_closed": True,
                        "idempotent_replay": cuts_match,
                        "cuts_match": cuts_match,
                        "close_key_matches": stored_close_key == close_key,
                        "code": None if cuts_match else AlreadyClosedError.code,
                        "phase": phase,
                        "stored_cuts": certificate["cuts"],
                        "incoming_cuts": dict(cuts),
                        "certificate": certificate,
                        "recovery": {
                            "period": f"{org}/{month}",
                            "state": "closed",
                            "certificate_id": certificate["certificate_id"],
                        },
                    }
                    if not cuts_match:
                        result["message"] = (
                            "期间已用不同来源截止位置冻结；旧结账凭据保持不变，"
                            "新的差异应在后续期间或更正凭证中处理。"
                        )
                else:
                    # close_start 即把期间置于 closing；cut 内待核实申请仍可过账。
                    db.execute(
                        "UPDATE period SET state='closing' WHERE org=? AND month=?",
                        (org, month),
                    )
                    db.execute(
                        """INSERT INTO le_close_job(
                               org, month, actor, close_key, cuts_json, status, attempts
                           ) VALUES(?, ?, ?, ?, ?, 'checking', 1)
                           ON CONFLICT(org, month) DO UPDATE SET
                               actor=excluded.actor,
                               close_key=excluded.close_key,
                               cuts_json=excluded.cuts_json,
                               status='checking',
                               attempts=le_close_job.attempts + 1,
                               updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')""",
                        (org, month, actor, close_key, canonical_json(cuts)),
                    )

                    plan = self._build_close_plan(db, org, month, cuts)
                    if not plan["can_close"]:
                        message = "尚不能冻结：存在未最终处置的来源位置"
                        db.execute(
                            """UPDATE le_close_job
                               SET status='not_ready', result_json=?
                               WHERE org=? AND month=?""",
                            (canonical_json(plan), org, month),
                        )
                        result = {
                            "ok": False,
                            "closed": False,
                            "code": CloseNotReadyError.code,
                            "message": message,
                            "phase": phase,
                            "close_key": close_key,
                            "plan": plan,
                            "remediation": (
                                "请收取缺失序号或等待/录入清算回报；完成后用相同参数 close_retry。"
                            ),
                        }
                    else:
                        certificate = self._finalize_close(
                            db,
                            org=org,
                            month=month,
                            actor=actor,
                            close_key=close_key,
                            cuts=cuts,
                            plan=plan,
                        )
                        db.execute(
                            """UPDATE le_close_job
                               SET status='completed', certificate_id=?, result_json=?
                               WHERE org=? AND month=?""",
                            (
                                certificate["certificate_id"],
                                canonical_json({"certificate_id": certificate["certificate_id"]}),
                                org,
                                month,
                            ),
                        )
                        result = {
                            "ok": True,
                            "closed": True,
                            "already_closed": False,
                            "phase": phase,
                            "close_key": close_key,
                            "certificate": certificate,
                        }
            return result

    def _finalize_close(
        self,
        db: sqlite3.Connection,
        *,
        org: str,
        month: str,
        actor: str,
        close_key: str,
        cuts: Mapping[str, Any],
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        for source_plan in plan["sources"]:
            if source_plan.get("source_registered"):
                db.execute(
                    "UPDATE source_cuts SET final_through=? WHERE source=?",
                    (source_plan["requested_through"], source_plan["source"]),
                )

        db.execute(
            "UPDATE period SET state='closed', actor=? WHERE org=? AND month=?",
            (actor, org, month),
        )

        balances = self._period_balances(db, org, month)
        voucher_rows = db.execute(
            """SELECT DISTINCT v.* FROM le_voucher v
               JOIN le_entry e ON e.voucher_id=v.voucher_id
               WHERE e.org=? AND e.month=?
               ORDER BY v.chain_seq""",
            (org, month),
        ).fetchall()
        vouchers = [self._voucher_dict(db, row) for row in voucher_rows]
        chain_row = db.execute(
            "SELECT chain_seq, voucher_hash FROM le_voucher ORDER BY chain_seq DESC LIMIT 1"
        ).fetchone()

        certificate_id = f"cert-{org}-{month}"
        snapshot = {
            "org": org,
            "month": month,
            "closed_by": actor,
            "cuts": dict(cuts),
            "source_plan": plan,
            "balances": balances["balances"],
            "currency_totals": balances["currency_totals"],
            "vouchers": [
                {
                    "voucher_id": voucher["voucher_id"],
                    "kind": voucher["kind"],
                    "voucher_hash": voucher["voucher_hash"],
                    "chain_seq": voucher["chain_seq"],
                    "original_voucher_id": voucher["original_voucher_id"],
                    "effective_month": voucher["effective_month"],
                    "entries": voucher["entries"],
                }
                for voucher in vouchers
            ],
            "chain_tip": chain_row["voucher_hash"] if chain_row else GENESIS_HASH,
        }
        certificate_hash = fingerprint(snapshot)
        credential_token = token(
            {
                "certificate_id": certificate_id,
                "org": org,
                "month": month,
                "certificate_hash": certificate_hash,
            }
        )

        db.execute(
            """INSERT INTO le_close_certificate(
                   certificate_id, org, month, actor, close_key, cuts_json,
                   snapshot_json, certificate_hash, credential_token,
                   chain_seq, chain_tip
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                certificate_id,
                org,
                month,
                actor,
                close_key,
                canonical_json(cuts),
                canonical_json(snapshot),
                certificate_hash,
                credential_token,
                chain_row["chain_seq"] if chain_row else 0,
                chain_row["voucher_hash"] if chain_row else GENESIS_HASH,
            ),
        )
        row = db.execute(
            "SELECT * FROM le_close_certificate WHERE certificate_id=?",
            (certificate_id,),
        ).fetchone()
        return self._certificate_dict(db, row)

    def _certificate_dict(
        self, db: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        result = dict(row)
        result["cuts"] = self._loads(result.pop("cuts_json", None), {})
        result["snapshot"] = self._loads(result.pop("snapshot_json", None), {})
        result["stable_read_credential"] = result.pop("credential_token")
        result["stable"] = True
        return result

    def read_closed_certificate(self, org: str, month: str) -> dict[str, Any]:
        with self.session() as db:
            period = db.execute(
                "SELECT state, actor FROM period WHERE org=? AND month=?",
                (org, month),
            ).fetchone()
            if period is None:
                raise NotFoundError(
                    f"期间未配置：{org} {month}", details={"org": org, "month": month}
                )
            row = db.execute(
                "SELECT * FROM le_close_certificate WHERE org=? AND month=?",
                (org, month),
            ).fetchone()
            if period["state"] != "closed" or row is None:
                raise ConflictError(
                    f"{org} {month} 尚未冻结，没有稳定结账读凭据",
                    details={
                        "org": org,
                        "month": month,
                        "state": period["state"],
                        "actor": period["actor"],
                        "hint": "可先调用 close_period 并处理 close_not_ready 明细",
                    },
                    code="period_not_closed",
                    http_status=409,
                )
            certificate = self._certificate_dict(db, row)
            # 这里只校验凭据自身哈希；不要把全局链尖放进读凭据，否则关账后的
            # 新更正凭证会让“同一份已发给财务的读凭据”看起来发生变化。
            certificate["verification"] = {
                "certificate_hash_matches": fingerprint(certificate["snapshot"])
                == certificate["certificate_hash"],
                "closed_chain_tip": certificate["chain_tip"],
                "note": "关账后的更正凭证追加在后续链上，不重写本凭据或旧账",
            }
            return certificate

    def read_close_credential(self, org: str, month: str) -> dict[str, Any]:
        return self.read_closed_certificate(org, month)

    # ------------------------------------------------------------------
    # 期间净额和凭证链查询
    # ------------------------------------------------------------------

    def _period_balances(
        self, db: sqlite3.Connection, org: str, month: str
    ) -> dict[str, Any]:
        rows = db.execute(
            """SELECT e.account AS account,
                      COALESCE(a.org, e.org) AS org,
                      a.kind AS kind,
                      e.currency AS currency,
                      SUM(e.amount) AS net_signed,
                      SUM(CASE WHEN e.amount > 0 THEN e.amount ELSE 0 END) AS debit_total,
                      SUM(CASE WHEN e.amount < 0 THEN -e.amount ELSE 0 END) AS credit_total,
                      COUNT(*) AS entry_count
               FROM le_entry e
               LEFT JOIN account a ON a.id=e.account
               WHERE e.org=? AND e.month=?
               GROUP BY e.account, e.currency, a.org, a.kind
               ORDER BY e.account, e.currency""",
            (org, month),
        ).fetchall()
        balances = [dict(row) for row in rows]
        currency_totals: dict[str, int] = defaultdict(int)
        for row in rows:
            currency_totals[row["currency"]] += int(row["net_signed"])
        return {
            "balances": balances,
            "currency_totals": dict(currency_totals),
        }

    def period_net(
        self, org: str, month: str, *, include_chain: bool = True
    ) -> dict[str, Any]:
        with self.session() as db:
            period = db.execute(
                "SELECT * FROM period WHERE org=? AND month=?", (org, month)
            ).fetchone()
            if period is None:
                raise NotFoundError(
                    f"期间未配置：{org} {month}", details={"org": org, "month": month}
                )
            balances = self._period_balances(db, org, month)
            result: dict[str, Any] = {
                "ok": True,
                "org": org,
                "month": month,
                "state": period["state"],
                "actor": period["actor"],
                **balances,
            }
            cert = db.execute(
                "SELECT certificate_id, certificate_hash FROM le_close_certificate WHERE org=? AND month=?",
                (org, month),
            ).fetchone()
            if cert:
                result["certificate_id"] = cert["certificate_id"]
                result["certificate_hash"] = cert["certificate_hash"]

            if include_chain:
                rows = db.execute(
                    """SELECT DISTINCT v.* FROM le_voucher v
                       JOIN le_entry e ON e.voucher_id=v.voucher_id
                       WHERE e.org=? AND e.month=?
                       ORDER BY v.chain_seq""",
                    (org, month),
                ).fetchall()
                result["voucher_chain"] = [self._voucher_dict(db, row) for row in rows]
                result["voucher_ids"] = [row["voucher_id"] for row in rows]
            return result

    def service_chain(self, ref: str) -> dict[str, Any]:
        ref = str(ref)
        with self.session() as db:
            root_row = db.execute(
                "SELECT * FROM le_voucher WHERE voucher_id=?", (ref,)
            ).fetchone()
            if root_row is None:
                req = db.execute(
                    "SELECT voucher_id FROM le_request WHERE request_id=?", (ref,)
                ).fetchone()
                if req and req["voucher_id"]:
                    root_row = db.execute(
                        "SELECT * FROM le_voucher WHERE voucher_id=?",
                        (req["voucher_id"],),
                    ).fetchone()
            if root_row is None:
                refund = db.execute(
                    "SELECT voucher_id FROM le_refund WHERE refund_id=?", (ref,)
                ).fetchone()
                if refund and refund["voucher_id"]:
                    root_row = db.execute(
                        "SELECT * FROM le_voucher WHERE voucher_id=?",
                        (refund["voucher_id"],),
                    ).fetchone()
            if root_row is None:
                raise NotFoundError(
                    f"找不到服务/业务请求/凭证：{ref}", details={"ref": ref}
                )

            root_id = root_row["voucher_id"]
            seen: set[str] = set()
            while root_row["original_voucher_id"]:
                parent_id = root_row["original_voucher_id"]
                parent = db.execute(
                    "SELECT * FROM le_voucher WHERE voucher_id=?", (parent_id,)
                ).fetchone()
                if parent is None or parent["voucher_id"] in seen:
                    break
                seen.add(parent["voucher_id"])
                root_row = parent
                root_id = parent["voucher_id"]

            family_rows = db.execute(
                """WITH RECURSIVE family(voucher_id) AS (
                       SELECT voucher_id FROM le_voucher WHERE voucher_id=?
                       UNION ALL
                       SELECT v.voucher_id
                       FROM le_voucher v
                       JOIN family f ON v.original_voucher_id=f.voucher_id
                   )
                   SELECT v.* FROM le_voucher v
                   JOIN family f ON f.voucher_id=v.voucher_id
                   ORDER BY v.chain_seq""",
                (root_id,),
            ).fetchall()
            vouchers = [self._voucher_dict(db, row) for row in family_rows]
            family_ids = [v["voucher_id"] for v in vouchers]
            placeholders = ",".join("?" for _ in family_ids)

            effective_entries: list[dict[str, Any]] = []
            period_keys: set[tuple[str, str]] = set()
            for voucher in vouchers:
                for entry in voucher["entries"]:
                    effective_entries.append(
                        {
                            "service_root": root_id,
                            "voucher_id": voucher["voucher_id"],
                            "voucher_kind": voucher["kind"],
                            "original_voucher_id": voucher["original_voucher_id"],
                            **entry,
                        }
                    )
                    if entry.get("org") and entry.get("month"):
                        period_keys.add((entry["org"], entry["month"]))

            period_states: list[dict[str, Any]] = []
            for org, month in sorted(period_keys):
                period = db.execute(
                    "SELECT org, month, state, actor FROM period WHERE org=? AND month=?",
                    (org, month),
                ).fetchone()
                cert = db.execute(
                    "SELECT certificate_id FROM le_close_certificate WHERE org=? AND month=?",
                    (org, month),
                ).fetchone()
                period_states.append(
                    {
                        "org": org,
                        "month": month,
                        "state": period["state"] if period else "missing",
                        "actor": period["actor"] if period else None,
                        "certificate_id": cert["certificate_id"] if cert else None,
                    }
                )

            requests = [
                dict(row)
                for row in db.execute(
                    f"""SELECT request_id, source, source_seq, status, voucher_id,
                               error_code, month
                        FROM le_request
                        WHERE voucher_id IN ({placeholders}) OR request_id=?""",
                    (*family_ids, ref),
                ).fetchall()
            ] if family_ids else []

            refunds = [
                dict(row)
                for row in db.execute(
                    f"""SELECT refund_id, original_voucher_id, amount, currency,
                               requested_month, effective_month, status, voucher_id, error_code
                        FROM le_refund
                        WHERE original_voucher_id IN ({placeholders})
                           OR refund_id=?
                           OR voucher_id IN ({placeholders})
                        ORDER BY refund_id""",
                    (*family_ids, ref, *family_ids),
                ).fetchall()
            ] if family_ids else []

            return {
                "ok": True,
                "query_ref": ref,
                "root_voucher_id": root_id,
                "voucher_chain": vouchers,
                "effective_entries": effective_entries,
                "period_states": period_states,
                "requests": requests,
                "refunds": refunds,
                "visibility_rule": (
                    "这里只返回已过账凭证的有效分录；申请收件、拒绝和阻塞状态仅作追踪，"
                    "不会形成单边应收/应付。"
                ),
            }

    # ------------------------------------------------------------------
    # 跨期退费和红冲
    # ------------------------------------------------------------------

    def create_refund(
        self,
        refund_id: str,
        original_voucher_id: str,
        amount: int,
        month: str,
        *,
        currency: str | None = None,
        lines: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
        period_policy: str = "error",
        target_month: str | None = None,
        voucher_kind: str = "refund",
    ) -> dict[str, Any]:
        refund_id = str(refund_id)
        original_voucher_id = str(original_voucher_id)
        month = str(month)
        if voucher_kind not in {"refund", "reversal"}:
            raise ValidationError("voucher_kind 只能是 refund 或 reversal")
        if not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
            raise ValidationError("退款/红冲金额必须是非负整数分", details={"amount": amount})

        call_payload = {
            "refund_id": refund_id,
            "original_voucher_id": original_voucher_id,
            "amount": amount,
            "month": month,
            "currency": currency,
            "lines": lines,
            "period_policy": period_policy,
            "target_month": target_month,
            "voucher_kind": voucher_kind,
        }
        fp = fingerprint(
            {
                "refund_id": refund_id,
                "original_voucher_id": original_voucher_id,
                "amount": amount,
                "month": month,
                "currency": currency,
                "lines": lines,
                "voucher_kind": voucher_kind,
            }
        )

        with self.session() as db:
            result: dict[str, Any] | None = None
            error_to_raise: LedgerError | None = None

            with self.immediate(db):
                existing = db.execute(
                    "SELECT * FROM le_refund WHERE refund_id=?", (refund_id,)
                ).fetchone()
                if existing is not None and existing["fingerprint"] != fp:
                    raise IdempotencyConflictError(
                        "退款编号已用于金额、原交易、期间或分摊不同的请求",
                        details={
                            "refund_id": refund_id,
                            "existing_fingerprint": existing["fingerprint"],
                            "incoming_fingerprint": fp,
                            "existing_status": existing["status"],
                        },
                    )
                if existing is not None and existing["status"] == "posted":
                    result = {
                        "ok": True,
                        "idempotent_replay": True,
                        "refund": self._refund_dict(db, existing),
                        "voucher": self._voucher_dict(db, existing["voucher_id"]),
                    }
                elif existing is not None and existing["status"] == "rejected":
                    error_to_raise = self._error_from_refund(existing)
                else:
                    original = db.execute(
                        "SELECT * FROM le_voucher WHERE voucher_id=?",
                        (original_voucher_id,),
                    ).fetchone()
                    if original is None:
                        raise NotFoundError(
                            f"原交易凭证不存在：{original_voucher_id}",
                            details={"original_voucher_id": original_voucher_id},
                        )
                    original_entries = db.execute(
                        "SELECT * FROM le_entry WHERE voucher_id=? ORDER BY line_no",
                        (original_voucher_id,),
                    ).fetchall()
                    refund_currency = currency or (
                        original["currency"] if original["currency"] != "MULTI" else None
                    )
                    if not refund_currency:
                        raise ValidationError(
                            "多币种原凭证退款必须显式指定 currency",
                            details={"original_voucher_id": original_voucher_id},
                        )
                    base_entries = [
                        entry
                        for entry in original_entries
                        if entry["currency"] == refund_currency
                    ]
                    if not base_entries:
                        raise ValidationError(
                            "原凭证没有指定币种的分录",
                            details={
                                "original_voucher_id": original_voucher_id,
                                "currency": refund_currency,
                            },
                        )

                    base_refundable = sum(
                        entry["amount"] for entry in base_entries if entry["amount"] > 0
                    )
                    already_row = db.execute(
                        """SELECT COALESCE(SUM(r.amount), 0) AS total
                           FROM le_refund r
                           WHERE r.original_voucher_id=?
                             AND r.status='posted'
                             AND r.currency=?""",
                        (original_voucher_id, refund_currency),
                    ).fetchone()
                    already_refunded = int(already_row["total"])
                    available = base_refundable - already_refunded

                    # 插入/复用退款单，便于把超额和期间冲突作为可恢复状态保存。
                    db.execute(
                        """INSERT INTO le_refund(
                               refund_id, kind, original_voucher_id, amount, currency,
                               requested_month, fingerprint, payload_json, status
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'checking')
                           ON CONFLICT(refund_id) DO UPDATE SET
                               kind=excluded.kind,
                               amount=excluded.amount,
                               currency=excluded.currency,
                               requested_month=excluded.requested_month,
                               fingerprint=excluded.fingerprint,
                               payload_json=excluded.payload_json,
                               status='checking',
                               error_code=NULL,
                               error_message=NULL,
                               error_details_json=NULL,
                               updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')""",
                        (
                            refund_id,
                            voucher_kind,
                            original_voucher_id,
                            amount,
                            refund_currency,
                            month,
                            fp,
                            canonical_json(call_payload),
                        ),
                    )

                    if amount > available:
                        exc = RefundLimitError(
                            "退款被拒绝：本次退款与已入账/同时处理中的退款合计超过原交易可退余额",
                            details={
                                "refund_id": refund_id,
                                "original_voucher_id": original_voucher_id,
                                "requested_amount": amount,
                                "original_refundable_base": base_refundable,
                                "already_refunded": already_refunded,
                                "available_amount": max(0, available),
                                "currency": refund_currency,
                                "rule": "并发退款通过 BEGIN IMMEDIATE 串行检查累计金额",
                            },
                        )
                        self._set_refund_error(db, refund_id, "rejected", exc)
                        error_to_raise = exc
                    else:
                        try:
                            if lines is None:
                                refund_lines = self._proportional_refund_lines(
                                    base_entries, amount, refund_currency, month
                                )
                            else:
                                refund_lines = self._explicit_refund_lines(
                                    base_entries, lines, refund_currency, month
                                )
                            self._check_refund_line_caps(db, refund_lines)
                            effective_month, period_states = self._resolve_effective_month(
                                db,
                                refund_lines,
                                month,
                                policy=period_policy,
                                target_month=target_month,
                                source="refund",
                                source_seq=None,
                            )
                            for line in refund_lines:
                                line["month"] = effective_month
                        except LedgerError as exc:
                            status = (
                                "blocked"
                                if isinstance(exc, PeriodConflictError)
                                else "rejected"
                            )
                            self._set_refund_error(db, refund_id, status, exc)
                            error_to_raise = exc
                        else:
                            self._insert_voucher(
                                db,
                                voucher_id=refund_id,
                                kind=voucher_kind,
                                lines=refund_lines,
                                currency=refund_currency,
                                source="refund",
                                source_seq=None,
                                fingerprint_value=fp,
                                requested_month=month,
                                effective_month=effective_month,
                                original_voucher_id=original_voucher_id,
                                period_policy=period_policy,
                                idempotency_key=f"refund:{refund_id}",
                                payload=call_payload,
                            )
                            for line_no, line in enumerate(refund_lines, start=1):
                                db.execute(
                                    """INSERT INTO le_refund_line(
                                           refund_id, line_no, original_line_id, account,
                                           org, month, currency, amount
                                       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                                    (
                                        refund_id,
                                        line_no,
                                        line.get("original_line_id"),
                                        line["account"],
                                        line.get("org"),
                                        line["month"],
                                        line["currency"],
                                        line["amount"],
                                    ),
                                )
                            db.execute(
                                """UPDATE le_refund
                                   SET status='posted', effective_month=?, voucher_id=?,
                                       error_code=NULL, error_message=NULL, error_details_json=NULL,
                                       updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                                   WHERE refund_id=?""",
                                (effective_month, refund_id, refund_id),
                            )
                            row = db.execute(
                                "SELECT * FROM le_refund WHERE refund_id=?", (refund_id,)
                            ).fetchone()
                            result = {
                                "ok": True,
                                "idempotent_replay": False,
                                "refund": self._refund_dict(db, row),
                                "voucher": self._voucher_dict(db, refund_id),
                                "period_states": period_states,
                            }

            if error_to_raise is not None:
                raise error_to_raise
            assert result is not None
            return result

    def reverse_voucher(
        self,
        reversal_id: str,
        original_voucher_id: str,
        month: str,
        *,
        amount: int | None = None,
        currency: str | None = None,
        lines: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
        period_policy: str = "error",
        target_month: str | None = None,
    ) -> dict[str, Any]:
        """编制全额或部分红字冲正；缺省 amount 表示按原币种全额冲回。"""

        if amount is None and lines is None:
            with self.session() as db:
                voucher = db.execute(
                    "SELECT currency FROM le_voucher WHERE voucher_id=?",
                    (str(original_voucher_id),),
                ).fetchone()
                if voucher is None:
                    raise NotFoundError(
                        f"原凭证不存在：{original_voucher_id}",
                        details={"original_voucher_id": original_voucher_id},
                    )
                effective_currency = currency or (
                    voucher["currency"] if voucher["currency"] != "MULTI" else None
                )
                if effective_currency is None:
                    raise ValidationError("多币种凭证全额冲正必须指定 currency")
                row = db.execute(
                    """SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS base
                       FROM le_entry
                       WHERE voucher_id=? AND currency=?""",
                    (str(original_voucher_id), effective_currency),
                ).fetchone()
                amount = int(row["base"])
                currency = effective_currency
        if amount is None:
            amount = 0
        return self.create_refund(
            reversal_id,
            original_voucher_id,
            amount,
            month,
            currency=currency,
            lines=lines,
            period_policy=period_policy,
            target_month=target_month,
            voucher_kind="reversal",
        )

    def retry_refund(self, refund_id: str, **overrides: Any) -> dict[str, Any]:
        with self.session() as db:
            row = db.execute(
                "SELECT payload_json, status FROM le_refund WHERE refund_id=?",
                (str(refund_id),),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"退款单不存在，可按新退款补办：{refund_id}",
                    details={"refund_id": refund_id, "may_create_new": True},
                )
            call = self._loads(row["payload_json"], {})
            call.update(overrides)
        return self.create_refund(**call)

    def _refund_dict(self, db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = self._loads(result.pop("payload_json", None), {})
        result["error_details"] = self._loads(result.pop("error_details_json", None), {})
        lines = db.execute(
            "SELECT * FROM le_refund_line WHERE refund_id=? ORDER BY line_no",
            (row["refund_id"],),
        ).fetchall()
        result["lines"] = [dict(line) for line in lines]
        return result

    @staticmethod
    def _error_from_refund(row: sqlite3.Row) -> LedgerError:
        details = Ledger._loads(None, {})
        try:
            details = json.loads(row["error_details_json"] or "{}")
        except json.JSONDecodeError:
            details = {}
        message = row["error_message"] or "退款已被拒绝"
        code = row["error_code"] or "refund_rejected"
        if code == RefundLimitError.code:
            return RefundLimitError(message, details={**details, "idempotent_replay": True})
        if code == PeriodConflictError.code:
            return PeriodConflictError(message, details={**details, "idempotent_replay": True})
        return ConflictError(
            message,
            details={**details, "idempotent_replay": True},
            code=code,
        )

    @staticmethod
    def _set_refund_error(
        db: sqlite3.Connection, refund_id: str, status: str, exc: LedgerError
    ) -> None:
        db.execute(
            """UPDATE le_refund
               SET status=?, error_code=?, error_message=?, error_details_json=?,
                   updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
               WHERE refund_id=?""",
            (status, exc.code, exc.message, canonical_json(exc.details), refund_id),
        )

    @staticmethod
    def _proportional_refund_lines(
        entries: Sequence[sqlite3.Row] | Sequence[Mapping[str, Any]],
        amount: int,
        currency: str,
        month: str,
    ) -> list[dict[str, Any]]:
        dict_entries = [dict(entry) for entry in entries]
        positive = {
            entry["id"]: entry["amount"]
            for entry in dict_entries
            if entry["amount"] > 0
        }
        negative = {
            entry["id"]: -entry["amount"]
            for entry in dict_entries
            if entry["amount"] < 0
        }
        positive_alloc = largest_remainder(amount, positive)
        negative_alloc = largest_remainder(amount, negative)

        lines: list[dict[str, Any]] = []
        for entry in sorted(dict_entries, key=lambda item: item["line_no"]):
            if entry["amount"] > 0:
                new_amount = -int(positive_alloc.get(entry["id"], 0))
            elif entry["amount"] < 0:
                new_amount = int(negative_alloc.get(entry["id"], 0))
            else:
                new_amount = 0
            lines.append(
                {
                    "original_line_id": entry["id"],
                    "account": entry["account"],
                    "org": entry.get("org"),
                    "month": month,
                    "currency": currency,
                    "amount": new_amount,
                }
            )
        totals: dict[str, int] = defaultdict(int)
        for line in lines:
            totals[line["currency"]] += line["amount"]
        if any(value != 0 for value in totals.values()):
            raise BalanceError(
                "按原分摊比例计算出的红冲分录不平衡",
                details={"currency_totals": dict(totals)},
            )
        return lines

    @staticmethod
    def _explicit_refund_lines(
        entries: Sequence[sqlite3.Row] | Sequence[Mapping[str, Any]],
        raw_lines: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        currency: str,
        month: str,
    ) -> list[dict[str, Any]]:
        dict_entries = [dict(entry) for entry in entries]
        by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entry in dict_entries:
            by_account[entry["account"]].append(entry)

        provided: dict[str, int] = {}
        if isinstance(raw_lines, Mapping):
            for account, value in raw_lines.items():
                if isinstance(value, Mapping):
                    provided[str(account)] = int(value["amount"])  # type: ignore[index]
                else:
                    provided[str(account)] = int(value)
        else:
            for item in raw_lines:
                item = dict(item)
                provided[str(item["account"])] = int(item["amount"])

        lines: list[dict[str, Any]] = []
        for entry in sorted(dict_entries, key=lambda item: item["line_no"]):
            if entry["account"] not in provided:
                amount = 0
            else:
                amount = provided.pop(entry["account"])
            if amount != 0 and (entry["amount"] * amount >= 0):
                raise ValidationError(
                    "红冲分录方向必须与原分录相反",
                    details={
                        "account": entry["account"],
                        "original_amount": entry["amount"],
                        "refund_amount": amount,
                    },
                )
            lines.append(
                {
                    "original_line_id": entry["id"],
                    "account": entry["account"],
                    "org": entry.get("org"),
                    "month": month,
                    "currency": currency,
                    "amount": amount,
                }
            )
        if provided:
            raise ValidationError(
                "红冲分摊引用了原凭证中不存在的账户",
                details={"unknown_accounts": sorted(provided)},
            )
        totals: dict[str, int] = defaultdict(int)
        for line in lines:
            totals[line["currency"]] += line["amount"]
        if any(value != 0 for value in totals.values()):
            raise BalanceError(
                "显式红冲分摊不平衡", details={"currency_totals": dict(totals)}
            )
        return lines

    def _check_refund_line_caps(
        self, db: sqlite3.Connection, lines: Sequence[Mapping[str, Any]]
    ) -> None:
        for line in lines:
            original_line_id = line.get("original_line_id")
            if original_line_id is None or int(line["amount"]) == 0:
                continue
            original = db.execute(
                "SELECT amount, account FROM le_entry WHERE id=?",
                (original_line_id,),
            ).fetchone()
            if original is None:
                raise ValidationError(
                    "红冲引用的原分摊行不存在",
                    details={"original_line_id": original_line_id},
                )
            used_row = db.execute(
                """SELECT COALESCE(SUM(ABS(rl.amount)), 0) AS used
                   FROM le_refund_line rl
                   JOIN le_refund r ON r.refund_id=rl.refund_id
                   WHERE rl.original_line_id=? AND r.status='posted'""",
                (original_line_id,),
            ).fetchone()
            used = int(used_row["used"])
            proposed = abs(int(line["amount"]))
            cap = abs(int(original["amount"]))
            if used + proposed > cap:
                raise RefundLimitError(
                    "该原分摊行的累计红冲金额超过原分摊",
                    details={
                        "original_line_id": original_line_id,
                        "account": line["account"],
                        "original_allocation": cap,
                        "already_reversed": used,
                        "requested": proposed,
                        "available": max(0, cap - used),
                    },
                )

    # ------------------------------------------------------------------
    # 恢复和通用 action 入口
    # ------------------------------------------------------------------

    def recovery(self, *, component: str | None = None) -> dict[str, Any]:
        with self.session() as db:
            try:
                db.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.DatabaseError:
                pass

            periods = [dict(row) for row in db.execute("SELECT * FROM period ORDER BY org, month")]
            certificates = {
                (row["org"], row["month"]): row["certificate_id"]
                for row in db.execute("SELECT org, month, certificate_id FROM le_close_certificate")
            }
            for period in periods:
                period["certificate_id"] = certificates.get(
                    (period["org"], period["month"])
                )

            requests = [
                dict(row)
                for row in db.execute(
                    """SELECT request_id, source, source_seq, month, status,
                              voucher_id, error_code
                       FROM le_request ORDER BY source, source_seq"""
                )
            ]
            for item in requests:
                if item["status"] == "posted":
                    item["recovery_action"] = "already_posted; retry returns same voucher"
                elif item["status"] == "rejected":
                    item["recovery_action"] = "terminal_rejection; use a new corrected request/reversal"
                elif item["status"] == "blocked":
                    item["recovery_action"] = "retry confirm with next_common_open period"
                else:
                    item["recovery_action"] = "may_confirm_or_replay_same_request"

            close_jobs = [
                dict(row)
                for row in db.execute(
                    """SELECT org, month, actor, status, attempts, certificate_id, close_key
                       FROM le_close_job ORDER BY org, month"""
                )
            ]
            refunds = [
                dict(row)
                for row in db.execute(
                    """SELECT refund_id, original_voucher_id, amount, currency, status,
                              voucher_id, error_code
                       FROM le_refund ORDER BY refund_id"""
                )
            ]
            cuts = [dict(row) for row in db.execute("SELECT * FROM source_cuts ORDER BY source")]
            chain = self.verify_chain()
            return {
                "ok": True,
                "restarted_component": component,
                "periods": periods,
                "closed_periods": [
                    {"org": p["org"], "month": p["month"], "certificate_id": p["certificate_id"]}
                    for p in periods
                    if p["state"] == "closed"
                ],
                "requests": requests,
                "close_jobs": close_jobs,
                "refunds": refunds,
                "source_cuts": cuts,
                "chain_verification": chain,
                "advice": (
                    "提交结果不明时，用相同业务编号/参数重试：已提交者返回原凭据，"
                    "未提交者仍可补办。"
                ),
            }

    def retry_operation(self, operation_ref: str, **overrides: Any) -> dict[str, Any]:
        ref = str(operation_ref)
        with self.session() as db:
            refund = db.execute(
                "SELECT 1 FROM le_refund WHERE refund_id=?", (ref,)
            ).fetchone()
            if refund:
                return self.retry_refund(ref, **overrides)
            request = db.execute(
                "SELECT status FROM le_request WHERE request_id=?", (ref,)
            ).fetchone()
            if request:
                return self.confirm_clearing(ref, **overrides)
            close_job = db.execute(
                "SELECT * FROM le_close_job WHERE close_key=? OR certificate_id=?",
                (ref, ref),
            ).fetchone()
            if close_job:
                cuts = self._loads(close_job["cuts_json"], {})
                return self.close_period(
                    close_job["actor"],
                    close_job["org"],
                    close_job["month"],
                    cuts,
                    idempotency_key=close_job["close_key"],
                    phase="retry",
                )
        raise NotFoundError(
            f"找不到可重试操作：{ref}",
            details={"ref": ref, "hint": "若数据库中确无记录，可按新操作补办"},
        )

    def handle_action(
        self, action: str, args: Sequence[Any] | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """时间线/HTTP 通用动作入口；领域错误转成 JSON 对象而不抛出。"""

        action = str(action)
        positional = list(args or [])
        try:
            if action in {"request", "clearing_request", "submit_request"}:
                result = self.receive_clearing(*positional, **kwargs)
            elif action in {"post_clearing", "post_request", "submit_and_confirm"}:
                result = self.post_clearing(*positional, **kwargs)
            elif action in {"confirm", "clearing_report", "report"}:
                result = self.confirm_clearing(*positional, **kwargs)
            elif action == "ack_unknown":
                result = self.note_delivery_unknown(positional[0], "ack_unknown", "request")
            elif action in {"close_start", "start_close"}:
                result = self.close_period(*positional, phase="start", **kwargs)
            elif action in {"close_retry", "retry_close", "close"}:
                result = self.close_period(*positional, phase="retry", **kwargs)
            elif action == "close_reply_unknown":
                org, month = positional[0], positional[1]
                self.note_delivery_unknown(
                    f"{org}/{month}", "close_reply_unknown", "period_close"
                )
                result = self.read_closed_certificate(org, month)
            elif action in {"read_closed", "read_close_credential"}:
                result = self.read_closed_certificate(*positional, **kwargs)
            elif action in {"read_period", "period_net", "read_net"}:
                result = self.period_net(*positional, **kwargs)
            elif action in {"read_service", "service_chain", "service"}:
                result = self.service_chain(*positional, **kwargs)
            elif action == "refund":
                result = self.create_refund(*positional, **kwargs)
            elif action in {"reverse", "reversal", "red_reversal"}:
                result = self.reverse_voucher(*positional, **kwargs)
            elif action == "retry":
                result = self.retry_operation(*positional, **kwargs)
            elif action == "restart":
                component = positional[0] if positional else kwargs.get("component")
                result = self.recovery(component=component)
            elif action == "recovery":
                result = self.recovery()
            else:
                raise ValidationError(f"未知动作：{action}", details={"action": action})
            if isinstance(result, dict) and "ok" not in result:
                result = {"ok": True, **result}
            return result
        except LedgerError as exc:
            return exc.to_dict()

    # 短别名，方便交互式调用和按业务词汇查找。
    request = receive_clearing
    post = post_clearing
    refund = create_refund
    reverse = reverse_voucher
    close = close_period
    read_closed = read_closed_certificate
