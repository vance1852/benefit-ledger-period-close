"""账本服务的领域不变量和并发/HTTP 验收测试。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import threading
from threading import Barrier
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from load_sample import load_sample
from ledger_service import Ledger
from ledger_service.errors import (
    IdempotencyConflictError,
    PeriodConflictError,
    RefundLimitError,
    UnauthorizedCloseError,
)
from ledger_service.server import create_server
from ledger_service.utils import canonical_json


FIXTURES = Path(__file__).parent / "fixtures"


class FixtureLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.tmp = Path(self._tempdir.name)

    def bootstrap(self, case: str) -> tuple[Path, Ledger]:
        db_path = self.tmp / f"{case}.db"
        source = FIXTURES / case
        load_sample(source, db_path, "main")
        return db_path, Ledger(db_path)


class CurrencyAndIdentityTest(FixtureLedgerTest):
    def test_duplicate_is_receipt_but_changed_payload_is_conflict(self) -> None:
        _, ledger = self.bootstrap("currency-and-request-identity")
        args = [
            "hub",
            1,
            "p1",
            "05",
            "CNY",
            {"a": -2200, "b": 2200},
        ]
        first = ledger.receive_clearing(*args)
        self.assertEqual(first["status"], "received_unverified")
        confirmed = ledger.confirm_clearing("p1")
        self.assertTrue(confirmed["posted"])
        self.assertTrue(confirmed["voucher"]["balanced"])

        replay = ledger.receive_clearing(*args)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(ledger.get_voucher("p1")["entries"][0]["amount"], -2200)

        with self.assertRaises(IdempotencyConflictError) as ctx:
            ledger.receive_clearing(
                "hub",
                1,
                "p1",
                "05",
                "CNY",
                {"a": -2500, "b": 2500},
            )
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        self.assertIn("existing_fingerprint", ctx.exception.details)
        # 旧回执和旧金额没有被遮盖或改写。
        voucher = ledger.get_voucher("p1")
        self.assertEqual(sum(e["amount"] for e in voucher["entries"]), 0)
        self.assertEqual({e["account"]: e["amount"] for e in voucher["entries"]}["a"], -2200)

    def test_cross_currency_legs_are_rejected_and_do_not_post(self) -> None:
        _, ledger = self.bootstrap("currency-and-request-identity")
        result = ledger.handle_action(
            "request",
            ["hub", 2, "p2", "05", "CNY", {"a": -3000, "c": 3000}],
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "currency_mismatch")
        self.assertEqual(
            result["error"]["details"]["currency_totals"], {"CNY": -3000, "HKD": 3000}
        )
        request = ledger.get_request("p2")
        self.assertEqual(request["status"], "rejected")
        with self.assertRaises(Exception):
            ledger.get_voucher("p2")

        # 回执丢失后原样重发终态拒绝，仍必须是同一份失败而不是 ok:true。
        replay = ledger.handle_action(
            "request",
            ["hub", 2, "p2", "05", "CNY", {"a": -3000, "c": 3000}],
        )
        self.assertFalse(replay["ok"])
        self.assertEqual(replay["error"]["code"], "currency_mismatch")
        self.assertTrue(replay["error"]["details"]["idempotent_replay"])
        with self.assertRaises(Exception):
            ledger.get_voucher("p2")


class OverlappingCloseAndRefundTest(FixtureLedgerTest):
    def setUp(self) -> None:
        super().setUp()
        self.db_path, self.ledger = self.bootstrap("overlapping-close-and-refunds")
        self.request_17 = [
            "clearing-1",
            17,
            "p17",
            "03",
            "CNY",
            {"fund-east": -5000, "recv-care": 5000},
        ]
        self.request_18 = [
            "clearing-1",
            18,
            "p18",
            "03",
            "CNY",
            {"fund-east": -3000, "recv-care": 3000},
        ]

    def receive_17_18(self) -> None:
        self.ledger.receive_clearing(*self.request_17)
        self.ledger.receive_clearing(*self.request_18)

    def test_close_explains_unverified_positions_then_freezes_and_recovers(self) -> None:
        self.receive_17_18()
        started = self.ledger.close_period(
            "zhou", "care", "03", {"clearing-1": 18}, phase="start"
        )
        self.assertFalse(started["ok"])
        self.assertEqual(started["code"], "close_not_ready")
        source_plan = started["plan"]["sources"][0]
        self.assertEqual(source_plan["previous_final_through"], 16)
        self.assertEqual([x["source_seq"] for x in source_plan["positions"]], [17, 18])
        self.assertEqual(
            {x["request_id"]: x["status"] for x in source_plan["unverified"]},
            {"p17": "received_unverified", "p18": "received_unverified"},
        )
        self.assertEqual(self.ledger.get_period("care", "03")["state"], "closing")

        self.ledger.confirm_clearing("p17")
        self.ledger.confirm_clearing("p18")
        closed = self.ledger.close_period(
            "zhou", "care", "03", {"clearing-1": 18}, phase="retry"
        )
        self.assertTrue(closed["ok"])
        self.assertTrue(closed["closed"])
        cert = closed["certificate"]
        self.assertEqual(cert["certificate_id"], "cert-care-03")
        self.assertEqual(self.ledger.get_period("care", "03")["state"], "closed")

        # 结账响应丢失后，相同重试和恢复查询都能确认“确实已冻结”。
        retry = self.ledger.close_period(
            "zhou", "care", "03", {"clearing-1": 18}, phase="retry"
        )
        self.assertTrue(retry["already_closed"])
        self.assertTrue(retry["ok"])
        self.assertTrue(retry["cuts_match"])
        changed_cut = self.ledger.close_period(
            "zhou", "care", "03", {"clearing-1": 19}, phase="retry"
        )
        self.assertTrue(changed_cut["already_closed"])
        self.assertFalse(changed_cut["ok"])
        self.assertFalse(changed_cut["cuts_match"])
        self.assertEqual(
            changed_cut["certificate"]["certificate_hash"], cert["certificate_hash"]
        )

        recovered = Ledger(self.db_path).recovery()
        self.assertIn(
            {"org": "care", "month": "03", "certificate_id": "cert-care-03"},
            recovered["closed_periods"],
        )
        statuses = {r["request_id"]: r["status"] for r in recovered["requests"]}
        self.assertEqual(statuses["p17"], "posted")
        self.assertEqual(statuses["p18"], "posted")

    def test_actor_cannot_close_other_org_period(self) -> None:
        self.receive_17_18()
        with self.assertRaises(UnauthorizedCloseError):
            self.ledger.close_period("lin", "care", "03", {"clearing-1": 18})
        # 授权失败不应把期间改成 closing。
        self.assertEqual(self.ledger.get_period("care", "03")["state"], "open")

    def test_closed_leg_blocks_whole_voucher_and_suggests_common_open_month(self) -> None:
        self.receive_17_18()
        self.ledger.confirm_clearing("p17")
        self.ledger.confirm_clearing("p18")
        self.ledger.close_period("zhou", "care", "03", {"clearing-1": 18})

        self.ledger.receive_clearing(
            "clearing-1",
            19,
            "p19",
            "03",
            "CNY",
            {"fund-east": -1500, "recv-care": 1500},
        )
        with self.assertRaises(PeriodConflictError) as ctx:
            self.ledger.confirm_clearing("p19")
        details = ctx.exception.details
        self.assertEqual(details["next_common_open_period"], "04")
        states = {(p["org"], p["state"]) for p in details["periods"]}
        self.assertIn(("care", "closed"), states)
        self.assertIn(("east", "open"), states)

        # 没有任何单边分录落库。
        with self.assertRaises(Exception):
            self.ledger.get_voucher("p19")
        blocked = self.ledger.get_request("p19")
        self.assertEqual(blocked["status"], "blocked")

        moved = self.ledger.confirm_clearing("p19", period_policy="next_common_open")
        self.assertTrue(moved["posted"])
        self.assertEqual(moved["voucher"]["effective_month"], "04")
        self.assertEqual(
            {(e["org"], e["month"]) for e in moved["voucher"]["entries"]},
            {("east", "04"), ("care", "04")},
        )

    def test_sequential_fixture_refund_and_idempotent_retry(self) -> None:
        r1 = self.ledger.create_refund("r1", "p16", 6000, "04")
        self.assertTrue(r1["ok"])
        amounts = {e["account"]: e["amount"] for e in r1["voucher"]["entries"]}
        self.assertEqual(
            amounts,
            {"recv-care": -6000, "fund-east": 4200, "fund-city": 1800},
        )

        with self.assertRaises(RefundLimitError) as ctx:
            self.ledger.create_refund("r2", "p16", 5000, "04")
        self.assertEqual(ctx.exception.details["available_amount"], 4000)

        retry = self.ledger.retry_refund("r1")
        self.assertTrue(retry["idempotent_replay"])
        self.assertEqual(retry["voucher"]["voucher_id"], "r1")
        period_04 = self.ledger.period_net("care", "04")
        self.assertEqual(period_04["balances"][0]["net_signed"], -6000)

    def test_concurrent_refunds_cannot_overdraw_refundable_balance(self) -> None:
        barrier = Barrier(2)
        results: list[dict[str, object] | BaseException] = []

        def refund(refund_id: str, amount: int) -> None:
            barrier.wait()
            try:
                results.append(self.ledger.create_refund(refund_id, "p16", amount, "04"))
            except BaseException as exc:  # capture in worker
                results.append(exc)

        with ThreadPoolExecutor(max_workers=2) as pool:
            future1 = pool.submit(refund, "cr1", 6000)
            future2 = pool.submit(refund, "cr2", 5000)
            future1.result()
            future2.result()

        successes = [r for r in results if isinstance(r, dict) and r.get("ok")]
        failures = [r for r in results if isinstance(r, RefundLimitError)]
        self.assertEqual(len(successes), 1, results)
        self.assertEqual(len(failures), 1, results)
        recovery = self.ledger.recovery()
        posted_total = sum(
            r["amount"] for r in recovery["refunds"] if r["status"] == "posted"
        )
        self.assertLessEqual(posted_total, 10000)
        self.assertEqual(posted_total, successes[0]["refund"]["amount"])

    def test_close_credential_is_stable_after_refund_and_restart(self) -> None:
        self.receive_17_18()
        self.ledger.confirm_clearing("p17")
        self.ledger.confirm_clearing("p18")
        closed = self.ledger.close_period(
            "zhou", "care", "03", {"clearing-1": 18}
        )
        cert_before = closed["certificate"]
        read_before = self.ledger.read_closed_certificate("care", "03")
        self.ledger.create_refund("r1", "p16", 6000, "04")

        restarted = Ledger(self.db_path)
        self.assertTrue(restarted.verify_chain()["ok"])
        read_after = restarted.read_closed_certificate("care", "03")
        self.assertEqual(
            canonical_json(read_before["snapshot"]),
            canonical_json(read_after["snapshot"]),
        )
        self.assertEqual(read_before["certificate_hash"], read_after["certificate_hash"])
        self.assertEqual(
            read_before["stable_read_credential"], read_after["stable_read_credential"]
        )
        self.assertEqual(cert_before["certificate_hash"], read_after["certificate_hash"])

        # 旧期间净额仍是关账时的 18000；更正落在 04 并通过原凭证链解释。
        net_03 = restarted.period_net("care", "03")
        self.assertEqual(net_03["balances"][0]["net_signed"], 18000)
        chain = restarted.service_chain("p16")
        chain_ids = [v["voucher_id"] for v in chain["voucher_chain"]]
        self.assertEqual(chain_ids, ["p16", "r1"])
        self.assertTrue(all(e["voucher_id"] != "r1" or e["month"] == "04"
                            for e in chain["effective_entries"]))

    def test_missing_source_sequence_blocks_close(self) -> None:
        ledger = Ledger(self.tmp / "gap.db")
        ledger.upsert_account("a1", "org-a", "payable", "CNY")
        ledger.upsert_account("b1", "org-b", "receivable", "CNY")
        ledger.upsert_period("org-a", "01", "open", "alice")
        ledger.upsert_period("org-b", "01", "open", "bob")
        ledger.set_source_cut("stream", 0)
        ledger.receive_clearing("stream", 1, "q1", "01", "CNY", {"a1": -100, "b1": 100})
        ledger.confirm_clearing("q1")
        result = ledger.close_period("bob", "org-b", "01", {"stream": 3})
        self.assertFalse(result["ok"])
        self.assertFalse(result["plan"]["can_close"])
        plan = result["plan"]["sources"][0]
        self.assertEqual(plan["missing"], [2, 3])


class HttpServerTest(FixtureLedgerTest):
    def test_http_actions_and_parallel_refunds(self) -> None:
        db_path = self.tmp / "http.db"
        load_sample(FIXTURES / "overlapping-close-and-refunds", db_path, "main")
        server = create_server(str(db_path), "127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        base = f"http://127.0.0.1:{server.server_address[1]}"

        def post(path: str, payload: dict) -> tuple[int, dict]:
            req = Request(
                base + path,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urlopen(req, timeout=10) as response:
                    return response.status, json.loads(response.read())
            except HTTPError as exc:
                return exc.code, json.loads(exc.read())

        status, health = self._get(base + "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        barrier = Barrier(2)

        def refund(payload: dict) -> tuple[int, dict]:
            barrier.wait()
            return post("/v1/refunds", payload)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(refund, {"refund_id": "h1", "original": "p16", "amount": 6000, "month": "04"})
            f2 = pool.submit(refund, {"refund_id": "h2", "original": "p16", "amount": 5000, "month": "04"})
            s1, b1 = f1.result()
            s2, b2 = f2.result()

        statuses = sorted([s1, s2])
        self.assertEqual(statuses, [200, 409])
        bodies = [b1, b2]
        self.assertEqual(sum(1 for b in bodies if b.get("ok")), 1)
        self.assertEqual(
            [b["error"]["code"] for b in bodies if not b.get("ok")],
            ["refund_limit_exceeded"],
        )

        status, net = self._get(base + "/v1/periods/care/04/net?chain=0")
        self.assertEqual(status, 200)
        self.assertEqual(net["org"], "care")

    @staticmethod
    def _get(url: str) -> tuple[int, dict]:
        try:
            with urlopen(url, timeout=10) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())


if __name__ == "__main__":
    unittest.main()
