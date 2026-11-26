"""账本服务命令行入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from .ledger import Ledger
from .server import serve
from .timeline import bootstrap_fixture, replay_timeline


def print_json(value: Any) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2, sort_keys=False)
    sys.stdout.write("\n")


def _load_args_json(text: str | None) -> list[Any]:
    if not text:
        return []
    value = json.loads(text)
    if not isinstance(value, list):
        raise SystemExit("--args-json 必须是 JSON 数组，例如 '[\"p17\"]'")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ledger_service",
        description="跨机构服务券账本：清算收件/确认、平衡过账、分期关账、跨期红冲和查询",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="在已有/新建 SQLite 文件上初始化账本扩展表")
    p_init.add_argument("database")

    p_boot = sub.add_parser("bootstrap", help="从 fixtures 目录装载 main.sql 并初始化账本")
    p_boot.add_argument("source", type=Path)
    p_boot.add_argument("database", type=Path)
    p_boot.add_argument("--role", default="main")

    p_serve = sub.add_parser("serve", help="启动本地 HTTP/JSON 服务")
    p_serve.add_argument("database")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8787)

    p_action = sub.add_parser("action", help="调用 timeline 通用动作")
    p_action.add_argument("database")
    p_action.add_argument("action")
    p_action.add_argument("--args-json", help="位置参数 JSON 数组", default=None)

    p_replay = sub.add_parser("replay", help="装载 fixture（如需）并重放 timeline.json")
    p_replay.add_argument("source", type=Path)
    p_replay.add_argument("database", type=Path)
    p_replay.add_argument("--role", default="main")
    p_replay.add_argument("--output", type=Path, default=None)

    p_net = sub.add_parser("net", help="查机构期间净额和凭证链")
    p_net.add_argument("database")
    p_net.add_argument("org")
    p_net.add_argument("month")
    p_net.add_argument("--no-chain", action="store_true")

    p_service = sub.add_parser("service", help="查一笔服务/凭证在各机构期间的有效分录")
    p_service.add_argument("database")
    p_service.add_argument("ref")

    p_closed = sub.add_parser("closed", help="读取稳定结账凭据")
    p_closed.add_argument("database")
    p_closed.add_argument("org")
    p_closed.add_argument("month")

    p_recover = sub.add_parser("recover", help="恢复检查：期间冻结状态、申请过账/待补办状态")
    p_recover.add_argument("database")

    p_verify = sub.add_parser("verify-chain", help="校验凭证哈希链")
    p_verify.add_argument("database")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "serve":
        serve(args.database, args.host, args.port)
        return 0

    if args.command == "bootstrap":
        print_json(bootstrap_fixture(args.source, args.database, args.role))
        return 0

    if args.command == "replay":
        if not Path(args.database).exists():
            bootstrap_fixture(args.source, args.database, args.role)
        ledger = Ledger(args.database)
        timeline_path = args.source / "timeline.json"
        timeline = json.loads(timeline_path.read_bytes())
        observations = replay_timeline(ledger, timeline)
        summary = {
            "ok": True,
            "observation_count": len(observations),
            "observations": observations,
            "recovery": ledger.recovery(),
        }
        text = json.dumps(summary, ensure_ascii=False, indent=2)
        if args.output:
            args.output.write_text(text + "\n", encoding="utf-8")
        print_json(summary)
        return 0

    ledger = Ledger(args.database)

    if args.command == "init":
        print_json({"ok": True, "database": str(args.database), "schema_version": "1"})
    elif args.command == "action":
        print_json(ledger.handle_action(args.action, _load_args_json(args.args_json)))
    elif args.command == "net":
        print_json(ledger.period_net(args.org, args.month, include_chain=not args.no_chain))
    elif args.command == "service":
        print_json(ledger.service_chain(args.ref))
    elif args.command == "closed":
        print_json(ledger.read_closed_certificate(args.org, args.month))
    elif args.command == "recover":
        print_json(ledger.recovery())
    elif args.command == "verify-chain":
        print_json(ledger.verify_chain())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
