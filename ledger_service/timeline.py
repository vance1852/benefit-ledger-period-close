"""仓库/fixture 格式装载与 timeline 重放辅助。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .ledger import Ledger


def bootstrap_fixture(
    source: str | Path,
    target: str | Path,
    role: str = "main",
    *,
    initialize: bool = True,
) -> dict[str, Any]:
    """按 catalog.json 找到 SQL，创建尚不存在的 SQLite 库并装载原始表。

    与仓库根目录 ``load_sample.py`` 的约束一致：``target`` 必须尚不存在。
    """

    source_dir = Path(source)
    catalog = json.loads((source_dir / "catalog.json").read_bytes())
    role_info = catalog["files"][role]
    script_path = source_dir / role_info["sql"]
    target_path = Path(target)

    with target_path.open("xb"):
        pass

    import sqlite3

    with sqlite3.connect(target_path) as db:
        db.executescript(script_path.read_text(encoding="utf-8"))
        if db.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise ValueError("SQLite 原始资料不完整")
        counts = {
            name: db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            for name in role_info["tables"]
        }

    if initialize:
        Ledger(target_path)
    return {"target": str(target_path), "tables": counts, "event_count": catalog.get("event_count")}


def load_timeline(path: str | Path) -> list[dict[str, Any]]:
    records = json.loads(Path(path).read_bytes())
    if not isinstance(records, list):
        raise ValueError("timeline.json 必须是记录数组")
    # 资料通常已有序；稳定排序保证同 at 的数组位置不被打乱。
    ordered = sorted(
        enumerate(records),
        key=lambda item: (item[1].get("at", 0), item[0]),
    )
    return [record for _, record in ordered]


def replay_timeline(
    ledger: Ledger,
    timeline: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """按 ``(at, 原数组位置)`` 顺序调用通用 action 入口。

    同一 at 的两条退款在资料中按数组位置决定先后；真正的并行竞争由 HTTP/线程测试覆盖。
    """

    observations: list[dict[str, Any]] = []
    records = list(timeline)
    ordered = sorted(
        enumerate(records),
        key=lambda item: (item[1].get("at", 0), item[0]),
    )
    for order_index, (original_index, record) in enumerate(ordered):
        action = record["action"]
        args = record.get("args", [])
        result = ledger.handle_action(action, args)
        observations.append(
            {
                "order": order_index + 1,
                "original_index": original_index,
                "at": record.get("at"),
                "action": action,
                "args": args,
                "result": result,
            }
        )
    return observations
