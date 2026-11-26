"""通用编码、哈希和整数金额工具。"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Mapping


def canonical_json(value: Any) -> str:
    """返回排序键、无多余空白的规范 JSON。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def fingerprint(value: Any) -> str:
    """规范载荷的 SHA-256，用于幂等指纹和凭证哈希。"""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def token(payload: Mapping[str, Any]) -> str:
    """生成不带密钥的本地读凭据 token。

    这里的目标是可复制、可校验和稳定，而不是跨信任边界的安全签名。
    """

    raw = canonical_json(payload).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def parse_token(value: str) -> dict[str, Any]:
    padding = "=" * (-len(value) % 4)
    raw = base64.urlsafe_b64decode(value + padding)
    result = json.loads(raw.decode("utf-8"))
    if not isinstance(result, dict):
        raise ValueError("credential token is not an object")
    return result


def require_int_amount(value: Any, *, field: str = "amount") -> int:
    # bool 是 int 的子类，但不能作为金额接受。
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} 必须是整数分")
    return value


def largest_remainder(total: int, weights: Mapping[Any, int]) -> dict[Any, int]:
    """按非负整数权重把 ``total`` 分为整数，并保证各项之和恰为 total。"""

    if total < 0:
        raise ValueError("total must be non-negative")
    items = [(key, int(weight)) for key, weight in weights.items()]
    if any(weight < 0 for _, weight in items):
        raise ValueError("weights must be non-negative")
    weight_sum = sum(weight for _, weight in items)
    if total == 0 or weight_sum == 0:
        return {key: 0 for key, _ in items}

    quotas: dict[Any, float] = {}
    allocated: dict[Any, int] = {}
    remainders: list[tuple[float, Any]] = []
    raw = total / weight_sum
    for key, weight in items:
        exact = raw * weight
        base = int(exact)
        allocated[key] = base
        quotas[key] = exact
        remainders.append((exact - base, key))

    left = total - sum(allocated.values())
    # 余数从大到小分配；并列时按键排序，保证结果确定。
    remainders.sort(key=lambda item: (-item[0], str(item[1])))
    for index in range(left):
        _, key = remainders[index % len(remainders)]
        allocated[key] += 1
    return allocated


def next_month_candidate(month: str) -> str | None:
    """仅用于常见零填充月份 ID 的递增；找不到格式时返回 None。"""

    if len(month) == 2 and month.isdigit():
        number = int(month)
        if 1 <= number <= 11:
            return f"{number + 1:02d}"
        return None
    if len(month) == 7 and month[4] == "-":
        year_text, mm = month.split("-", 1)
        if year_text.isdigit() and mm.isdigit() and 1 <= int(mm) <= 12:
            year = int(year_text)
            number = int(mm)
            if number < 12:
                return f"{year:04d}-{number + 1:02d}"
            return f"{year + 1:04d}-01"
    return None
