"""定义进入统一时间线的事件类型与载荷校验。

所有外部世界的变化（到达、甩箱、进展、故障、封锁）都先归一化为
TimelineEvent 再追加，业务推导只依赖事件内容，不依赖消息到达顺序。
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ValidationError

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")

EVENT_TYPES = frozenset({
    "arrival.early",
    "arrival.confirmed",
    "arrival.delayed",
    "arrival.skipped",
    "work.partial",
    "equipment.failed",
    "equipment.recovered",
})

ARRIVAL_TYPES = frozenset({"arrival.early", "arrival.confirmed", "arrival.delayed"})


def parse_ts(value: str, field: str = "时间") -> datetime:
    """把 ISO-8601 文本解析为带 UTC 时区的时间。"""

    if not isinstance(value, str):
        raise ValidationError(f"{field}必须是 ISO-8601 文本")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field}不是有效的 ISO-8601 时间") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field}必须携带时区")
    return parsed.astimezone(timezone.utc)


def format_ts(value: datetime) -> str:
    """输出统一的 UTC 文本。"""

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def add_minutes(value: datetime, minutes: int | float) -> datetime:
    return value + timedelta(minutes=minutes)


def _identifier(value: Any, field: str) -> str:
    value = str(value).strip()
    if not IDENTIFIER.fullmatch(value):
        raise ValidationError(f"{field} 格式无效")
    return value


def _container_ids(payload: dict[str, Any]) -> tuple[str, ...]:
    raw = payload.get("container_ids")
    if not isinstance(raw, list) or not raw:
        raise ValidationError("container_ids 必须是非空数组")
    result: list[str] = []
    for item in raw:
        result.append(_identifier(item, "container_id"))
    if len(set(result)) != len(result):
        raise ValidationError("container_ids 不能重复")
    return tuple(result)


def normalize_event(data: dict[str, Any]) -> dict[str, Any]:
    """校验事件载荷并返回规范化后的字典。

    规范化只做字段排序/时间统一等无害转换，不改变业务含义，
    从而保证相同事件以不同文本形式重放时摘要一致。
    """

    event_type = str(data.get("event_type", "")).strip()
    if event_type not in EVENT_TYPES:
        raise ValidationError("event_type 不被支持")
    occurred = parse_ts(data.get("occurred_at", ""), "occurred_at")
    payload: dict[str, Any] = {"event_type": event_type, "occurred_at": format_ts(occurred)}

    if event_type in ARRIVAL_TYPES:
        payload["batch_id"] = _identifier(data.get("batch_id", ""), "batch_id")
        payload["arrival_at"] = format_ts(parse_ts(data.get("arrival_at", ""), "arrival_at"))
    elif event_type == "arrival.skipped":
        payload["batch_id"] = _identifier(data.get("batch_id", ""), "batch_id")
        payload["container_ids"] = list(_container_ids(data))
        payload["reason"] = str(data.get("reason", "")).strip()[:200]
    elif event_type == "work.partial":
        payload["shipment_id"] = _identifier(data.get("shipment_id", ""), "shipment_id")
        payload["container_ids"] = list(_container_ids(data))
    elif event_type in ("equipment.failed", "equipment.recovered"):
        payload["resource_id"] = _identifier(data.get("resource_id", ""), "resource_id")
        if event_type == "equipment.failed":
            payload["end_ts"] = format_ts(parse_ts(data.get("end_ts", ""), "end_ts"))

    sequence = data.get("sequence", 0)
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise ValidationError("sequence 必须是非负整数")
    payload["sequence"] = sequence
    return payload
