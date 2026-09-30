"""统一的 UTC 时间文本工具：解析、按分钟取整、区间运算。

系统内所有时间均以 ISO8601 UTC 文本（Z 结尾）存储与比较，
跨午夜计划不会出现日期回绕问题。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .errors import ValidationError


def parse_ts(value: str, field: str = "时间") -> datetime:
    """把 ISO8601 文本解析为带 UTC 时区的时间。"""

    if not isinstance(value, str):
        raise ValidationError(f"{field} 必须是 ISO8601 字符串")
    text = value.strip()
    if not text:
        raise ValidationError(f"{field} 不能为空")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field} 不是合法的 ISO8601 时间") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field} 必须携带时区偏移")
    return parsed.astimezone(timezone.utc)


def format_ts(value: datetime) -> str:
    """格式化为 Z 结尾的 UTC 文本。"""

    return value.astimezone(timezone.utc).isoformat(timespec="minutes").replace("+00:00", "Z")


def parse_minute(value: str, field: str = "时间") -> datetime:
    """解析并按分钟向下取整（调度以分钟为最小刻度）。"""

    parsed = parse_ts(value, field).replace(second=0, microsecond=0)
    return parsed


def add_minutes(value: datetime, minutes: int) -> datetime:
    return value + timedelta(minutes=minutes)


def minute_diff(start: datetime, end: datetime) -> int:
    """两个 UTC 时间相差的整分钟数（end - start）。"""

    return int((end - start).total_seconds() // 60)


def overlaps(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    """半开区间 [start, end) 是否重叠；首尾相接不算冲突。"""

    return start_a < end_b and start_b < end_a
