"""时间与单位换算辅助。资料时刻带 +08:00，仓内统一存 UTC 时刻。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

CN_TZ = timezone(timedelta(hours=8))
UTC = timezone.utc


def parse_dt(value: str) -> datetime:
    """解析带时区的 ISO 时刻并转为 UTC。"""
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError(f"时刻缺少时区：{value}")
    return stamp.astimezone(UTC)


def iso8(stamp: datetime) -> str:
    """按 +08:00 输出 ISO 字符串。"""
    if stamp is None:
        return None
    return stamp.astimezone(CN_TZ).isoformat()


def day8(stamp: datetime):
    """按 +08:00 取业务日期（跨午夜归当天）。"""
    return stamp.astimezone(CN_TZ).date()


def f_to_c(value: float) -> float:
    """华氏转摄氏，标准关系，保留 9 位小数避免浮点尾差。"""
    return round((value - 32.0) * 5.0 / 9.0, 9)


def split_by_midnight(start: datetime, end: datetime):
    """按 +08:00 午夜把 [start, end) 切成 (day, s, e) 列表，左闭右开。"""
    parts = []
    cur = start
    while cur < end:
        d = day8(cur)
        next_day = datetime(d.year, d.month, d.day, tzinfo=CN_TZ) + timedelta(days=1)
        nxt = min(next_day.astimezone(UTC), end)
        parts.append((d, cur, nxt))
        cur = nxt
    return parts
