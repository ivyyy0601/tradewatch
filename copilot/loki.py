"""查日志：通过 Loki 的 HTTP 接口（不需要进服务器、不需要 docker 权限，只读）"""

import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from copilot import config, topology

LEVELS = {"DEBUG", "INFO", "WARN", "ERROR"}


def build_query(component, contains="", levels=None):
    cont = topology.container(component)
    if not re.match(r"^[a-z0-9-]+$", cont):
        raise ValueError(f"bad component {component!r}")
    sel = f'{{container="{cont}"'
    lv = [l for l in (levels or []) if l in LEVELS]
    if lv and set(lv) != LEVELS:
        sel += f', level=~"{"|".join(lv)}"'
    sel += "}"
    if contains:
        safe = str(contains).replace("\\", "").replace('"', "")[:60]
        sel += f' |= "{safe}"'
    return sel


def search(component, start, end, contains="", levels=None, limit=30):
    """返回 [(时间, 一行日志)]，最新的在前"""
    q = build_query(component, contains, levels)
    params = urllib.parse.urlencode({
        "query": q, "limit": max(1, min(int(limit), 200)), "direction": "backward",
        "start": str(int(start.timestamp() * 1e9)), "end": str(int(end.timestamp() * 1e9)),
    })
    with urllib.request.urlopen(f"{config.LOKI_URL}/loki/api/v1/query_range?{params}", timeout=10) as r:
        data = json.load(r)
    out = []
    for stream in data.get("data", {}).get("result", []):
        for ts, line in stream.get("values", []):
            out.append((int(ts), line))
    out.sort(reverse=True)
    return q, [(datetime.fromtimestamp(t / 1e9).astimezone().isoformat(timespec="seconds"), compact(l))
               for t, l in out[:limit]]


def compact(line, n=400):
    """日志行太长就截短；JSON 日志去掉没用的字段"""
    try:
        d = json.loads(line)
        if isinstance(d, dict):
            d.pop("ts", None)
            line = json.dumps(d, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        pass
    return line if len(line) <= n else line[:n] + "…"


def window(as_of, minutes_before, minutes_after):
    return as_of - timedelta(minutes=minutes_before), as_of + timedelta(minutes=minutes_after)
