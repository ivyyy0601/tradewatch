"""数据库：copilot 自己的表（读写），以及给 AI 用的只读查询"""

import json
import os
import re
from datetime import date, datetime, timedelta
from decimal import Decimal

import yaml

from common import connect, log
from copilot import config

SERVICE = "copilot"
_QUERIES = None


def jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: jsonable(x) for k, x in v.items()}
    return v


def rows(cur, sql, params=None):
    """执行 SQL，返回字典列表"""
    cur.execute(sql, params)
    if cur.description is None:
        return []
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(cur, sql, params=None):
    r = rows(cur, sql, params)
    return r[0] if r else None


def as_json(v):
    """写进 JSONB 列"""
    return json.dumps(jsonable(v), ensure_ascii=False)


def open_rw():
    db = connect(SERVICE)
    db.autocommit = True
    return db


def open_ro():
    """给 AI 工具用的连接：只读事务 + 每条 SQL 最多跑 5 秒"""
    db = connect(SERVICE + "-ro")
    if hasattr(db, "set_session"):
        db.set_session(readonly=True, autocommit=True)
    cur = db.cursor()
    try:
        cur.execute(f"SET statement_timeout = {int(config.QUERY_TIMEOUT_MS)}")
    except Exception:                                   # noqa: BLE001
        pass
    return db


def ensure_schema(cur):
    path = os.path.join(os.path.dirname(config.HERE), "sql", "03_copilot.sql")
    with open(path) as fh:
        sql = fh.read()
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            cur.execute(body)
    log(SERVICE, "INFO", "schema_ready")


# ============================================================
# 命名查询（AI 只能用这些）
# ============================================================
def queries():
    global _QUERIES
    if _QUERIES is None:
        with open(os.path.join(config.HERE, "queries.yaml")) as fh:
            _QUERIES = yaml.safe_load(fh)
        for name, q in _QUERIES.items():
            sql = q["sql"].strip().rstrip(";")
            if not re.match(r"^(SELECT|WITH)\b", sql, re.I) or ";" in sql:
                raise SystemExit(f"query {name} must be a single SELECT")
            q["sql"] = sql
            q.setdefault("params", {})
    return _QUERIES


class BadParams(ValueError):
    pass


def _check(name, spec, value):
    from config import BROKERS, PMS, SYMBOLS
    t = spec.get("type", "text")
    if value is None or value == "":
        if "default" in spec:
            return spec["default"]
        raise BadParams(f"missing parameter {name}")
    if t == "int":
        try:
            v = int(value)
        except (TypeError, ValueError):
            raise BadParams(f"{name} must be an integer") from None
        return max(spec.get("min", v), min(spec.get("max", v), v))
    v = str(value).strip()
    allowed = {"pm": set(PMS), "pm_or_all": set(PMS) | {"ALL"}, "broker": set(BROKERS), "symbol": set(SYMBOLS)}.get(t)
    if allowed is not None and v not in allowed:
        raise BadParams(f"{name} must be one of {sorted(allowed)}, got {v!r}")
    if len(v) > 80:
        raise BadParams(f"{name} too long")
    return v


def named_query(cur, name, params, as_of):
    """跑一条命名查询，返回 (行, 是否被截断)"""
    q = queries().get(name)
    if q is None:
        raise BadParams(f"unknown query {name!r}; available: {', '.join(sorted(queries()))}")
    params = params or {}
    unknown = set(params) - set(q["params"])
    if unknown:
        raise BadParams(f"unknown parameters {sorted(unknown)} for {name}; expected {sorted(q['params'])}")
    bound = {k: _check(k, spec, params.get(k)) for k, spec in q["params"].items()}
    bound["as_of"] = as_of.isoformat() if isinstance(as_of, datetime) else str(as_of)
    out = rows(cur, q["sql"], bound)
    truncated = len(out) > config.QUERY_MAX_ROWS
    return [jsonable(r) for r in out[:config.QUERY_MAX_ROWS]], truncated
