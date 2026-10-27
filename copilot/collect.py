"""取证：按剧本（playbooks.yaml）把固定要看的证据收集好，打包成证据包（纯代码，不用 AI）"""

import json
import os
from datetime import datetime

import yaml

from common import log
from copilot import config, db, loki, topology, tools

SERVICE = "copilot"
_PB = None


def playbooks():
    global _PB
    if _PB is None:
        with open(os.path.join(config.HERE, "playbooks.yaml")) as fh:
            _PB = yaml.safe_load(fh)
    return _PB


def _fill(v, ctx):
    if isinstance(v, str):
        return v.format(**ctx)
    if isinstance(v, dict):
        return {k: _fill(x, ctx) for k, x in v.items()}
    if isinstance(v, list):
        return [_fill(x, ctx) for x in v]
    return v


def plan(alerts):
    """一个事故要跑哪些步骤（多条告警的剧本合并，重复的只跑一次）"""
    pb = playbooks()
    steps, seen = [], set()
    for a in alerts:
        ctx = topology.context(a)
        for raw in pb["rules"].get(a["rule"], []) + pb["defaults"]:
            try:
                s = _fill(raw, ctx)
            except (KeyError, IndexError):
                continue
            if "query" in s and any(v == "" for v in (s.get("params") or {}).values()):
                continue                                         # 参数填不上（比如告警里没有 PM）就跳过
            if "logs" in s and not s["logs"].get("service"):
                continue
            sig = json.dumps(s, sort_keys=True)
            if sig not in seen:
                seen.add(sig)
                steps.append(s)
    return steps


def gather(cur_ro, incident, alerts, as_of):
    """返回证据包（dict）"""
    comps = set()
    for a in alerts:
        comps |= topology.components(a)
    evidence = {
        "incident": {"incident_id": incident["incident_id"], "title": incident["title"], "severity": incident["severity"],
                     "opened_at": incident["opened_at"], "as_of": as_of.isoformat()},
        "alerts": [{k: a.get(k) for k in ("alert_id", "rule", "key", "severity", "pm_id", "fired_at", "resolved_at", "detail")}
                   for a in alerts],
        "topology": topology.describe(comps),
        "sections": [],
        "runbooks": {},
    }
    for s in plan(alerts):
        if "note" in s:
            evidence["sections"].append({"title": "提示", "note": s["note"]})
            continue
        if "query" in s:
            title = f"query {s['query']} {json.dumps(s.get('params') or {}, ensure_ascii=False)}"
            try:
                out, truncated = db.named_query(cur_ro, s["query"], s.get("params") or {}, as_of)
                evidence["sections"].append({"title": title, "rows": out, "truncated": truncated})
            except Exception as e:                               # noqa: BLE001
                evidence["sections"].append({"title": title, "error": str(e)[:300]})
            continue
        lg = s["logs"]
        title = f"logs {lg['service']} contains={lg.get('contains', '')!r} level={lg.get('level')}"
        try:
            start, end = loki.window(as_of, lg.get("minutes_before", 15), lg.get("minutes_after", 2))
            q, lines = loki.search(lg["service"], start, end, lg.get("contains", ""), lg.get("level"), lg.get("limit", 30))
            evidence["sections"].append({"title": title, "logql": q, "lines": lines})
        except Exception as e:                                   # noqa: BLE001  Loki 不可用时证据包照样生成
            evidence["sections"].append({"title": title, "error": f"logs unavailable: {str(e)[:200]}"})
    for rule in sorted({a["rule"] for a in alerts}):
        text = tools.runbook_text(rule)
        if text:
            evidence["runbooks"][rule] = text
    shrink(evidence)
    log(SERVICE, "INFO", "evidence_collected", incident_id=incident["incident_id"],
        sections=len(evidence["sections"]), chars=len(json.dumps(db.jsonable(evidence), ensure_ascii=False)))
    return db.jsonable(evidence)


def shrink(evidence, limit=None):
    """证据包太大就逐步砍掉每段的行数，保证能放进 AI 的上下文"""
    limit = limit or config.EVIDENCE_CHARS
    size = lambda: len(json.dumps(db.jsonable(evidence), ensure_ascii=False, default=str))
    keep = 40
    while size() > limit and keep >= 3:
        for sec in evidence["sections"]:
            for k in ("rows", "lines"):
                if k in sec and len(sec[k]) > keep:
                    sec[k] = sec[k][:keep]
                    sec["truncated"] = True
        keep //= 2
    for rb in list(evidence["runbooks"]):
        if size() > limit:
            evidence["runbooks"][rb] = evidence["runbooks"][rb][:1500] + "…"


def summary_text(evidence, max_sections=8):
    """给人看的证据摘要（没有 AI 时用）"""
    lines = []
    for sec in evidence["sections"]:
        if "note" in sec:
            continue
        if "error" in sec:
            lines.append(f"· {sec['title']}: 出错 {sec['error'][:80]}")
        elif "rows" in sec:
            lines.append(f"· {sec['title']}: {len(sec['rows'])} 行")
        elif "lines" in sec:
            lines.append(f"· {sec['title']}: {len(sec['lines'])} 条日志")
        if len(lines) >= max_sections:
            break
    return "\n".join(lines)
