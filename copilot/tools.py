"""工具层：AI 能用的全部工具都在这里注册。每个工具有：名字、说明、参数格式、权限级别、结果长度上限
权限级别：read = 只读，AI 可以直接用；suggest = 只生成"建议"，要人批准后才会执行
AI 拿不到任何能直接改系统的工具（真正执行操作在 approvals.py，只有人批准后才调用）"""

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime

from config import PMS
from copilot import config, db, loki, topology

ACTIONS = {
    "pause_pm": "暂停一个 PM（不再生成新母单，已有母单继续执行）",
    "cancel_parent": "撤掉一个执行中的母单和它挂在券商的子单",
}


@dataclass
class Ctx:
    cur_ro: object                     # 只读连接
    cur_rw: object                     # 写 copilot 自己的表（记录工具调用）
    as_of: datetime
    investigation_id: int = 0
    steps: int = 0
    proposals: list = field(default_factory=list)


@dataclass
class Tool:
    name: str
    description: str
    schema: dict
    level: str
    fn: object


REGISTRY = {}


def tool(name, description, properties=None, required=(), level="read"):
    def deco(fn):
        REGISTRY[name] = Tool(name, description, {"type": "object", "properties": properties or {},
                                                  "required": list(required)}, level, fn)
        return fn
    return deco


def specs(levels=("read", "suggest")):
    """给 Claude API 的 tools 参数"""
    return [{"name": t.name, "description": t.description, "input_schema": t.schema}
            for t in REGISTRY.values() if t.level in levels]


def execute(ctx, name, args):
    """执行一个工具，记录审计日志，返回给 AI 的文本（有长度上限）"""
    t = REGISTRY.get(name)
    ctx.steps += 1
    t0 = time.time()
    ok = True
    try:
        if t is None:
            raise ValueError(f"unknown tool {name}")
        result = t.fn(ctx, **(args or {}))
    except TypeError as e:
        ok, result = False, {"error": f"bad arguments: {e}"}
    except db.BadParams as e:
        ok, result = False, {"error": str(e)}
    except Exception as e:                                       # noqa: BLE001  工具出错不能让整个调查崩掉
        ok, result = False, {"error": f"{type(e).__name__}: {str(e)[:300]}"}
        try:
            ctx.cur_ro.connection.rollback()
        except Exception:                                        # noqa: BLE001
            pass
    text = result if isinstance(result, str) else json.dumps(db.jsonable(result), ensure_ascii=False, default=str)
    if len(text) > config.TOOL_RESULT_CHARS:
        text = text[:config.TOOL_RESULT_CHARS] + f"…（结果太长，只显示前 {config.TOOL_RESULT_CHARS} 个字符）"
    if ctx.investigation_id:
        try:
            ctx.cur_rw.execute(
                "INSERT INTO investigation_steps (investigation_id, step_no, tool, args, ok, result_chars, duration_ms) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (ctx.investigation_id, ctx.steps, name, db.as_json(args or {}), ok, len(text),
                 int((time.time() - t0) * 1000)))
        except Exception:                                        # noqa: BLE001
            pass
    return ok, text


# ============================================================
# 只读工具
# ============================================================
@tool("get_system_status", "系统整体状态：各 PM 是否暂停 / 敞口 / 盈亏、FIX 连接、服务心跳、行情新鲜度、最近 10 分钟 Kafka 积压")
def get_system_status(ctx):
    out = {}
    for name, params in (("pm_state", {}), ("fix_sessions", {}), ("heartbeats", {}),
                         ("market_data_freshness", {"minutes": 10}), ("kafka_lag_window", {"minutes": 10})):
        out[name], _ = db.named_query(ctx.cur_ro, name, params, ctx.as_of)
    out["note"] = "pm_state / fix_sessions / heartbeats 是当前值；回放过去的事故时仅供参考"
    return out


@tool("trace_order", "一张单的完整经历：母单信息、子单、每个订单事件、和券商的 FIX 消息。母单号（P 开头）或子单号（C 开头）都可以",
      {"order_id": {"type": "string", "description": "例如 PB44FB9A8 或 C40A441F9F"}}, ["order_id"])
def trace_order(ctx, order_id):
    oid = str(order_id).strip().upper()[:20]
    cur = ctx.cur_ro
    pid = oid
    if oid.startswith("C"):
        r = db.one(cur, "SELECT parent_id FROM child_orders WHERE child_id = %s", (oid,))
        if not r:
            return {"error": f"no child order {oid}"}
        pid = r["parent_id"]
    parent = db.one(cur, "SELECT * FROM parent_orders WHERE parent_id = %s", (pid,))
    if not parent:
        return {"error": f"no parent order {pid}"}
    return {
        "parent": parent,
        "children": db.rows(cur, "SELECT child_id, broker, qty, limit_price, status, filled_qty, avg_price, reason, created_at "
                                 "FROM child_orders WHERE parent_id = %s ORDER BY created_at LIMIT 30", (pid,)),
        "events": db.rows(cur, "SELECT ts, service, event_type, child_id, detail FROM order_events "
                               "WHERE parent_id = %s ORDER BY ts, event_id LIMIT 60", (pid,)),
        "fix": db.rows(cur, "SELECT f.ts, f.direction, f.broker, f.msg_type, f.raw FROM fix_messages f "
                            "JOIN child_orders c ON c.child_id = f.cl_ord_id WHERE c.parent_id = %s ORDER BY f.ts LIMIT 40", (pid,)),
    }


@tool("run_query", "运行一条命名查询（只能用查询库里的，不能自己写 SQL）。查询都以调查时间点 as_of 为准往前看。"
      "可用的查询和参数见系统提示里的查询库",
      {"name": {"type": "string", "description": "查询名，例如 pm_order_size_profile"},
       "params": {"type": "object", "description": "查询参数，例如 {\"pm_id\": \"PM_ALPHA\", \"hours\": 6}"}}, ["name"])
def run_query(ctx, name, params=None):
    out, truncated = db.named_query(ctx.cur_ro, name, params or {}, ctx.as_of)
    return {"query": name, "rows": out, "truncated": truncated, "row_count": len(out)}


@tool("search_logs", "在 Loki 里查某个部件在调查时间点前后的日志。service 是拓扑里的部件名：market-data、oms、ems、fix-gateway、"
      "order-store、monitor、kafka、broker:NBRG、broker:HRBR。contains 是要包含的文字（可以空），level 可选 INFO/WARN/ERROR",
      {"service": {"type": "string"}, "contains": {"type": "string"},
       "level": {"type": "array", "items": {"type": "string", "enum": ["INFO", "WARN", "ERROR"]}},
       "minutes_before": {"type": "integer", "minimum": 1, "maximum": 1440},
       "minutes_after": {"type": "integer", "minimum": 0, "maximum": 60},
       "limit": {"type": "integer", "minimum": 1, "maximum": 80}}, ["service"])
def search_logs(ctx, service, contains="", level=None, minutes_before=30, minutes_after=5, limit=40):
    if service not in topology.topo()["components"]:
        return {"error": f"unknown service {service!r}; use one of {sorted(topology.topo()['components'])}"}
    start, end = loki.window(ctx.as_of, int(minutes_before), int(minutes_after))
    q, lines = loki.search(service, start, end, contains, level or ["INFO", "WARN", "ERROR"], int(limit))
    return {"logql": q, "from": start.isoformat(), "to": end.isoformat(), "lines": lines,
            "note": "最新的在前；没有结果不一定代表没发生，也可能是日志里没打这个词"}


@tool("read_runbook", "读某条告警规则的处理手册（docs/runbooks/<rule>.md），里面有人写的排查步骤和经验",
      {"rule": {"type": "string"}}, ["rule"])
def read_runbook(ctx, rule):
    return runbook_text(rule) or {"note": f"还没有 {rule} 的 runbook"}


@tool("similar_incidents", "以前同类型的事故：当时的告警、AI 的判断、值班的人写的处理说明（帮助识别反复出现的问题）",
      {"rule": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 10}}, ["rule"])
def similar_incidents(ctx, rule, limit=5):
    return db.rows(ctx.cur_ro, """
        SELECT i.incident_id, i.title, i.opened_at, i.resolved_at,
               (SELECT v.category FROM investigations v WHERE v.incident_id = i.incident_id ORDER BY v.investigation_id DESC LIMIT 1) AS category,
               (SELECT v.root_cause FROM investigations v WHERE v.incident_id = i.incident_id ORDER BY v.investigation_id DESC LIMIT 1) AS root_cause,
               (SELECT string_agg(DISTINCT a.note, ' | ') FROM incident_alerts ia JOIN alerts a ON a.alert_id = ia.alert_id
                 WHERE ia.incident_id = i.incident_id AND a.note IS NOT NULL) AS human_notes
        FROM incidents i
        WHERE i.opened_at < %s::timestamptz - interval '10 minutes'
          AND EXISTS (SELECT 1 FROM incident_alerts ia WHERE ia.incident_id = i.incident_id AND ia.rule = %s)
        ORDER BY i.opened_at DESC LIMIT %s""", (ctx.as_of.isoformat(), str(rule)[:40], max(1, min(int(limit), 10))))


@tool("describe_component", "拓扑：某个部件是做什么的、上下游是谁、读写哪些 Kafka topic",
      {"name": {"type": "string"}}, ["name"])
def describe_component(ctx, name):
    node = topology.topo()["components"].get(name)
    if not node:
        return {"error": f"unknown component; available: {sorted(topology.topo()['components'])}"}
    return {"name": name, **node}


# ============================================================
# 建议类工具：只记录建议，不执行
# ============================================================
@tool("propose_action", "建议一个操作，交给值班的人批准。不会立刻执行。只在有明确证据说明继续下去会造成损失时才建议。"
      "action 只能是 pause_pm（target 填 PM 名）或 cancel_parent（target 填母单号）。永远不能建议恢复 PM",
      {"action": {"type": "string", "enum": list(ACTIONS)}, "target": {"type": "string"},
       "reason": {"type": "string", "description": "为什么要这么做，引用证据"}},
      ["action", "target", "reason"], level="suggest")
def propose_action(ctx, action, target, reason):
    ok, why = validate_action(ctx.cur_ro, action, target)
    if not ok:
        return {"accepted": False, "why": why}
    if any(p["action"] == action and p["target"] == target for p in ctx.proposals):
        return {"accepted": True, "note": "已经建议过了"}
    ctx.proposals.append({"action": action, "target": str(target).strip(), "reason": str(reason)[:500]})
    return {"accepted": True, "note": "已记录为建议，等待人批准；不会自动执行"}


def validate_action(cur, action, target):
    """硬规则写在代码里，不靠 AI 自觉"""
    target = str(target).strip()
    if action not in ACTIONS:
        return False, f"action must be one of {list(ACTIONS)} (resuming a PM is never allowed)"
    if action == "pause_pm":
        if target not in PMS:
            return False, f"unknown PM {target}"
        r = db.one(cur, "SELECT paused FROM control WHERE pm_id = %s", (target,))
        if r and r["paused"]:
            return False, f"{target} is already paused"
        return True, ""
    r = db.one(cur, "SELECT status FROM parent_orders WHERE parent_id = %s", (target.upper(),))
    if not r:
        return False, f"no parent order {target}"
    if r["status"] != "WORKING":
        return False, f"parent {target} is {r['status']}, nothing to cancel"
    return True, ""


def runbook_text(rule):
    path = os.path.join(config.DOCS_DIR, "runbooks", f"{os.path.basename(str(rule))}.md")
    try:
        with open(path) as fh:
            return fh.read()[:6000]
    except OSError:
        return ""
