"""建议 → 人批准 → 执行。执行就是往 Kafka 的 ops.commands 发命令（和 ./tw pause / 网页按钮发的完全一样）
硬规则写在代码里：只能暂停 PM 或撤母单；永远不能恢复 PM；批准前再检查一遍目标还有效；过期作废"""

from datetime import datetime, timezone

from common import log, now
from copilot import config, db, notify, tools

SERVICE = "copilot"


def create(cur, incident_id, investigation_id, proposals):
    ids = []
    for p in proposals:
        r = db.one(cur, """
            INSERT INTO action_proposals (incident_id, investigation_id, action, target, reason)
            VALUES (%s, %s, %s, %s, %s) RETURNING proposal_id""",
            (incident_id, investigation_id, p["action"], p["target"], p["reason"]))
        ids.append(r["proposal_id"])
        log(SERVICE, "WARN", "action_proposed", proposal_id=r["proposal_id"], incident_id=incident_id,
            action=p["action"], target=p["target"])
    return ids


def describe(p):
    verb = {"pause_pm": "暂停", "cancel_parent": "撤掉母单"}.get(p["action"], p["action"])
    return f"#{p['proposal_id']} {verb} {p['target']}：{p['reason']}"


def notify_new(cur, ids):
    for pid in ids:
        p = db.one(cur, "SELECT * FROM action_proposals WHERE proposal_id = %s", (pid,))
        notify.send(f"🟠 AI 建议操作（INC-{p['incident_id']}）\n{describe(p)}\n\n"
                    f"{config.PROPOSAL_TTL_MIN} 分钟内没人批准就作废。也可以用 ./tw approve {pid} / ./tw reject {pid}",
                    buttons=[[("✅ 批准", f"ap:{pid}:y"), ("❌ 拒绝", f"ap:{pid}:n")]])


def decide(cur, proposal_id, approve, by, note=None):
    """人做决定。返回给人看的一句话"""
    p = db.one(cur, "SELECT * FROM action_proposals WHERE proposal_id = %s", (proposal_id,))
    if not p:
        return f"没有建议 #{proposal_id}"
    if p["status"] != "pending":
        return f"建议 #{proposal_id} 已经是 {p['status']}，不能再改"
    status = "approved" if approve else "rejected"
    cur.execute("UPDATE action_proposals SET status = %s, decided_at = now(), decided_by = %s, note = %s "
                "WHERE proposal_id = %s AND status = 'pending'", (status, by, note, proposal_id))
    log(SERVICE, "INFO", "action_decided", proposal_id=proposal_id, status=status, by=by)
    return f"建议 #{proposal_id} 已{'批准，马上执行' if approve else '拒绝'}"


def process(cur, ro_cur, bus):
    """作废过期的建议；执行已批准的建议"""
    cur.execute("UPDATE action_proposals SET status = 'expired' WHERE status = 'pending' "
                "AND created_at < now() - make_interval(mins => %s)", (config.PROPOSAL_TTL_MIN,))
    for p in db.rows(cur, "SELECT * FROM action_proposals WHERE status = 'approved' ORDER BY proposal_id"):
        ok, why = tools.validate_action(ro_cur, p["action"], p["target"])
        if not ok:
            cur.execute("UPDATE action_proposals SET status = 'failed', note = %s WHERE proposal_id = %s",
                        (f"执行前检查没通过：{why}", p["proposal_id"]))
            notify.send(f"⚠️ 建议 #{p['proposal_id']} 没有执行：{why}")
            continue
        by = f"copilot (approved by {p['decided_by']})"
        reason = f"AI 建议 #{p['proposal_id']}（INC-{p['incident_id']}）：{p['reason']}"[:300]
        if p["action"] == "pause_pm":
            cmd = {"cmd": "pause", "pm_id": p["target"], "by": by, "reason": reason, "ts": now()}
        else:
            cmd = {"cmd": "cancel", "parent_id": p["target"].upper(), "by": by, "reason": reason, "ts": now()}
        bus.send("ops.commands", p["target"], cmd)
        bus.flush()
        cur.execute("UPDATE action_proposals SET status = 'executed', executed_at = %s WHERE proposal_id = %s",
                    (datetime.now(timezone.utc).isoformat(), p["proposal_id"]))
        log(SERVICE, "WARN", "action_executed", proposal_id=p["proposal_id"], action=p["action"], target=p["target"], by=by)
        notify.send(f"✅ 已执行建议 #{p['proposal_id']}：{p['action']} {p['target']}（{p['decided_by']} 批准）。"
                    f"用 ./tw status 确认生效")
