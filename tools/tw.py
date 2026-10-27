"""
tw：值班用的命令行工具
================================================
在服务器的 /opt/tradewatch 里运行：
    ./tw status                         所有服务、FIX 连接、Kafka 积压、PM 状态、未解决的告警
    ./tw trace <母单号或子单号>          一张单从生成到成交的完整经历（事件 + FIX 消息）
    ./tw alerts [all]                   未解决的告警（加 all 看最近 50 条）
    ./tw resolve <告警编号> <处理说明>   关掉一条告警，写下你做了什么
    ./tw pause  <PM> <原因>             暂停一个 PM（kill switch）
    ./tw resume <PM> <原因>             恢复
    ./tw cancel <母单号> <原因>         撤掉一个母单和它的子单
    ./tw lag                            每个服务在 Kafka 上积压了多少条消息
    ./tw report [日期_时间]             马上生成复盘报告（不写日期就生成所有还没出的）

  AI 值班助手（copilot）：
    ./tw incidents [all]                最近的事故（一组相关告警）和 AI 的判断
    ./tw incident <事故号>               一个事故的详情：告警、AI 报告、调查过程、建议的操作
    ./tw investigate <事故号|a告警编号>  让 copilot 马上（重新）调查，例：./tw investigate 12、./tw investigate a55
    ./tw proposals                      AI 建议、等你批准的操作
    ./tw approve <建议号> [说明]         批准（copilot 会马上执行）
    ./tw reject  <建议号> [说明]         拒绝
    ./tw ai-eval                        对照真实答案给 AI 打分（会显示答案，练习时先自己查完再跑）
    ./tw ai-backfill [小时]             用过去的历史告警补建事故
    ./tw ai-replay [小时] [条数] [--force]  用当时的数据让 AI 重新调查过去的事故（会花钱）
（./tw 其实就是 docker compose run --rm tools python -m tools.tw ...）
"""

import os
import sys

os.environ.setdefault("TW_QUIET", "1")

from common import Bus, connect, now  # noqa: E402
from config import PMS  # noqa: E402

USER = os.getenv("TW_USER", "ivy")


def table(cur, sql, params=None, title=None):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = [["" if v is None else str(v) for v in r] for r in cur.fetchall()]
    if title:
        print(f"\n== {title} ==")
    if not rows:
        print("（无）")
        return rows
    width = lambda s: sum(2 if ord(ch) > 0x2E80 else 1 for ch in s)          # 中文占两格
    widths = [max(width(x) for x in [c] + [r[i] for r in rows]) for i, c in enumerate(cols)]
    pad = lambda s, w: s + " " * (w - width(s))
    print("  ".join(pad(c, w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(pad(x, w) for x, w in zip(r, widths)))
    return rows


def status(cur):
    table(cur, """SELECT service AS "服务", ROUND(EXTRACT(EPOCH FROM now() - last_seen)) AS "心跳(秒前)",
                  CASE WHEN last_seen > now() - interval '90 seconds' THEN 'OK' ELSE '!! STALE' END AS "状态"
                  FROM service_heartbeats ORDER BY service""", title="服务心跳")
    table(cur, """SELECT broker AS "券商", status AS "连接", ROUND(EXTRACT(EPOCH FROM now() - last_heartbeat)) AS "心跳(秒前)",
                  ROUND(EXTRACT(EPOCH FROM now() - last_msg_in)) AS "最后回报(秒前)" FROM fix_sessions ORDER BY broker""",
          title="FIX 连接")
    table(cur, """SELECT symbol AS "股票", price AS "价格", ROUND(EXTRACT(EPOCH FROM now() - ts)) AS "更新(秒前)"
                  FROM latest_prices ORDER BY symbol""", title="行情")
    table(cur, """SELECT group_id AS "服务(consumer group)", SUM(lag) AS "积压" FROM kafka_lag
                  WHERE ts = (SELECT MAX(ts) FROM kafka_lag) GROUP BY group_id ORDER BY 2 DESC""", title="Kafka 积压")
    table(cur, """SELECT p.pm_id AS "PM", CASE WHEN c.paused THEN 'PAUSED' ELSE 'ACTIVE' END AS "状态",
                  c.reason AS "原因", c.changed_by AS "操作人",
                  (SELECT COUNT(*) FROM parent_orders o WHERE o.pm_id = p.pm_id AND o.status = 'WORKING') AS "执行中母单",
                  ROUND(e.gross) AS "总敞口", ROUND(e.net) AS "净敞口", ROUND(e.pnl) AS "盈亏"
                  FROM pms p LEFT JOIN control c USING (pm_id) LEFT JOIN pm_exposure e USING (pm_id) ORDER BY 1""",
          title="PM")
    alerts(cur)


def alerts(cur, show_all=False):
    where = "" if show_all else "WHERE resolved_at IS NULL"
    table(cur, f"""SELECT alert_id AS "编号", severity AS "级别", rule AS "规则", to_char(fired_at, 'MM-DD HH24:MI') AS "时间",
                   detail AS "内容", COALESCE(to_char(resolved_at, 'HH24:MI'), '') AS "解决", COALESCE(note, '') AS "处理说明"
                   FROM alerts {where} ORDER BY fired_at DESC LIMIT 50""",
          title="告警" if show_all else "未解决的告警")


def trace(cur, oid):
    pid = oid
    if oid.startswith("C"):
        cur.execute("SELECT parent_id FROM child_orders WHERE child_id = %s", (oid,))
        r = cur.fetchone()
        if not r:
            print(f"找不到子单 {oid}")
            return
        pid = r[0]
    rows = table(cur, """SELECT parent_id AS "母单", pm_id AS "PM", symbol AS "股票", side AS "方向", qty AS "数量",
                         filled_qty AS "已成交", avg_price AS "均价", ref_price AS "下单价", status AS "状态",
                         reject_reason AS "原因", to_char(created_at, 'HH24:MI:SS') AS "创建"
                         FROM parent_orders WHERE parent_id = %s""", (pid,), title="母单")
    if not rows:
        print(f"找不到母单 {pid}（如果刚下的单，可能 order-store 还没写进数据库：看 ./tw lag）")
        return
    table(cur, """SELECT child_id AS "子单", broker AS "券商", qty AS "数量", limit_price AS "限价", filled_qty AS "已成交",
                  avg_price AS "均价", status AS "状态", reason AS "原因", to_char(created_at, 'HH24:MI:SS') AS "发出"
                  FROM child_orders WHERE parent_id = %s ORDER BY created_at""", (pid,), title="子单")
    table(cur, """SELECT to_char(ts, 'HH24:MI:SS.MS') AS "时间", service AS "服务", event_type AS "事件",
                  COALESCE(child_id, '') AS "子单", COALESCE(detail, '') AS "说明"
                  FROM order_events WHERE parent_id = %s ORDER BY ts, event_id""", (pid,), title="经历（order_events）")
    table(cur, """SELECT to_char(f.ts, 'HH24:MI:SS.MS') AS "时间", f.direction AS "方向", f.broker AS "券商",
                  f.msg_type AS "35", f.raw AS "FIX 原文"
                  FROM fix_messages f JOIN child_orders c ON c.child_id = f.cl_ord_id
                  WHERE c.parent_id = %s ORDER BY f.ts, f.id""", (pid,), title="FIX 消息")


def command(cmd, target, reason):
    bus = Bus("tw")
    if cmd in ("pause", "resume"):
        if target not in PMS:
            sys.exit(f"没有这个 PM：{target}（可选：{', '.join(PMS)}）")
        bus.send("ops.commands", target, {"cmd": cmd, "pm_id": target, "by": USER, "reason": reason, "ts": now()})
    else:
        bus.send("ops.commands", target, {"cmd": "cancel", "parent_id": target, "by": USER, "reason": reason,
                                          "ts": now()})
    bus.flush()
    print(f"已发送：{cmd} {target}（{reason}）。几秒后用 ./tw status 或 ./tw trace 确认生效。")


# ============================================================
# AI 值班助手（copilot）
# ============================================================
def incidents(cur, show_all=False):
    where = "" if show_all else "WHERE i.opened_at > now() - interval '24 hours'"
    table(cur, f"""SELECT i.incident_id AS "事故", i.severity AS "级别", i.status AS "状态", i.title AS "标题",
                   to_char(i.opened_at, 'MM-DD HH24:MI') AS "开始",
                   (SELECT COUNT(*) FROM incident_alerts ia WHERE ia.incident_id = i.incident_id) AS "告警数",
                   COALESCE(v.mode, '') AS "方式", COALESCE(v.category, '') AS "AI 判断",
                   COALESCE(ROUND(v.confidence, 2)::text, '') AS "可信度"
                   FROM incidents i LEFT JOIN LATERAL (SELECT * FROM investigations x WHERE x.incident_id = i.incident_id
                                                       ORDER BY x.investigation_id DESC LIMIT 1) v ON true
                   {where} ORDER BY i.opened_at DESC LIMIT 40""", title="事故")


def incident(cur, iid):
    import json
    from copilot import report
    if not table(cur, """SELECT incident_id AS "事故", severity AS "级别", status AS "状态", title AS "标题", source AS "来源",
                         opened_at AS "开始", resolved_at AS "结束", array_to_string(components, ', ') AS "涉及部件"
                         FROM incidents WHERE incident_id = %s""", (iid,), title="事故"):
        return
    table(cur, """SELECT alert_id AS "告警", to_char(fired_at, 'MM-DD HH24:MI:SS') AS "时间", severity AS "级别",
                  rule AS "规则", key AS "key", COALESCE(to_char(resolved_at, 'HH24:MI'), '') AS "解决", detail AS "内容"
                  FROM incident_alerts WHERE incident_id = %s ORDER BY fired_at""", (iid,), title="告警")
    table(cur, """SELECT investigation_id AS "调查", mode AS "方式", trigger AS "触发", to_char(started_at, 'MM-DD HH24:MI:SS') AS "时间",
                  category AS "类别", ROUND(confidence, 2) AS "可信度", ROUND(cost_usd, 3) AS "费用$",
                  (SELECT COUNT(*) FROM investigation_steps s WHERE s.investigation_id = v.investigation_id) AS "工具次数",
                  COALESCE(error, '') AS "错误"
                  FROM investigations v WHERE incident_id = %s ORDER BY investigation_id""", (iid,), title="调查记录")
    cur.execute("SELECT * FROM investigations WHERE incident_id = %s ORDER BY investigation_id DESC LIMIT 1", (iid,))
    row = cur.fetchone()
    if row:
        inv = dict(zip([c[0] for c in cur.description], row))
        if isinstance(inv.get("report"), str):
            inv["report"] = json.loads(inv["report"])
        cur.execute("SELECT * FROM incidents WHERE incident_id = %s", (iid,))
        inc = dict(zip([c[0] for c in cur.description], cur.fetchone()))
        cur.execute("SELECT * FROM incident_alerts WHERE incident_id = %s ORDER BY fired_at", (iid,))
        al = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
        inv["steps"] = len(table(cur, """SELECT step_no AS "步", tool AS "工具", args::text AS "参数", ok AS "成功",
                                         result_chars AS "结果字符", duration_ms AS "毫秒"
                                         FROM investigation_steps WHERE investigation_id = %s ORDER BY step_no""",
                                 (inv["investigation_id"],), title="最新一次调查的工具调用"))
        print("\n== 最新一次调查的报告 ==")
        print(report.telegram(inc, al, inv))
    table(cur, """SELECT proposal_id AS "建议", action AS "操作", target AS "目标", status AS "状态",
                  COALESCE(decided_by, '') AS "决定人", reason AS "理由"
                  FROM action_proposals WHERE incident_id = %s ORDER BY proposal_id""", (iid,), title="建议的操作")


def proposals(cur):
    table(cur, """SELECT proposal_id AS "建议", incident_id AS "事故", action AS "操作", target AS "目标",
                  to_char(created_at, 'MM-DD HH24:MI') AS "时间", status AS "状态", COALESCE(decided_by, '') AS "决定人",
                  reason AS "理由"
                  FROM action_proposals ORDER BY (status = 'pending') DESC, proposal_id DESC LIMIT 30""", title="AI 建议的操作")


def copilot_event(event):
    bus = Bus("tw")
    bus.send("ops.alerts", "tw", {**event, "by": USER, "ts": now()})
    bus.flush()


def main(argv):
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return
    cmd, args = argv[0], argv[1:]
    if cmd in ("pause", "resume", "cancel"):
        if len(args) < 2:
            sys.exit(f"用法：./tw {cmd} <{'PM' if cmd != 'cancel' else '母单号'}> <原因>")
        command(cmd, args[0], " ".join(args[1:]))
        return
    if cmd in ("ai-eval", "ai-backfill", "ai-replay"):
        from copilot import eval as ai_eval
        ai_eval.main({"ai-eval": ["score"], "ai-backfill": ["backfill"], "ai-replay": ["replay"]}[cmd] + args)
        return
    if cmd == "investigate" and args:
        target = args[0]
        if target.lower().startswith("a") and target[1:].isdigit():
            copilot_event({"event": "investigate", "alert_id": int(target[1:])})
        elif target.isdigit():
            copilot_event({"event": "investigate", "incident_id": int(target)})
        else:
            sys.exit("用法：./tw investigate <事故号>  或  ./tw investigate a<告警编号>")
        print(f"已请 copilot 调查 {target}。大约一分钟后看 Telegram，或者 ./tw incidents")
        return
    if cmd == "report":
        from tools import report
        suffixes = args or report.pending_sessions()
        if not suffixes:
            print("没有新的对账单需要出报告。已有的报告在 data/reports/")
        for s in suffixes:
            path, breaks = report.generate(s)
            print(f"{path}  对不上：{breaks}")
        return
    db = connect("tw")
    cur = db.cursor()
    if cmd == "status":
        status(cur)
    elif cmd == "trace" and args:
        trace(cur, args[0])
    elif cmd == "alerts":
        alerts(cur, show_all=bool(args and args[0] == "all"))
    elif cmd == "resolve" and len(args) >= 2:
        cur.execute("UPDATE alerts SET resolved_at = COALESCE(resolved_at, now()), note = %s WHERE alert_id = %s",
                    (f"{USER}: {' '.join(args[1:])}", int(args[0])))
        print("已更新" if cur.rowcount else f"没有编号 {args[0]} 的告警")
    elif cmd == "incidents":
        incidents(cur, show_all=bool(args and args[0] == "all"))
    elif cmd == "incident" and args and args[0].isdigit():
        incident(cur, int(args[0]))
    elif cmd == "proposals":
        proposals(cur)
    elif cmd in ("approve", "reject") and args and args[0].isdigit():
        from copilot import approvals
        msg = approvals.decide(cur, int(args[0]), cmd == "approve", f"tw:{USER}", " ".join(args[1:]) or None)
        if cmd == "approve" and "已批准" in msg:
            copilot_event({"event": "proposal_decided", "proposal_id": int(args[0])})
        print(msg)
    elif cmd == "lag":
        table(cur, """SELECT group_id AS "服务(consumer group)", topic AS "topic", lag AS "积压",
                      to_char(ts, 'HH24:MI:SS') AS "时间" FROM kafka_lag
                      WHERE ts = (SELECT MAX(ts) FROM kafka_lag) ORDER BY lag DESC, group_id""", title="Kafka 积压")
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
