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
    elif cmd == "lag":
        table(cur, """SELECT group_id AS "服务(consumer group)", topic AS "topic", lag AS "积压",
                      to_char(ts, 'HH24:MI:SS') AS "时间" FROM kafka_lag
                      WHERE ts = (SELECT MAX(ts) FROM kafka_lag) ORDER BY lag DESC, group_id""", title="Kafka 积压")
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
