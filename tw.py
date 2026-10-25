"""
tw：值班用的命令行工具
------------------------------------------------
在服务器上运行（进到 monitor 容器里执行）：
    docker compose exec monitor python tw.py status            # 现在各策略怎么样
    docker compose exec monitor python tw.py alerts            # 最近的告警
    docker compose exec monitor python tw.py trace <order_id>  # 追踪一笔单
    docker compose exec monitor python tw.py pause  <strategy> --by Ivy --reason "..."
    docker compose exec monitor python tw.py resume <strategy> --by Ivy --reason "..."
"""

import argparse
import os
import sys

from common import connect

os.environ.setdefault("TW_QUIET", "1")


def table(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = [["" if v is None else str(v) for v in r] for r in cur.fetchall()]
    widths = [max(len(x) for x in [c] + [r[i] for r in rows]) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(x.ljust(w) for x, w in zip(r, widths)))
    if not rows:
        print("（没有记录）")


def status(cur, _):
    cur.execute("SELECT max(ts) AT TIME ZONE 'America/New_York' FROM market_data")
    now = cur.fetchone()[0]
    print(f"行情时间：{now:%Y-%m-%d %H:%M}（美东）\n" if now else "还没有行情数据\n")
    print("== 今天各策略 ==")
    table(cur, """
        SELECT o.strategy,
               CASE WHEN c.paused THEN 'PAUSED' ELSE 'ACTIVE' END AS state,
               count(*) AS orders,
               count(*) FILTER (WHERE o.status = 'FILLED') AS filled,
               count(*) FILTER (WHERE o.status = 'REJECTED') AS rejected,
               count(*) FILTER (WHERE o.status IN ('APPROVED', 'ACKED', 'PARTIALLY_FILLED')) AS open
        FROM orders o LEFT JOIN control c USING (strategy)
        WHERE o.created_at::date = (SELECT max(ts)::date FROM market_data)
        GROUP BY o.strategy, c.paused ORDER BY o.strategy""")
    print("\n== kill switch ==")
    table(cur, "SELECT strategy, paused, reason, changed_by, "
               "to_char(changed_at AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') AS changed_at FROM control")


def alerts(cur, args):
    table(cur, """
        SELECT alert_id AS id, severity AS sev, rule,
               to_char(fired_at AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') AS fired, detail
        FROM alerts ORDER BY alert_id DESC LIMIT %s""", (args.n,))


def trace(cur, args):
    cur.execute("SELECT strategy, symbol, side, qty, ref_price, status, filled_qty, avg_price, trace_id "
                "FROM orders WHERE order_id = %s", (args.order_id,))
    o = cur.fetchone()
    if not o:
        sys.exit(f"找不到订单 {args.order_id}")
    print(f"{args.order_id}  {o[0]} {o[2]} {o[3]} {o[1]} @ {o[4]}   最终状态 {o[5]}，"
          f"成交 {o[6]} 股，均价 {o[7]}   trace_id={o[8]}\n")
    table(cur, """
        SELECT to_char(ts AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') AS time, service, event_type,
               qty, price, reason
        FROM order_events WHERE order_id = %s ORDER BY event_id""", (args.order_id,))


def set_paused(paused):
    def run(cur, args):
        cur.execute(
            "INSERT INTO control VALUES (%s, %s, %s, %s, now()) ON CONFLICT (strategy) DO UPDATE SET "
            "paused = EXCLUDED.paused, reason = EXCLUDED.reason, changed_by = EXCLUDED.changed_by, "
            "changed_at = EXCLUDED.changed_at",
            (args.strategy, paused, args.reason, args.by),
        )
        print(f"{args.strategy} 已{'暂停' if paused else '恢复'}（操作人 {args.by}，原因：{args.reason}）")
    return run


def main():
    p = argparse.ArgumentParser(prog="tw", description="TradeWatch 值班工具")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="各策略现在的情况").set_defaults(fn=status)
    a = sub.add_parser("alerts", help="最近的告警")
    a.add_argument("-n", type=int, default=20)
    a.set_defaults(fn=alerts)
    t = sub.add_parser("trace", help="追踪一笔单的完整经历")
    t.add_argument("order_id")
    t.set_defaults(fn=trace)
    for name, paused in (("pause", True), ("resume", False)):
        s = sub.add_parser(name, help=f"{'暂停' if paused else '恢复'}一个策略")
        s.add_argument("strategy")
        s.add_argument("--by", required=True, help="谁操作的")
        s.add_argument("--reason", required=True, help="为什么")
        s.set_defaults(fn=set_paused(paused))
    args = p.parse_args()
    cur = connect("tw").cursor()
    args.fn(cur, args)


if __name__ == "__main__":
    main()
