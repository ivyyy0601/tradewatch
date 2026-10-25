"""
TradeWatch 监控（tw-monitor）：一直运行，和交易系统分开
------------------------------------------------
每 10 秒（真实时间）跑一遍规则。"现在"用的是行情时钟：market_data 里最新的时间，
因为交易系统是在回放历史行情，规则里的"3 分钟"指的是行情时间的 3 分钟。

发现新问题 → 记进 alerts → 发 Telegram（没配置就只打日志）。
fat finger 是 P1，会自动 kill switch；恢复必须由人用 tw.py 手动操作。
"""

import os
import time
import urllib.parse
import urllib.request

from common import connect, log

INTERVAL = int(os.getenv("MONITOR_INTERVAL", "10"))

# 每条规则：name, severity, 是否自动 kill switch, 是否合并成一条消息, SQL, 下一步建议
# SQL 统一返回三列：key（去重用）、strategy、detail；%(now)s = 行情时钟
RULES = [
    # 每笔卡住的单各记一条告警（不会漏），但同一轮发现的合并成一条消息发出去（不会刷屏）
    ("stuck_orders", "P2", False, True, """
        SELECT 'stuck-' || order_id,
               strategy,
               order_id || ' ' || strategy || ' ' || symbol || ' 停在 ' || status || '（'
                 || to_char(updated_at AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') || ' 起）'
        FROM orders
        WHERE status IN ('APPROVED', 'ACKED', 'PARTIALLY_FILLED')
          AND updated_at <= %(now)s - interval '3 minutes'
          AND updated_at >  %(now)s - interval '2 hours'
        ORDER BY updated_at
     """, "这些单是不是集中在同一时间段、同一个券商？先确认券商连接，再用 tw.py trace 看单笔经历"),

    ("high_reject_rate", "P2", False, False, """
        SELECT strategy || '-' || to_char(%(now)s AT TIME ZONE 'America/New_York', 'YYYY-MM-DD'),
               strategy,
               strategy || ' 最近 30 分钟拒单率 ' || round(100.0 * rej / n) || '%%（' || rej || '/' || n || '）'
        FROM (SELECT strategy, count(*) AS n, count(*) FILTER (WHERE status = 'REJECTED') AS rej
              FROM orders WHERE created_at > %(now)s - interval '30 minutes' GROUP BY strategy) t
        WHERE n >= 5 AND rej::numeric / n > 0.3
     """, "按拒单原因分组：是风控拒的还是券商拒的？是不是另一个告警的连锁反应？"),

    ("fat_finger", "P1", True, False, """
        SELECT o.strategy || '-' || to_char(%(now)s AT TIME ZONE 'America/New_York', 'YYYY-MM-DD'),
               o.strategy,
               o.strategy || ' 最近 10 分钟有 ' || count(*) || ' 笔超大单被风控拦下，最大 ' || max(o.qty) || ' 股'
        FROM order_events e JOIN orders o USING (order_id)
        WHERE e.event_type = 'RISK_REJECT' AND e.reason LIKE 'qty%%'
          AND e.ts > %(now)s - interval '10 minutes' AND e.ts <= %(now)s
        GROUP BY o.strategy
     """, "策略生成的数量不对：查这个策略最近有没有上线新版本或改参数，修好前不要恢复"),

    ("position_break", "P1", False, False, """
        SELECT 'pos-' || coalesce(p.strategy, b.strategy) || '-' || coalesce(p.symbol, b.symbol)
                 || '-' || (coalesce(b.qty, 0) - coalesce(p.qty, 0)),
               coalesce(p.strategy, b.strategy),
               coalesce(p.strategy, b.strategy) || ' ' || coalesce(p.symbol, b.symbol)
                 || ' 持仓对不上：我们记 ' || coalesce(p.qty, 0) || ' 股，券商记 ' || coalesce(b.qty, 0) || ' 股'
        FROM positions p FULL OUTER JOIN broker_positions b USING (strategy, symbol)
        WHERE coalesce(p.qty, 0) <> coalesce(b.qty, 0)
     """, "逐笔对比我们的成交记录和券商的成交记录，找出漏掉的那几笔"),
]


def telegram(text):
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=10)
    except Exception as e:                       # 告警发不出去不能让监控挂掉
        log("monitor", "ERROR", "telegram_failed", error=str(e))


def notify(name, severity, detail, action, next_step, now):
    log("monitor", "ERROR" if severity == "P1" else "WARN", "alert", rule=name, severity=severity,
        detail=detail, action=action or None, market_ts=now)
    icon = "🔴" if severity == "P1" else "🟠"
    telegram(f"{icon} [{severity}] {name}\n{detail}\n"
             + (f"自动动作：{action}\n" if action else "")
             + f"下一步：{next_step}\n行情时间：{now:%m-%d %H:%M}")


def check(cur, now):
    for name, severity, auto_kill, grouped, sql, next_step in RULES:
        cur.execute(sql, {"now": now})
        new_details = []
        for key, strategy, detail in cur.fetchall():
            cur.execute(
                "INSERT INTO alerts (rule, key, severity, strategy, detail, fired_at) "
                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (rule, key) DO NOTHING",
                (name, key, severity, strategy, detail, now),
            )
            if cur.rowcount == 0:
                continue                         # 报过了
            if grouped:
                new_details.append(detail)       # 先攒着，这一轮结束后合并发送
                continue
            action = ""
            if auto_kill and strategy:
                cur.execute(
                    "INSERT INTO control VALUES (%s, true, %s, 'monitor', %s) ON CONFLICT (strategy) "
                    "DO UPDATE SET paused = true, reason = EXCLUDED.reason, changed_by = 'monitor', "
                    "changed_at = EXCLUDED.changed_at",
                    (strategy, name, now),
                )
                action = f"已自动暂停 {strategy}（kill switch），恢复请用：python tw.py resume {strategy}"
            notify(name, severity, detail, action, next_step, now)
        if new_details:
            summary = f"新增 {len(new_details)} 笔订单超过 3 分钟没有回报：\n" + "\n".join(new_details[:8])
            if len(new_details) > 8:
                summary += f"\n……还有 {len(new_details) - 8} 笔，用 tw.py alerts 查看"
            notify(name, severity, summary, "", next_step, now)


def main():
    cur = connect("monitor").cursor()
    log("monitor", "INFO", "start", interval=INTERVAL, rules=[r[0] for r in RULES])
    telegram("✅ TradeWatch 监控已启动")
    rounds = 0
    while True:
        cur.execute("SELECT max(ts) FROM market_data")
        now = cur.fetchone()[0]
        if now is not None:
            check(cur, now)
        rounds += 1
        if rounds % 30 == 0:                     # 每 5 分钟打一条心跳，证明监控自己还活着
            log("monitor", "INFO", "heartbeat", market_ts=now)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
