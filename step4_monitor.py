"""
第 4 步：监控和告警
------------------------------------------------
目标：理解四件事
  1. 监控就是"每隔几秒自动跑一遍 SQL 检查"
  2. 每条规则 = 一条 SQL + 一个阈值 + 一个严重级别（P1 / P2 / P3）
  3. 同一个问题只告警一次（去重），不然值班的人会被刷屏
  4. P1 级别自动采取行动：kill switch 暂停出问题的策略

先运行 step3 生成数据库，再每 2 秒检查一次，共检查 3 轮。
运行：python step4_monitor.py
"""

import contextlib
import io
import runpy
import sqlite3
import time

DB_FILE = "tradewatch.db"
STUCK_SECONDS = 3        # 订单超过 3 秒没有新回报，算卡住（真实系统里一般更长）
REJECT_PCT = 40          # 拒单率超过 40% 告警
MIN_ORDERS = 5           # 至少 5 笔单才算拒单率，避免样本太少误报


# ---------- 告警规则：每条都是一条 SQL ----------
# 每条 SQL 返回的每一行 = 一个问题；key 用来去重，detail 是告警内容
RULES = [
    {
        "name": "stuck_order",
        "severity": "P2",
        "sql": f"""
            SELECT order_id AS key, strategy,
                   order_id || ' 停在 ' || status || ' 已 '
                     || CAST((julianday('now','localtime') - julianday(updated_at)) * 86400 AS INT)
                     || ' 秒没有新回报' AS detail
            FROM orders
            WHERE status IN ('APPROVED', 'ACKED', 'PARTIALLY_FILLED')
              AND (julianday('now','localtime') - julianday(updated_at)) * 86400 > {STUCK_SECONDS}
        """,
        "next": "查 trace，看卡在哪个服务；确认券商连接是否正常",
    },
    {
        "name": "high_reject_rate",
        "severity": "P2",
        "sql": f"""
            SELECT strategy AS key, strategy,
                   strategy || ' 拒单率 ' || ROUND(100.0 * rejected / total, 1)
                     || '%（' || rejected || '/' || total || '）' AS detail
            FROM (
                SELECT strategy, COUNT(*) AS total,
                       SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected
                FROM orders GROUP BY strategy
            )
            WHERE total >= {MIN_ORDERS} AND 100.0 * rejected / total > {REJECT_PCT}
        """,
        "next": "按拒单原因分组，看是风控拒还是券商拒",
    },
    {
        "name": "fat_finger",
        "severity": "P1",
        "sql": """
            SELECT o.strategy AS key, o.strategy,
                   o.strategy || ' 下了 ' || COUNT(*) || ' 笔超大单，最大 ' || MAX(o.qty) || ' 股（已被风控拦下）'
                     AS detail
            FROM order_events e JOIN orders o ON o.order_id = e.order_id
            WHERE e.event_type = 'RISK_REJECT' AND e.reason LIKE 'qty%'
            GROUP BY o.strategy
        """,
        "next": "策略可能有 bug：查最近改过的参数，确认后再恢复",
    },
]


# ---------- 告警记录表 + kill switch 表 ----------
def setup_alert_tables(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            rule      TEXT NOT NULL,
            key       TEXT NOT NULL,
            severity  TEXT NOT NULL,
            detail    TEXT NOT NULL,
            fired_at  TEXT NOT NULL,
            UNIQUE (rule, key)              -- 同一规则、同一对象只记一次 = 去重
        );
        CREATE TABLE IF NOT EXISTS control (
            strategy  TEXT PRIMARY KEY,
            paused    INTEGER NOT NULL,     -- 1 = 已暂停
            reason    TEXT,
            at        TEXT
        );
    """)


def kill_switch(db, strategy, reason):
    """暂停策略。真实系统里风控会读这张表，拦下这个策略之后的所有订单。"""
    db.execute(
        "INSERT OR REPLACE INTO control VALUES (?, 1, ?, datetime('now','localtime'))",
        (strategy, reason),
    )


# ---------- 跑一轮检查 ----------
def check_once(db):
    new_alerts = 0
    for rule in RULES:
        for key, strategy, detail in db.execute(rule["sql"]).fetchall():
            cur = db.execute(
                "INSERT OR IGNORE INTO alerts (rule, key, severity, detail, fired_at) "
                "VALUES (?, ?, ?, ?, datetime('now','localtime'))",
                (rule["name"], key, rule["severity"], detail),
            )
            if cur.rowcount == 0:
                continue                    # 已经告警过，不重复发
            new_alerts += 1
            action = "无"
            if rule["severity"] == "P1":
                kill_switch(db, strategy, rule["name"])
                action = f"已暂停策略 {strategy}（kill switch）"
            # 以后这里换成发 Telegram
            print(f"  🚨 [{rule['severity']}] {rule['name']}  strategy={strategy}\n"
                  f"     {detail}\n"
                  f"     自动动作：{action}\n"
                  f"     下一步：{rule['next']}")
    db.commit()
    if new_alerts == 0:
        print("  ✅ 没有新告警")


# ---------- 运行 ----------
if __name__ == "__main__":
    print("先运行 step3，生成一份新的交易数据……")
    with contextlib.redirect_stdout(io.StringIO()):      # 隐藏 step3 的输出
        runpy.run_path("step3_database.py", run_name="__main__")

    db = sqlite3.connect(DB_FILE)
    setup_alert_tables(db)

    for round_no in range(1, 4):
        print(f"\n===== 第 {round_no} 轮检查  {time.strftime('%H:%M:%S')} =====")
        check_once(db)
        if round_no < 3:
            time.sleep(2)

    print("\n===== 被 kill switch 暂停的策略 =====")
    for row in db.execute("SELECT strategy, reason, at FROM control"):
        print("  ", row)
