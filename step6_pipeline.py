"""
第 6 步：把所有东西串起来，用真实行情驱动
------------------------------------------------
真实行情回放 → 策略看价格自动下单 → 风控 → 券商 → 数据库 → 监控一直在查

和前面的区别：
  - 价格是 data/us_1m.csv 里这周的真实美股分钟价格
  - 策略根据价格变化自己决定买卖，不再是随机下单
  - 券商用"下一分钟"的真实价格成交，所以会有真实的滑点
  - 监控每 5 分钟（回放时间）检查一次，P1 会真的让风控拦下这个策略

回放不等待，几秒钟跑完一整周。结果存在 tradewatch_live.db。
运行：python step6_pipeline.py
"""

import os
import random
import sqlite3
from collections import defaultdict, deque

import pandas as pd

CSV_FILE = "data/us_1m.csv"
DB_FILE = "tradewatch_live.db"
random.seed(3)

# ============================================================
# 数据库
# ============================================================
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
db = sqlite3.connect(DB_FILE)
db.executescript("""
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY, strategy TEXT, symbol TEXT, side TEXT, qty INT,
    ref_price REAL,              -- 下单时看到的价格（用来算滑点）
    status TEXT, filled_qty INT DEFAULT 0,
    fill_price REAL,             -- 实际成交价
    created_at TEXT, updated_at TEXT
);
CREATE TABLE order_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, service TEXT,
    event_type TEXT, price REAL, reason TEXT, ts TEXT
);
CREATE TABLE alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT, rule TEXT, key TEXT, severity TEXT,
    detail TEXT, fired_at TEXT, UNIQUE (rule, key)
);
""")


def log_event(oid, service, event, ts, price=None, reason=None, status=None):
    """记一条事件；如果给了 status，同时更新订单状态。"""
    db.execute("INSERT INTO order_events (order_id, service, event_type, price, reason, ts) "
               "VALUES (?, ?, ?, ?, ?, ?)", (oid, service, event, price, reason, ts))
    if status:
        db.execute("UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?", (status, ts, oid))


# ============================================================
# 策略：看价格自动下单
# ============================================================
history = defaultdict(lambda: deque(maxlen=20))   # 每只股票最近 20 分钟的价格
last_trade_min = {}                               # 冷却：同一策略同一股票 15 分钟内不重复下单
order_no = 0


def momo(symbol, prices):
    """动量：5 分钟涨超 0.3% 就买，跌超 0.3% 就卖。"""
    if len(prices) < 6:
        return None
    change = prices[-1] / prices[-6] - 1
    if change > 0.003:
        return "BUY"
    if change < -0.003:
        return "SELL"


def meanrev(symbol, prices):
    """均值回归：价格比 20 分钟均价高 0.5% 就卖，低 0.5% 就买（赌它会回来）。"""
    if len(prices) < 20:
        return None
    dev = prices[-1] / (sum(prices) / len(prices)) - 1
    if dev > 0.005:
        return "SELL"
    if dev < -0.005:
        return "BUY"


STRATEGIES = {"momo": momo, "meanrev": meanrev}


def run_strategies(ts, minute_no, ticks, new_orders):
    global order_no
    for symbol, price in ticks.items():
        history[symbol].append(price)
        for name, fn in STRATEGIES.items():
            side = fn(symbol, list(history[symbol]))
            if not side or minute_no - last_trade_min.get((name, symbol), -99) < 15:
                continue
            last_trade_min[(name, symbol)] = minute_no
            qty = 10
            if random.random() < 0.01:          # 1% 概率"手滑"：数量多打两个 0
                qty *= 100
            order_no += 1
            oid = f"ord_{order_no:04d}"
            db.execute("INSERT INTO orders (order_id, strategy, symbol, side, qty, ref_price, status, "
                       "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'NEW', ?, ?)",
                       (oid, name, symbol, side, qty, price, ts, ts))
            log_event(oid, f"strategy-{name}", "NEW", ts, price=price)
            new_orders.append(oid)


# ============================================================
# 风控
# ============================================================
MAX_QTY = 100
paused = set()                                   # 被 kill switch 暂停的策略


def risk_gateway(ts, new_orders, to_broker):
    for oid in new_orders:
        strategy, qty = db.execute("SELECT strategy, qty FROM orders WHERE order_id = ?", (oid,)).fetchone()
        if strategy in paused:
            log_event(oid, "risk-gateway", "RISK_REJECT", ts, reason="strategy paused", status="REJECTED")
        elif qty > MAX_QTY:
            log_event(oid, "risk-gateway", "RISK_REJECT", ts, reason=f"qty {qty} > {MAX_QTY}", status="REJECTED")
        else:
            log_event(oid, "risk-gateway", "RISK_PASS", ts, status="APPROVED")
            to_broker.append(oid)


# ============================================================
# 券商：下一分钟用真实价格成交
# ============================================================
def mock_broker(ts, ticks, pending):
    """pending 里是上一分钟发来的单，这一分钟处理。"""
    for oid in pending:
        symbol = db.execute("SELECT symbol FROM orders WHERE order_id = ?", (oid,)).fetchone()[0]
        r = random.random()
        if r < 0.03:
            continue                              # 3%：券商不回复（卡住）
        if r < 0.08:
            log_event(oid, "mock-broker", "BROKER_REJECT", ts, reason="INSUFFICIENT_BUYING_POWER",
                      status="REJECTED")
            continue
        price = ticks.get(symbol)
        if price is None:
            continue
        log_event(oid, "mock-broker", "ACK", ts, status="ACKED")
        log_event(oid, "mock-broker", "FILL", ts, price=price, status="FILLED")
        db.execute("UPDATE orders SET filled_qty = qty, fill_price = ? WHERE order_id = ?", (price, oid))


# ============================================================
# 监控
# ============================================================
RULES = [
    ("stuck_order", "P2", """
        SELECT order_id, strategy, order_id || ' ' || symbol || ' 停在 ' || status || ' 超过 3 分钟'
        FROM orders WHERE status IN ('APPROVED', 'ACKED')
          AND (julianday(:now) - julianday(updated_at)) * 1440 >= 3"""),
    ("high_reject_rate", "P2", """
        SELECT strategy, strategy, strategy || ' 最近 30 分钟拒单率 ' || ROUND(100.0 * rej / n) || '%（' || rej || '/' || n || '）'
        FROM (SELECT strategy, COUNT(*) n, SUM(status = 'REJECTED') rej FROM orders
              WHERE (julianday(:now) - julianday(created_at)) * 1440 <= 30 GROUP BY strategy)
        WHERE n >= 5 AND 1.0 * rej / n > 0.3"""),
    ("fat_finger", "P1", """
        SELECT o.order_id, o.strategy, o.strategy || ' 下了 ' || o.qty || ' 股 ' || o.symbol || '（正常 10 股）'
        FROM order_events e JOIN orders o USING (order_id)
        WHERE e.event_type = 'RISK_REJECT' AND e.reason LIKE 'qty%'"""),
]


def monitor(ts):
    for rule, severity, sql in RULES:
        for key, strategy, detail in db.execute(sql, {"now": ts}).fetchall():
            cur = db.execute("INSERT OR IGNORE INTO alerts (rule, key, severity, detail, fired_at) "
                             "VALUES (?, ?, ?, ?, ?)", (rule, key, severity, detail, ts))
            if cur.rowcount == 0:
                continue
            action = ""
            if severity == "P1" and strategy not in paused:
                paused.add(strategy)
                action = f" → kill switch：暂停 {strategy}"
            print(f"  {ts[5:16]}  🚨 [{severity}] {rule}: {detail}{action}")


# ============================================================
# 主循环：一分钟一分钟回放
# ============================================================
df = pd.read_csv(CSV_FILE)
df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
print(f"回放 {df['symbol'].nunique()} 只股票，{df['ts'].min():%m-%d} 到 {df['ts'].max():%m-%d}\n")

pending = []                                     # 等券商处理的单
current_day = None
for minute_no, (ts, group) in enumerate(df.groupby("ts")):
    if ts.date() != current_day:                 # 新的一天：解除暂停（相当于值班的人查完问题后恢复）
        current_day = ts.date()
        paused.clear()
        print(f"===== {current_day}  开盘 =====")
    t = ts.isoformat()
    ticks = dict(zip(group["symbol"], group["close"]))

    mock_broker(t, ticks, pending)               # ① 券商处理上一分钟的单
    new_orders, pending = [], []
    run_strategies(t, minute_no, ticks, new_orders)   # ② 策略看最新价格下单
    risk_gateway(t, new_orders, pending)         # ③ 风控检查，通过的下一分钟给券商
    if minute_no % 5 == 0:
        monitor(t)                               # ④ 每 5 分钟监控一次
    db.commit()

# ============================================================
# 收盘复盘
# ============================================================
print("\n===== 复盘：每个策略 =====")
q = """SELECT strategy, COUNT(*) AS orders, SUM(status = 'FILLED') AS filled,
              SUM(status = 'REJECTED') AS rejected,
              ROUND(AVG(CASE WHEN status = 'FILLED' THEN
                    (fill_price / ref_price - 1) * 10000 * (CASE side WHEN 'BUY' THEN 1 ELSE -1 END) END), 1)
                    AS avg_slippage_bps
       FROM orders GROUP BY strategy"""
print(pd.read_sql(q, db).to_string(index=False))

print("\n===== 复盘：告警汇总 =====")
print(pd.read_sql("SELECT severity, rule, COUNT(*) AS n FROM alerts GROUP BY severity, rule ORDER BY severity", db)
      .to_string(index=False))
print(f"\n数据存在 {DB_FILE}，可以用 sqlite3 自己查。")
