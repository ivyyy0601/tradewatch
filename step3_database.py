"""
第 3 步：把订单和事件存进数据库，用 SQL 查
------------------------------------------------
目标：理解三件事
  1. 两张表：orders 存"每笔订单现在的状态"，order_events 存"发生过的每件事"
  2. 记录员（order-store）同时读 orders.new 和 orders.events，trace 不再缺 NEW
  3. 值班时的问题，都可以写成一条 SQL 来回答

直接复用 step2 的策略、风控、券商。数据库用 Python 自带的 SQLite，
文件叫 tradewatch.db，每次运行重新建。以后换成 PostgreSQL，SQL 基本不变。
运行：python step3_database.py
"""

import os
import random
import sqlite3
from datetime import datetime

# 复用 step2 里写好的服务和消息通道
from step2_message_passing import TOPICS, strategy, risk_gateway, mock_broker

DB_FILE = "tradewatch.db"


# ---------- 1. 建表 ----------
SCHEMA = """
CREATE TABLE orders (
    order_id    TEXT PRIMARY KEY,
    strategy    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL,
    qty         INTEGER NOT NULL,
    price       REAL NOT NULL,
    status      TEXT NOT NULL,          -- 当前状态，随事件更新
    filled_qty  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE order_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,   -- 自动编号，也代表先后顺序
    order_id    TEXT NOT NULL REFERENCES orders(order_id),
    service     TEXT NOT NULL,          -- 哪个服务说的
    event_type  TEXT NOT NULL,          -- 发生了什么
    qty         INTEGER,                -- 成交数量（成交事件才有）
    reason      TEXT,                   -- 拒单原因（拒单事件才有）
    ts          TEXT NOT NULL
);
"""

# 事件 → 订单的新状态（和第 1 步的状态机对应）
NEW_STATUS = {
    "RISK_PASS":     "APPROVED",
    "RISK_REJECT":   "REJECTED",
    "BROKER_REJECT": "REJECTED",
    "ACK":           "ACKED",
    "PARTIAL_FILL":  "PARTIALLY_FILLED",
    "FILL":          "FILLED",
}


def now():
    return datetime.now().isoformat(timespec="milliseconds")


def setup_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)               # 每次从头开始，方便练习
    db = sqlite3.connect(DB_FILE)
    db.executescript(SCHEMA)
    return db


# ---------- 2. 记录员：把消息写进数据库 ----------
def record_new_orders(db):
    """读 orders.new，每笔订单插入一行，并记一条 NEW 事件。
    注意：这里只是"看"队列（for 循环），不 popleft，订单还留给风控去取。
    这就像 Kafka 允许两个服务读同一条消息。"""
    for o in TOPICS["orders.new"]:
        t = now()
        db.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, 'NEW', 0, ?, ?)",
            (o["order_id"], o["strategy"], o["symbol"], o["side"], o["qty"], o["price"], t, t),
        )
        db.execute(
            "INSERT INTO order_events (order_id, service, event_type, ts) VALUES (?, ?, 'NEW', ?)",
            (o["order_id"], "strategy-" + o["strategy"], t),
        )
    db.commit()


def record_events(db):
    """读 orders.events，每条回报：① 记进 order_events ② 更新 orders 的状态。"""
    while TOPICS["orders.events"]:
        e = TOPICS["orders.events"].popleft()
        t = now()
        db.execute(
            "INSERT INTO order_events (order_id, service, event_type, qty, reason, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (e["order_id"], e["service"], e["event"], e.get("qty"), e.get("reason"), t),
        )
        db.execute(
            "UPDATE orders SET status = ?, filled_qty = filled_qty + ?, updated_at = ? "
            "WHERE order_id = ?",
            (NEW_STATUS[e["event"]], e.get("qty") or 0, t, e["order_id"]),
        )
    db.commit()


# ---------- 3. 用 SQL 回答值班问题 ----------
def show(db, title, sql, params=()):
    """执行 SQL，用表格打印结果。"""
    cur = db.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    widths = [max(len(str(x)) for x in [c] + [r[i] for r in rows]) for i, c in enumerate(cols)]
    print(f"\n### {title}")
    print("  " + "  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        print("  " + "  ".join(str(x).ljust(w) for x, w in zip(r, widths)))


# ---------- 运行 ----------
if __name__ == "__main__":
    db = setup_db()
    random.seed(42)

    # 1) 三个策略随机下 30 笔单；偶尔出现"手滑"的数量或离谱的价格
    market = {"AAA": 10.0, "BBB": 20.0}
    for _ in range(30):
        name = random.choice(["momo", "meanrev", "rebal"])
        symbol = random.choice(["AAA", "BBB"])
        qty = random.choice([100, 200, 300])
        price = round(market[symbol] * random.uniform(0.99, 1.01), 2)
        r = random.random()
        if r < 0.08:
            qty *= 50                        # fat finger
        elif r < 0.15:
            price = round(price * 1.2, 2)    # 价格算错
        strategy(name, symbol, random.choice(["BUY", "SELL"]), qty, price)

    # 2) 交易链路：记录新单 → 风控 → 券商 → 记录回报
    record_new_orders(db)
    risk_gateway()
    mock_broker()

    # 模拟一个故障：最后一笔通过风控的单，券商一直没回报（卡住了）
    stuck = next(o for o in reversed(list(TOPICS["orders.events"])) if o["event"] == "RISK_PASS")
    TOPICS["orders.events"] = type(TOPICS["orders.events"])(
        e for e in TOPICS["orders.events"]
        if not (e["order_id"] == stuck["order_id"] and e["service"] == "mock-broker")
    )
    record_events(db)

    print("\n\n================ 以下是 SQL 查询 ================")

    show(db, "Q1. 现在每种状态各有几笔单？",
         """SELECT status, COUNT(*) AS n
            FROM orders
            GROUP BY status
            ORDER BY n DESC""")

    show(db, "Q2. 哪些单被拒了？被谁拒的？为什么？",
         """SELECT o.order_id, o.strategy, o.symbol, o.qty, o.price,
                   e.service AS rejected_by, e.reason
            FROM orders o
            JOIN order_events e ON e.order_id = o.order_id
            WHERE e.event_type IN ('RISK_REJECT', 'BROKER_REJECT')
            ORDER BY e.service, o.strategy""")

    show(db, "Q3. 每个策略的拒单率",
         """SELECT strategy,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected,
                   ROUND(100.0 * SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) / COUNT(*), 1)
                       AS reject_pct
            FROM orders
            GROUP BY strategy
            ORDER BY reject_pct DESC""")

    show(db, "Q4. 发出去了但还没有最终结果的单（可能卡住了）",
         """SELECT order_id, strategy, symbol, qty, status, filled_qty, updated_at
            FROM orders
            WHERE status NOT IN ('FILLED', 'REJECTED', 'CANCELLED')""")

    show(db, f"Q5. 追踪卡住的那笔单：{stuck['order_id']}",
         """SELECT event_id, service, event_type, qty, reason
            FROM order_events
            WHERE order_id = ?
            ORDER BY event_id""", (stuck["order_id"],))

    print(f"\n数据已存在 {DB_FILE}，可以用 `sqlite3 {DB_FILE}` 自己写 SQL 查。")
