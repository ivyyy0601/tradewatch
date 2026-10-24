"""
step4 演示版：交易一直在动，监控一直在查
------------------------------------------------
模拟 15 秒（用假时钟，一瞬间跑完）：
  - 每 1 秒：交易系统下 1 笔新单，券商处理上一秒的单
  - 每 3 秒：监控跑一遍 SQL，看有没有问题
运行：python step4_live_demo.py
"""

import random
import sqlite3

random.seed(10)
db = sqlite3.connect(":memory:")          # 内存数据库，跑完就没了
db.execute("CREATE TABLE orders (order_id TEXT, strategy TEXT, qty INT, status TEXT, updated_at INT)")

already_alerted = set()                   # 告警记录本：报过的不再报
order_no = 0


# ---------- 交易系统：每秒执行一次 ----------
def trading(now):
    global order_no
    # ① 券商处理上一秒发来的单：大多成交，偶尔不回复（卡住）
    for (oid,) in db.execute("SELECT order_id FROM orders WHERE status='APPROVED' AND updated_at=?",
                             (now - 1,)).fetchall():
        if random.random() < 0.8:
            db.execute("UPDATE orders SET status='FILLED', updated_at=? WHERE order_id=?", (now, oid))
            print(f"  [交易] 券商：{oid} 成交")
        else:
            print(f"  [交易] 券商：{oid} ……没有回复")

    # ② 策略下一笔新单；偶尔手滑下超大单，被风控拒绝
    order_no += 1
    oid = f"ord_{order_no:02d}"
    strategy = random.choice(["momo", "rebal"])
    qty = 5000 if random.random() < 0.15 else 100
    status = "REJECTED" if qty > 1000 else "APPROVED"
    db.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", (oid, strategy, qty, status, now))
    print(f"  [交易] {strategy} 下单 {oid} {qty} 股 → 风控{'拒绝' if status == 'REJECTED' else '通过'}")


# ---------- 监控：每 3 秒执行一次 ----------
def monitor(now):
    print(f"  [监控] 开始检查……")
    found = False

    # 规则 1：通过风控超过 3 秒还没成交 = 卡住
    for (oid,) in db.execute("SELECT order_id FROM orders WHERE status='APPROVED' AND ? - updated_at >= 3",
                             (now,)).fetchall():
        if ("stuck", oid) not in already_alerted:
            already_alerted.add(("stuck", oid))
            print(f"  [监控] 🚨 P2 订单卡住：{oid} 已经 3 秒以上没有回复")
            found = True

    # 规则 2：有超大单被拒 = 策略可能有 bug
    for oid, strategy in db.execute("SELECT order_id, strategy FROM orders WHERE status='REJECTED' AND qty > 1000").fetchall():
        if ("fat", oid) not in already_alerted:
            already_alerted.add(("fat", oid))
            print(f"  [监控] 🚨 P1 超大单：{strategy} 下了 {oid}，暂停 {strategy}")
            found = True

    if not found:
        print(f"  [监控] ✅ 没有新问题")


# ---------- 主循环：时间一秒一秒往前走 ----------
for now in range(1, 16):
    print(f"\n第 {now} 秒")
    trading(now)              # 交易每秒都在动
    if now % 3 == 0:
        monitor(now)          # 监控每 3 秒查一次
