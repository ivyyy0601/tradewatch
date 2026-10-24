"""
第 2 步：服务之间发消息
------------------------------------------------
目标：理解三件事
  1. 策略、风控、券商是三个独立的"服务"，每个只管自己那一段
  2. 它们之间不直接调用，只往"消息通道"（topic）里放消息、从里面取消息
  3. 消息就是一个字典（以后会变成 JSON），里面一定带 order_id

这一步用 Python 的 deque 模拟消息通道，第 5 步换成 Kafka，逻辑不变。
运行：python step2_message_passing.py
"""

import random
import uuid
from collections import deque

random.seed(7)  # 固定随机数，让每次运行结果一样，方便对照

# ---------- 消息通道（以后就是 Kafka 的 topic） ----------
TOPICS = {
    "orders.new":      deque(),   # 策略 → 风控
    "orders.approved": deque(),   # 风控 → 券商
    "orders.events":   deque(),   # 风控/券商 → 记录（所有回报）
}


def send(topic, msg):
    """往通道里放一条消息，并打印出来，让你看到消息的流动。"""
    TOPICS[topic].append(msg)
    print(f"  发送 → {topic:<16} {msg['order_id']}  {msg.get('event', '订单本身')}"
          f"  {msg.get('reason', '')}")


# ---------- 服务 1：策略 ----------
def strategy(name, symbol, side, qty, price):
    """策略只做一件事：决定下单，然后把订单发出去。它不知道后面谁来处理。"""
    order = {
        "order_id": "ord_" + uuid.uuid4().hex[:6],
        "strategy": name,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
    }
    print(f"\n[strategy-{name}] 决定 {side} {qty} {symbol} @ {price}")
    send("orders.new", order)


# ---------- 服务 2：风控 ----------
MAX_QTY = 1000          # 单笔最多 1000 股
MARKET_PRICE = {"AAA": 10.0, "BBB": 20.0}


def risk_gateway():
    """从 orders.new 取订单，检查后决定放行还是拦截。"""
    while TOPICS["orders.new"]:
        order = TOPICS["orders.new"].popleft()
        print(f"[risk-gateway] 收到 {order['order_id']}，开始检查")

        if order["qty"] > MAX_QTY:
            send("orders.events", {"order_id": order["order_id"], "service": "risk-gateway",
                                   "event": "RISK_REJECT", "reason": f"qty {order['qty']} > {MAX_QTY}"})
            continue

        deviation = abs(order["price"] / MARKET_PRICE[order["symbol"]] - 1)
        if deviation > 0.05:
            send("orders.events", {"order_id": order["order_id"], "service": "risk-gateway",
                                   "event": "RISK_REJECT", "reason": f"价格偏离 {deviation:.0%}"})
            continue

        send("orders.events", {"order_id": order["order_id"], "service": "risk-gateway",
                               "event": "RISK_PASS"})
        send("orders.approved", order)   # 通过的订单原样转给券商


# ---------- 服务 3：模拟券商 ----------
def mock_broker():
    """从 orders.approved 取订单，模拟券商的各种回应。"""
    while TOPICS["orders.approved"]:
        order = TOPICS["orders.approved"].popleft()
        oid = order["order_id"]
        print(f"[mock-broker] 收到 {oid}")

        if random.random() < 0.3:                       # 30% 概率拒单
            send("orders.events", {"order_id": oid, "service": "mock-broker",
                                   "event": "BROKER_REJECT", "reason": "INSUFFICIENT_MARGIN"})
            continue

        send("orders.events", {"order_id": oid, "service": "mock-broker", "event": "ACK"})
        first = order["qty"] // 2                       # 分两次成交
        send("orders.events", {"order_id": oid, "service": "mock-broker",
                               "event": "PARTIAL_FILL", "qty": first})
        send("orders.events", {"order_id": oid, "service": "mock-broker",
                               "event": "FILL", "qty": order["qty"] - first})


# ---------- 服务 4：记录（以后是 order-store，写进数据库） ----------
def order_store():
    """把所有回报按订单归类，就是第 1 步里的 trace。"""
    by_order = {}
    while TOPICS["orders.events"]:
        e = TOPICS["orders.events"].popleft()
        by_order.setdefault(e["order_id"], []).append(e)

    print("\n========== 每笔订单的 trace ==========")
    for oid, events in by_order.items():
        print(f"\n{oid}")
        for e in events:
            print(f"  [{e['service']:<12}] {e['event']:<13} {e.get('reason', '')}{e.get('qty', '')}")


# ---------- 运行 ----------
if __name__ == "__main__":
    print("===== 1. 策略下单 =====")
    strategy("momo", "AAA", "BUY", 100, 10.02)      # 正常
    strategy("momo", "AAA", "BUY", 5000, 10.01)     # 数量太大（fat finger）
    strategy("meanrev", "BBB", "SELL", 200, 23.00)  # 价格离谱
    strategy("meanrev", "BBB", "SELL", 300, 19.98)  # 正常
    strategy("momo", "AAA", "SELL", 100, 9.99)      # 正常

    print("\n===== 2. 风控处理 =====")
    risk_gateway()

    print("\n===== 3. 券商处理 =====")
    mock_broker()

    order_store()
