"""
第 1 步：一笔订单的生命周期
------------------------------------------------
目标：理解两件事
  1. 订单有"状态"（NEW、FILLED……），状态只能按规定的路线变化
  2. 每次变化都记一条"事件"，事件串起来就是这笔订单的完整经历（trace）

这一步没有 Kafka、没有数据库，只有 Python。
运行：python step1_order_lifecycle.py
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime


# ---------- 1. 允许的状态变化 ----------
# key = 当前状态，value = 下一步可以变成的状态
# 例如 NEW 之后只能被风控通过(APPROVED)或拒绝(REJECTED)，不可能直接 FILLED
ALLOWED = {
    "NEW":              {"APPROVED", "REJECTED"},
    "APPROVED":         {"ACKED", "REJECTED"},                 # 发给券商后，券商确认或拒单
    "ACKED":            {"PARTIALLY_FILLED", "FILLED", "CANCELLED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED"},
    "FILLED":           set(),                                 # 终态，不能再变
    "REJECTED":         set(),
    "CANCELLED":        set(),
}


# ---------- 2. 事件：发生过的一件事 ----------
@dataclass
class OrderEvent:
    order_id: str
    service: str          # 哪个服务产生的（排查时知道是谁说的）
    event_type: str       # 发生了什么
    detail: str = ""      # 具体原因，比如拒单原因
    ts: datetime = field(default_factory=datetime.now)


# ---------- 3. 订单 ----------
@dataclass
class Order:
    strategy_id: str
    symbol: str
    side: str             # BUY / SELL
    qty: int
    ref_price: float      # 下单时的行情价，以后用来算滑点
    order_id: str = field(default_factory=lambda: "ord_" + uuid.uuid4().hex[:6])
    status: str = "NEW"
    filled_qty: int = 0
    events: list = field(default_factory=list)

    def change(self, new_status, service, event_type, detail=""):
        """改变状态。不合法的变化直接报错 —— 这就是在系统里'抓 bug'。"""
        if new_status not in ALLOWED[self.status]:
            raise ValueError(
                f"{self.order_id}: 非法状态变化 {self.status} -> {new_status}"
            )
        self.status = new_status
        self.events.append(OrderEvent(self.order_id, service, event_type, detail))

    def fill(self, qty, service="mock-broker"):
        """成交一部分或全部。"""
        self.filled_qty += qty
        if self.filled_qty >= self.qty:
            self.change("FILLED", service, "FILL", f"{qty} 成交，累计 {self.filled_qty}/{self.qty}")
        else:
            self.change("PARTIALLY_FILLED", service, "PARTIAL_FILL",
                        f"{qty} 成交，累计 {self.filled_qty}/{self.qty}")

    def trace(self):
        """打印这笔订单的完整经历 —— 以后的 `tw trace` 就是这个。"""
        print(f"\n=== {self.order_id}  {self.strategy_id} {self.side} {self.qty} {self.symbol}"
              f"  最终状态: {self.status} ===")
        for e in self.events:
            print(f"  {e.ts:%H:%M:%S.%f}"[:-3] + f"  [{e.service:<13}] {e.event_type:<13} {e.detail}")


# ---------- 4. 演示 ----------
if __name__ == "__main__":
    # 情况 A：正常订单，分两次成交
    a = Order("momo", "AAA", "BUY", 100, ref_price=10.00)
    a.events.append(OrderEvent(a.order_id, "strategy-momo", "NEW", "信号：价格突破"))
    a.change("APPROVED", "risk-gateway", "RISK_PASS")
    a.change("ACKED", "mock-broker", "ACK")
    a.fill(40)
    a.fill(60)
    a.trace()

    # 情况 B：券商拒单
    b = Order("meanrev", "BBB", "SELL", 500, ref_price=20.00)
    b.events.append(OrderEvent(b.order_id, "strategy-meanrev", "NEW"))
    b.change("APPROVED", "risk-gateway", "RISK_PASS")
    b.change("REJECTED", "mock-broker", "BROKER_REJECT", "INSUFFICIENT_MARGIN")
    b.trace()

    # 情况 C：一个 bug —— 拒单之后又收到了成交回报
    print("\n=== 情况 C：拒单后又来了成交 ===")
    try:
        b.fill(100)
    except ValueError as err:
        print("  抓到异常:", err)
