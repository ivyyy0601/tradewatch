"""
TradeWatch 交易系统（tw-engine）：一直运行
------------------------------------------------
相当于公司里"开发团队写好的交易系统"。你值班时不改它，只看它留下的日志和数据。

每一分钟（回放时间）依次发生：
  ① 行情：读出这一分钟 4 只股票的真实价格，写进 market_data
  ② 券商：处理之前收到的单（确认、成交、拒单），更新券商自己的持仓
  ③ 策略：看最新价格决定要不要下单
  ④ 风控：检查新单，通过的交给券商

速度：SPEED=60 表示真实 1 秒 = 行情 1 分钟，一个交易日 6.5 分钟放完。
一周数据放完，时间往后挪 7 天，接着放下一轮，所以时间一直往前走。

现实世界会出的意外由 faults.py 随机制造（练习时不要看那个文件）。
"""

import os
import random
import time
import uuid
from collections import defaultdict, deque
from datetime import timedelta

import pandas as pd

from common import connect, log
from faults import Faults

CSV_FILE = os.getenv("CSV_FILE", "data/us_1m.csv")
SPEED = float(os.getenv("SPEED", "60"))
MAX_QTY = 100                     # 风控：单笔最多 100 股
BASE_QTY = 10                     # 策略正常每次下 10 股
COOLDOWN_MIN = 15                 # 同一策略同一股票 15 分钟内不重复下单
START_CASH = 30_000               # 券商给每个策略的资金（美元）

db = connect("engine")
db.autocommit = False             # 每一分钟的所有改动一起提交，不会被看到"做到一半"的状态
cur = db.cursor()
faults = Faults()


# ============================================================
# 记录：每件事写进 order_events，同时打一行日志
# ============================================================
def record(o, service, event_type, ts, qty=None, price=None, reason=None, status=None):
    cur.execute(
        "INSERT INTO order_events (order_id, trace_id, service, event_type, qty, price, reason, ts) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (o["id"], o["trace"], service, event_type, qty, price, reason, ts),
    )
    if status:
        o["status"] = status
        cur.execute("UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s", (status, ts, o["id"]))
    level = "WARN" if "REJECT" in event_type else "INFO"
    log(service, level, event_type, market_ts=ts, order_id=o["id"], trace_id=o["trace"],
        strategy=o["strategy"], symbol=o["symbol"], side=o["side"], qty=qty, price=price, reason=reason)


# ============================================================
# 策略
# ============================================================
history = defaultdict(lambda: deque(maxlen=20))     # 每只股票最近 20 分钟价格
last_order_at = {}                                  # (策略, 股票) → 上次下单的分钟序号


def momo(prices):
    """动量：5 分钟涨超 0.3% 买，跌超 0.3% 卖。"""
    if len(prices) < 6:
        return None
    change = prices[-1] / prices[-6] - 1
    return "BUY" if change > 0.003 else "SELL" if change < -0.003 else None


def meanrev(prices):
    """均值回归：比 20 分钟均价高 0.5% 卖，低 0.5% 买。"""
    if len(prices) < 20:
        return None
    dev = prices[-1] / (sum(prices) / len(prices)) - 1
    return "SELL" if dev > 0.005 else "BUY" if dev < -0.005 else None


STRATEGIES = {"momo": momo, "meanrev": meanrev}


def new_order(ts, name, symbol, side, qty, price):
    o = {"id": "ord_" + uuid.uuid4().hex[:10], "trace": uuid.uuid4().hex[:16],
         "strategy": name, "symbol": symbol, "side": side, "qty": qty,
         "ref_price": price, "filled": 0, "notional": 0.0, "status": "NEW"}
    cur.execute(
        "INSERT INTO orders (order_id, trace_id, strategy, symbol, side, qty, ref_price, status, "
        "created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, 'NEW', %s, %s)",
        (o["id"], o["trace"], name, symbol, side, qty, price, ts, ts),
    )
    record(o, f"strategy-{name}", "NEW", ts, qty=qty, price=price)
    return o


def run_strategies(ts, minute_no, prices):
    orders = []
    for symbol, price in prices.items():
        history[symbol].append(price)
        for name, fn in STRATEGIES.items():
            side = fn(list(history[symbol]))
            if not side or minute_no - last_order_at.get((name, symbol), -999) < COOLDOWN_MIN:
                continue
            last_order_at[(name, symbol)] = minute_no
            f = faults.active("bad_deploy", ts)
            qty = BASE_QTY * (100 if f and f["strategy"] == name else 1)
            o = new_order(ts, name, symbol, side, qty, price)
            orders.append(o)
            f = faults.active("duplicate_send", ts)
            if f and f["strategy"] == name:
                log(f"strategy-{name}", "WARN", "send_timeout_retry", market_ts=ts, order_id=o["id"],
                    detail="no response within 2s, resending")
                orders.append(new_order(ts, name, symbol, side, qty, price))
    return orders


# ============================================================
# 风控
# ============================================================
def risk_gateway(ts, new_orders, broker_inbox):
    for o in new_orders:
        cur.execute("SELECT paused FROM control WHERE strategy = %s", (o["strategy"],))
        row = cur.fetchone()
        if row and row[0]:
            record(o, "risk-gateway", "RISK_REJECT", ts, reason="strategy paused", status="REJECTED")
        elif o["qty"] > MAX_QTY:
            record(o, "risk-gateway", "RISK_REJECT", ts, reason=f"qty {o['qty']} > {MAX_QTY}", status="REJECTED")
        else:
            record(o, "risk-gateway", "RISK_PASS", ts, status="APPROVED")
            if not faults.active("broker_outage", ts):
                broker_inbox.append(o)


# ============================================================
# 模拟券商
# ============================================================
broker_cash = defaultdict(lambda: START_CASH)


def broker(ts, prices, working):
    """working：券商手上还没做完的单。返回这一分钟之后还没做完的单。"""
    still_working = []
    for o in working:
        price = prices.get(o["symbol"])
        if price is None or (o["status"] == "APPROVED" and faults.active("slow_broker", ts)):
            still_working.append(o)
            continue

        if o["status"] == "APPROVED":
            halt = faults.active("symbol_halt", ts)
            if halt and halt["symbol"] == o["symbol"]:
                record(o, "mock-broker", "BROKER_REJECT", ts, reason="SYMBOL_HALTED", status="REJECTED")
                continue
            notional = o["qty"] * price
            if o["side"] == "BUY" and broker_cash[o["strategy"]] < notional:
                record(o, "mock-broker", "BROKER_REJECT", ts, reason="INSUFFICIENT_BUYING_POWER", status="REJECTED")
                continue
            record(o, "mock-broker", "ACK", ts, status="ACKED")

        # 成交：80% 一次成交完；20% 先成交一半，剩下的下一分钟再成交
        remaining = o["qty"] - o["filled"]
        fill_qty = remaining if (o["filled"] > 0 or random.random() < 0.8) else remaining // 2
        o["filled"] += fill_qty
        o["notional"] += fill_qty * price
        sign = 1 if o["side"] == "BUY" else -1
        broker_cash[o["strategy"]] -= sign * fill_qty * price
        cur.execute(
            "INSERT INTO broker_positions VALUES (%s, %s, %s, %s) ON CONFLICT (strategy, symbol) "
            "DO UPDATE SET qty = broker_positions.qty + EXCLUDED.qty, updated_at = EXCLUDED.updated_at",
            (o["strategy"], o["symbol"], sign * fill_qty, ts),
        )
        done = o["filled"] >= o["qty"]
        if not faults.active("lost_fill_report", ts):
            record(o, "mock-broker", "FILL" if done else "PARTIAL_FILL", ts, qty=fill_qty, price=price,
                   status="FILLED" if done else "PARTIALLY_FILLED")
            cur.execute("UPDATE orders SET filled_qty = %s, avg_price = %s WHERE order_id = %s",
                        (o["filled"], round(o["notional"] / o["filled"], 4), o["id"]))
        if not done:
            still_working.append(o)
    return still_working


# ============================================================
# 主循环
# ============================================================
def main():
    df = pd.read_csv(CSV_FILE)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
    days = sorted(df["ts"].dt.date.unique())
    first_ts = df["ts"].min()

    # 如果是重启，接着上次的时间往后放，不重复
    cur.execute("SELECT max(ts) FROM market_data")
    last = cur.fetchone()[0]
    loop = 0 if last is None else int((pd.Timestamp(last) - first_ts) / timedelta(days=7)) + 1
    log("engine", "INFO", "start", days=len(days), symbols=df["symbol"].nunique(), speed=SPEED, loop=loop)

    minute_no, working, last_price = 0, [], {}
    while True:
        shift = timedelta(days=7 * loop)
        for day in days:
            faults.new_day(day + shift)
            log("engine", "INFO", "market_open", date=str(day + shift))
            for ts, group in df[df["ts"].dt.date == day].groupby("ts"):
                t = (ts + shift).to_pydatetime()
                for service, level, msg, fields in faults.system_logs(t):
                    log(service, level, msg, market_ts=t, **fields)

                stale = faults.active("stale_feed", t)
                for row in group.itertuples():
                    if stale and stale["symbol"] == row.symbol and row.symbol in last_price:
                        continue                                        # 这只股票的行情没有更新
                    last_price[row.symbol] = round(row.close, 4)
                    cur.execute("INSERT INTO market_data VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (t, row.symbol, last_price[row.symbol], int(row.volume)))
                prices = dict(last_price)

                working = broker(t, prices, working)                    # ② 券商
                new_orders = run_strategies(t, minute_no, prices)       # ③ 策略
                risk_gateway(t, new_orders, working)                    # ④ 风控 → 券商
                db.commit()                                             # 这一分钟的改动一起生效
                minute_no += 1
                time.sleep(60 / SPEED)
            log("engine", "INFO", "market_close", date=str(day + shift))
            history.clear()                                             # 新的一天重新开始算信号
        loop += 1


if __name__ == "__main__":
    main()
