"""
TradeWatch 交易系统（tw-engine）
================================================
相当于公司里开发团队写好的交易系统。值班时你不改它，只看它的屏幕、日志和数据库。

每根 K 线（默认真实 1 分钟 = 行情 1 分钟）依次发生：
  ① 行情：读出这一分钟 4 只股票的真实历史价格，打上"现在"的真实时间，写进 market_data
  ② 券商：处理收到的子单（确认、成交、撤单、拒单），通过 FIX 回报给我们
  ③ FIX 网关：收到回报 → 更新子单、记成交、更新母单和持仓
  ④ EMS 执行算法（TWAP）：给每个母单发下一个子单；超时的子单撤掉重下
  ⑤ 每 5 根 K 线：PM 策略给出目标仓位 → OMS 生成母单 → 交易前风控检查
每个"交易日"（历史数据的一天）结束时，券商发来当天的成交对账单文件。
历史数据放完就从头再放，一直运行。
"""

import csv
import math
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd

import fix
from common import connect, log
from config import (BROKERS, CHILD_LIMIT_BPS, CHILD_TIMEOUT_BARS, DAILY_LOCATES, LOT, MAX_ORDER_NOTIONAL,
                    MIN_ORDER_NOTIONAL, OUR_COMP_ID, PMS, REBALANCE_EVERY, ROUTING, TWAP_SLICES)
from faults import Faults
from strategies import STRATEGIES

CSV_FILE = os.getenv("CSV_FILE", "data/us_1m.csv")
STATEMENT_DIR = os.getenv("STATEMENT_DIR", "data/statements")
SPEED = float(os.getenv("SPEED", "1"))           # 1 = 真实速度；10 = 快 10 倍（练习用）

db = connect("engine")
db.autocommit = False                             # 每根 K 线的改动一起提交
cur = db.cursor()
faults = Faults()


def now():
    return datetime.now(timezone.utc)


def new_id(prefix, n=8):
    return prefix + uuid.uuid4().hex[:n].upper()


# ============================================================
# 状态（内存里）
# ============================================================
bar = 0
last_price = {}                                   # 最新行情
hist = defaultdict(list)                          # 每只股票的价格历史
positions = defaultdict(int)                      # (pm, symbol) → 我们记的持仓
parents = {}                                      # 还在执行的母单
children = {}                                     # 还没结束的子单
locates = dict(DAILY_LOCATES)
session_up = {b: True for b in BROKERS}
brokers = {b: {"inbox": [], "book": {}, "outbox": [], "trades": []} for b in BROKERS}


def event(parent_id, child_id, service, event_type, detail=None, level="INFO", **fields):
    cur.execute("INSERT INTO order_events (parent_id, child_id, service, event_type, detail, ts) "
                "VALUES (%s, %s, %s, %s, %s, %s)", (parent_id, child_id, service, event_type, detail, now()))
    log(service, level, event_type, parent_id=parent_id, child_id=child_id, detail=detail, **fields)


# ============================================================
# FIX 网关：我们这边负责收发 FIX 消息的程序
# ============================================================
def fix_send(broker, msg_type, cl_ord_id, fields):
    raw = fix.build(msg_type, OUR_COMP_ID, broker, fields)
    cur.execute("INSERT INTO fix_messages (ts, broker, direction, msg_type, cl_ord_id, raw) "
                "VALUES (%s, %s, 'OUT', %s, %s, %s)", (now(), broker, msg_type, cl_ord_id, raw))
    log("fix-gateway", "INFO", "fix_out", broker=broker, cl_ord_id=cl_ord_id, raw=raw)
    brokers[broker]["inbox"].append((msg_type, cl_ord_id, dict(fields)))


def fix_receive(broker, msg_type, cl_ord_id, fields):
    """收到券商发来的一条消息。"""
    raw = fix.build(msg_type, broker, OUR_COMP_ID, list(fields.items()))
    t = now()
    cur.execute("INSERT INTO fix_messages (ts, broker, direction, msg_type, cl_ord_id, raw) "
                "VALUES (%s, %s, 'IN', %s, %s, %s)", (t, broker, msg_type, cl_ord_id, raw))
    cur.execute("UPDATE fix_sessions SET last_msg_in = %s, updated_at = %s WHERE broker = %s", (t, t, broker))
    log("fix-gateway", "INFO", "fix_in", broker=broker, cl_ord_id=cl_ord_id, raw=raw)

    c = children.get(cl_ord_id)
    if c is None:
        log("fix-gateway", "WARN", "unknown_cl_ord_id", broker=broker, cl_ord_id=cl_ord_id)
        return
    p = parents.get(c["parent"])
    if msg_type == "9":                                          # 撤单被拒
        event(c["parent"], c["id"], "fix-gateway", "CANCEL_REJECTED", fields.get("58"), level="WARN")
        if fields.get("39") == "2":                              # 券商说这单早就全部成交了
            c["status"] = "FILLED"
            update_child(c, reason=f"cancel rejected {fields.get('58')}: broker reports order filled")
            finish_child(c)                                      # 母单按我们自己记到的成交量继续执行
        else:
            c["status"] = c.pop("status_before_cancel", c["status"])
            update_child(c, reason=f"cancel rejected: {fields.get('58')}")
        return

    exec_type = fields["150"]
    if exec_type == "0":                                         # 券商确认收到
        c["status"] = "NEW"
        update_child(c)
        event(c["parent"], c["id"], "fix-gateway", "CHILD_ACK", f"broker order {fields['37']}")
    elif exec_type == "8":                                       # 券商拒单
        c["status"] = "REJECTED"
        update_child(c, reason=fields.get("58"))
        event(c["parent"], c["id"], "fix-gateway", "CHILD_REJECTED", fields.get("58"), level="WARN")
        finish_child(c)
    elif exec_type == "4":                                       # 撤单成功
        c["status"] = "CANCELLED"
        update_child(c)
        event(c["parent"], c["id"], "fix-gateway", "CHILD_CANCELLED", f"leaves {fields['151']}")
        finish_child(c)
    elif exec_type == "F":                                       # 成交
        qty, px, cum = int(fields["32"]), float(fields["31"]), int(fields["14"])
        if cum != c["filled"] + qty:                             # 券商的累计数和我们对不上
            log("fix-gateway", "WARN", "cum_qty_mismatch", cl_ord_id=c["id"], broker=broker,
                ours=c["filled"] + qty, broker_cum=cum)
        c["filled"] += qty
        c["notional"] += qty * px
        c["status"] = "FILLED" if fields["39"] == "2" else "PARTIALLY_FILLED"
        update_child(c)
        cur.execute("INSERT INTO executions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (fields["17"], c["id"], c["parent"], c["pm"], broker, c["symbol"], c["side"], qty, px, now()))
        sign = 1 if c["side"] == "BUY" else -1
        positions[(c["pm"], c["symbol"])] += sign * qty
        event(c["parent"], c["id"], "fix-gateway", "FILL", f"{qty} @ {px}", qty=qty, price=px)
        if p:
            p["filled"] += qty
            p["notional"] += qty * px
            status = "DONE" if p["filled"] >= p["qty"] else "WORKING"
            cur.execute("UPDATE parent_orders SET filled_qty = %s, avg_price = %s, status = %s, updated_at = %s "
                        "WHERE parent_id = %s",
                        (p["filled"], round(p["notional"] / p["filled"], 4), status, now(), p["id"]))
            if status == "DONE":
                event(p["id"], None, "ems", "PARENT_DONE", f"{p['filled']} @ {p['notional'] / p['filled']:.4f}")
                parents.pop(p["id"], None)
        if c["status"] == "FILLED":
            finish_child(c)


def update_child(c, reason=None):
    cur.execute("UPDATE child_orders SET status = %s, filled_qty = %s, avg_price = %s, "
                "reason = COALESCE(%s, reason), updated_at = %s WHERE child_id = %s",
                (c["status"], c["filled"], round(c["notional"] / c["filled"], 4) if c["filled"] else None,
                 reason, now(), c["id"]))


def finish_child(c):
    children.pop(c["id"], None)
    p = parents.get(c["parent"])
    if p and p.get("open_child") == c["id"]:
        p["open_child"] = None


# ============================================================
# 模拟券商（券商内部，我们看不到它的日志）
# ============================================================
def broker_step(b):
    bk = brokers[b]
    silent = faults.active("broker_silent", broker=b)
    if not silent:
        for msg_type, cl_ord_id, f in bk["inbox"]:
            if msg_type == "D":                                  # 新单
                o = {"cl": cl_ord_id, "symbol": f["55"], "side": "BUY" if f["54"] == "1" else "SELL",
                     "qty": int(f["38"]), "limit": float(f["44"]), "cum": 0, "pm": f["1"],
                     "order_id": new_id(b[:2] + "-", 7), "open": True}
                halt = faults.active("symbol_halt", symbol=o["symbol"])
                if halt:
                    bk["outbox"].append(("8", cl_ord_id, exec_report(o, "8", "8", text="SYMBOL_HALTED")))
                    continue
                bk["book"][cl_ord_id] = o
                bk["outbox"].append(("8", cl_ord_id, exec_report(o, "0", "0")))
            elif msg_type == "F":                                # 撤单
                o = bk["book"].get(f["41"])
                if o and o["open"]:
                    o["open"] = False
                    bk["outbox"].append(("8", f["41"], exec_report(o, "4", "4")))
                else:
                    status = "2" if o else "8"
                    bk["outbox"].append(("9", f["41"], {"41": f["41"], "39": status, "434": "1",
                                                        "58": "TOO_LATE_TO_CANCEL" if o else "UNKNOWN_ORDER"}))
        bk["inbox"] = []
        for o in bk["book"].values():                            # 撮合：价格到了限价就成交
            px = last_true_price.get(o["symbol"])
            if not o["open"] or px is None or faults.active("symbol_halt", symbol=o["symbol"]):
                continue
            if (o["side"] == "BUY" and px <= o["limit"]) or (o["side"] == "SELL" and px >= o["limit"]):
                leaves = o["qty"] - o["cum"]
                qty = leaves if random.random() < 0.75 else max(LOT, leaves // 2 // LOT * LOT)
                o["cum"] += qty
                done = o["cum"] >= o["qty"]
                o["open"] = not done
                exec_id = new_id("EX", 10)
                bk["trades"].append({"exec_id": exec_id, "account": o["pm"], "symbol": o["symbol"], "side": o["side"],
                                     "qty": qty, "price": round(px, 4), "cl_ord_id": o["cl"],
                                     "exec_time": now().isoformat(timespec="seconds")})
                rpt = exec_report(o, "F", "2" if done else "1", last_qty=qty, last_px=round(px, 4), exec_id=exec_id)
                if faults.active("lost_exec_report", broker=b):
                    continue                                     # 券商成交了，但这条回报丢了
                bk["outbox"].append(("8", o["cl"], rpt))
    # 连接正常才能把消息送到我们这边；断线期间先存着，重连后补发
    if session_up[b]:
        for msg_type, cl_ord_id, f in bk["outbox"]:
            fix_receive(b, msg_type, cl_ord_id, f)
        bk["outbox"] = []


def exec_report(o, exec_type, ord_status, text=None, last_qty=None, last_px=None, exec_id=None):
    return {"37": o["order_id"], "11": o["cl"], "17": exec_id or new_id("EX", 10), "150": exec_type, "39": ord_status,
            "55": o["symbol"], "54": fix.side_code(o["side"]), "38": o["qty"], "32": last_qty, "31": last_px,
            "14": o["cum"], "151": 0 if exec_type in ("4", "8") else o["qty"] - o["cum"], "58": text}


def write_statements(trade_date):
    """一个交易日结束：每家券商发来当天的成交对账单（和真实的 prime broker 文件一样是 CSV）。"""
    os.makedirs(STATEMENT_DIR, exist_ok=True)
    for b, bk in brokers.items():
        path = os.path.join(STATEMENT_DIR, f"{b}_{trade_date}_{now():%H%M}.csv")
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["exec_id", "account", "symbol", "side", "qty", "price",
                                               "cl_ord_id", "exec_time"])
            w.writeheader()
            w.writerows(bk["trades"])
        log("file-drop", "INFO", "statement_received", broker=b, file=path, trades=len(bk["trades"]))
        bk["trades"] = []


# ============================================================
# EMS：TWAP 执行算法
# ============================================================
def ems_step():
    for p in list(parents.values()):
        if faults.active("algo_stall", pm=p["pm"]):
            continue
        b = ROUTING[p["symbol"]]
        if not session_up[b]:
            continue                                             # 连接断了，先不发
        c = children.get(p.get("open_child"))
        if c:
            if bar - c["sent_bar"] >= CHILD_TIMEOUT_BARS and not c.get("cancel_sent"):
                c["cancel_sent"] = True
                c["status_before_cancel"] = c["status"]
                c["status"] = "PENDING_CANCEL"
                update_child(c)
                fix_send(b, "F", c["id"], [("11", new_id("CX", 8)), ("41", c["id"]), ("55", c["symbol"]),
                                           ("54", fix.side_code(c["side"])), ("38", c["qty"])])
                event(p["id"], c["id"], "ems", "CHILD_CANCEL_SENT", f"not filled within {CHILD_TIMEOUT_BARS} bars")
            continue
        remaining = p["qty"] - p["filled"]
        if remaining <= 0:
            continue
        slices_left = max(1, TWAP_SLICES - p["slices"])
        qty = min(remaining, max(LOT, math.ceil(remaining / slices_left / LOT) * LOT))
        px = last_price[p["symbol"]]
        limit = round(px * (1 + CHILD_LIMIT_BPS / 1e4) if p["side"] == "BUY" else px * (1 - CHILD_LIMIT_BPS / 1e4), 2)
        cid = new_id("C", 9)
        c = {"id": cid, "parent": p["id"], "pm": p["pm"], "broker": b, "symbol": p["symbol"], "side": p["side"],
             "qty": qty, "limit": limit, "status": "PENDING_NEW", "filled": 0, "notional": 0.0, "sent_bar": bar}
        children[cid] = c
        p["open_child"], p["slices"] = cid, p["slices"] + 1
        cur.execute("INSERT INTO child_orders (child_id, parent_id, pm_id, broker, symbol, side, qty, limit_price, "
                    "status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING_NEW', %s, %s)",
                    (cid, p["id"], p["pm"], b, p["symbol"], p["side"], qty, limit, now(), now()))
        event(p["id"], cid, "ems", "CHILD_SENT", f"slice {p['slices']}/{TWAP_SLICES} {qty} @ {limit} to {b}")
        fix_send(b, "D", cid, [("11", cid), ("1", p["pm"]), ("55", p["symbol"]), ("54", fix.side_code(p["side"])),
                               ("38", qty), ("40", "2"), ("44", limit), ("59", "0")])


# ============================================================
# OMS + 交易前风控
# ============================================================
def projected():
    """持仓 + 还没执行完的母单 = 如果全部做完会是多少。"""
    pos = defaultdict(int, positions)
    for p in parents.values():
        pos[(p["pm"], p["symbol"])] += (1 if p["side"] == "BUY" else -1) * (p["qty"] - p["filled"])
    return pos


def risk_check(pm_id, symbol, side, qty, price):
    cfg = PMS[pm_id]
    cur.execute("SELECT paused FROM control WHERE pm_id = %s", (pm_id,))
    row = cur.fetchone()
    if row and row[0]:
        return "PM_PAUSED"
    if qty * price > MAX_ORDER_NOTIONAL:
        return f"MAX_ORDER_NOTIONAL ({qty * price:,.0f} > {MAX_ORDER_NOTIONAL:,})"
    pos = projected()
    before = pos[(pm_id, symbol)]
    after = before + (qty if side == "BUY" else -qty)
    if abs(after * price) > cfg["max_position"]:
        return f"POSITION_LIMIT ({abs(after * price):,.0f} > {cfg['max_position']:,})"
    pos[(pm_id, symbol)] = after
    gross = sum(abs(q * last_price[s]) for (p, s), q in pos.items() if p == pm_id)
    net = sum(q * last_price[s] for (p, s), q in pos.items() if p == pm_id)
    if gross > cfg["max_gross"]:
        return f"GROSS_LIMIT ({gross:,.0f} > {cfg['max_gross']:,})"
    if abs(net) > cfg["max_net"]:
        return f"NET_LIMIT ({abs(net):,.0f} > {cfg['max_net']:,})"
    new_short = max(0, -after) - max(0, -before)
    if new_short > 0:
        if new_short > locates[symbol]:
            return f"NO_LOCATE (need {new_short}, available {locates[symbol]})"
        locates[symbol] -= new_short
        cur.execute("UPDATE locates SET available = %s, updated_at = %s WHERE symbol = %s",
                    (locates[symbol], now(), symbol))
    return None


def create_parent(pm_id, symbol, side, qty, price, retry=False):
    pid = new_id("P", 8)
    cur.execute("INSERT INTO parent_orders (parent_id, pm_id, symbol, side, qty, ref_price, algo, status, "
                "created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, 'TWAP', 'NEW', %s, %s)",
                (pid, pm_id, symbol, side, qty, price, now(), now()))
    event(pid, None, "oms", "PARENT_NEW", f"{pm_id} {side} {qty} {symbol} @ {price}" + (" (resubmit)" if retry else ""))
    reason = risk_check(pm_id, symbol, side, qty, price)
    if reason:
        cur.execute("UPDATE parent_orders SET status = 'REJECTED', reject_reason = %s, updated_at = %s "
                    "WHERE parent_id = %s", (reason, now(), pid))
        event(pid, None, "risk", "RISK_REJECT", reason, level="WARN", pm_id=pm_id, symbol=symbol)
        return
    cur.execute("UPDATE parent_orders SET status = 'WORKING', updated_at = %s WHERE parent_id = %s", (now(), pid))
    event(pid, None, "risk", "RISK_PASS")
    parents[pid] = {"id": pid, "pm": pm_id, "symbol": symbol, "side": side, "qty": qty, "filled": 0,
                    "notional": 0.0, "slices": 0, "open_child": None}
    if not retry and faults.active("duplicate_parent", pm=pm_id):
        log("oms", "WARN", "submit_ack_timeout_resubmitting", parent_id=pid, pm_id=pm_id)
        create_parent(pm_id, symbol, side, qty, price, retry=True)


def ops_step():
    """值班的人可以把母单状态改成 CANCEL_REQUESTED，系统会把它和它的子单一起撤掉。"""
    cur.execute("SELECT parent_id FROM parent_orders WHERE status = 'CANCEL_REQUESTED'")
    for (pid,) in cur.fetchall():
        p = parents.pop(pid, None)
        for c in [c for c in children.values() if c["parent"] == pid]:
            c["status"] = "CANCELLED"
            update_child(c, reason="manual cancel by ops")
            children.pop(c["id"], None)
        cur.execute("UPDATE parent_orders SET status = 'CANCELLED', reject_reason = 'manual cancel by ops', "
                    "updated_at = %s WHERE parent_id = %s", (now(), pid))
        event(pid, None, "oms", "PARENT_CANCELLED", "manual cancel by ops" if p else "not working (already gone)")


def oms_step():
    working = {(p["pm"], p["symbol"]) for p in parents.values()}
    for pm_id, cfg in PMS.items():
        targets = STRATEGIES[cfg["strategy"]](hist, last_price, cfg)
        bad = faults.active("bad_deploy", pm=pm_id)
        for symbol, target in targets.items():
            if bad:
                target *= 40                                     # 新版本的 bug：单位算错
            if (pm_id, symbol) in working:
                continue
            diff = target - positions[(pm_id, symbol)]
            if abs(diff) * last_price[symbol] < MIN_ORDER_NOTIONAL:
                continue
            create_parent(pm_id, symbol, "BUY" if diff > 0 else "SELL", abs(diff), last_price[symbol])


# ============================================================
# 启动 / 开盘 / 收盘
# ============================================================
last_true_price = {}


def startup():
    for pm_id, c in PMS.items():
        cur.execute("INSERT INTO pms VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pm_id) DO UPDATE SET "
                    "name = EXCLUDED.name, strategy = EXCLUDED.strategy, capital = EXCLUDED.capital, "
                    "max_gross = EXCLUDED.max_gross, max_net = EXCLUDED.max_net, max_position = EXCLUDED.max_position",
                    (pm_id, c["name"], c["strategy"], c["capital"], c["max_gross"], c["max_net"], c["max_position"]))
    for b, name in BROKERS.items():
        cur.execute("INSERT INTO fix_sessions VALUES (%s, %s, 'UP', %s, NULL, %s) ON CONFLICT (broker) DO UPDATE "
                    "SET status = 'UP', updated_at = EXCLUDED.updated_at", (b, name, now(), now()))
        fix_send(b, "A", None, [("98", "0"), ("108", "30")])     # 登录
        fix_receive_admin(b, "A")
    # 重启后：上次没做完的单在内存里已经没了，标记为取消；持仓从数据库恢复
    cur.execute("UPDATE parent_orders SET status = 'CANCELLED', reject_reason = 'engine restart', updated_at = %s "
                "WHERE status IN ('NEW', 'WORKING')", (now(),))
    cur.execute("UPDATE child_orders SET status = 'CANCELLED', reason = 'engine restart', updated_at = %s "
                "WHERE status NOT IN ('FILLED', 'CANCELLED', 'REJECTED')", (now(),))
    cur.execute("SELECT pm_id, symbol, qty FROM positions")
    for pm_id, symbol, qty in cur.fetchall():
        positions[(pm_id, symbol)] = int(qty)
    db.commit()


def fix_receive_admin(b, msg_type):
    raw = fix.build(msg_type, b, OUR_COMP_ID, [("98", "0"), ("108", "30")] if msg_type == "A" else [])
    cur.execute("INSERT INTO fix_messages (ts, broker, direction, msg_type, cl_ord_id, raw) "
                "VALUES (%s, %s, 'IN', %s, NULL, %s)", (now(), b, msg_type, raw))
    log("fix-gateway", "INFO", "fix_in", broker=b, raw=raw)


def start_of_day(trade_date):
    global locates
    locates = dict(DAILY_LOCATES)
    for s, n in locates.items():
        cur.execute("INSERT INTO locates VALUES (%s, %s, %s) ON CONFLICT (symbol) DO UPDATE SET "
                    "available = EXCLUDED.available, updated_at = EXCLUDED.updated_at", (s, n, now()))
    log("ops", "INFO", "start_of_day", trade_date=str(trade_date), locates=locates)


def apply_faults():
    for service, level, msg, fields in faults.tick():
        log(service, level, msg, **fields)
    for b in BROKERS:
        down = bool(faults.active("broker_session_down", broker=b))
        if down and session_up[b]:
            session_up[b] = False
            cur.execute("UPDATE fix_sessions SET status = 'DOWN', updated_at = %s WHERE broker = %s", (now(), b))
        elif not down and not session_up[b]:
            session_up[b] = True
            cur.execute("UPDATE fix_sessions SET status = 'UP', updated_at = %s WHERE broker = %s", (now(), b))
            fix_send(b, "A", None, [("98", "0"), ("108", "30")])
            fix_receive_admin(b, "A")
        if session_up[b]:
            cur.execute("UPDATE fix_sessions SET last_heartbeat = %s WHERE broker = %s", (now(), b))
    f = faults.active("locate_shortage")
    if f and locates.get(f["symbol"]):
        locates[f["symbol"]] = 0
        cur.execute("UPDATE locates SET available = 0, updated_at = %s WHERE symbol = %s", (now(), f["symbol"]))


def main():
    global bar
    df = pd.read_csv(CSV_FILE)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
    days = sorted(df["ts"].dt.date.unique())
    startup()
    log("engine", "INFO", "start", pms=list(PMS), brokers=list(BROKERS), speed=SPEED)
    while True:
        for day in days:
            start_of_day(now().date())
            for _, group in df[df["ts"].dt.date == day].groupby("ts"):
                t0 = time.time()
                apply_faults()
                stale = faults.active("stale_feed")
                for row in group.itertuples():
                    last_true_price[row.symbol] = round(row.close, 4)          # 真实市场价（券商撮合用）
                    if stale and stale["symbol"] == row.symbol and row.symbol in last_price:
                        continue                                              # 我们的行情没更新
                    last_price[row.symbol] = round(row.close, 4)
                    cur.execute("INSERT INTO market_data VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (now(), row.symbol, last_price[row.symbol], int(row.volume)))
                for s, px in last_price.items():
                    hist[s].append(px)
                    del hist[s][:-300]
                for b in BROKERS:
                    broker_step(b)                                            # ②③ 券商 → FIX 回报
                ops_step()                                                    # 值班人员的手动操作
                ems_step()                                                    # ④ 执行算法
                if bar % REBALANCE_EVERY == 0:
                    oms_step()                                                # ⑤ 策略 → 母单 → 风控
                db.commit()
                bar += 1
                time.sleep(max(0.0, 60 / SPEED - (time.time() - t0)))
            write_statements(now().strftime("%Y%m%d"))
            db.commit()


if __name__ == "__main__":
    main()
