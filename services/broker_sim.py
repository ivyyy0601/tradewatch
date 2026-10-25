"""
模拟券商（tw-broker-sim）：代表"外面的世界"——两家券商 NBRG 和 HRBR
================================================
真实工作里你看不到券商内部的日志，所以这个服务几乎不打日志。你只能从 FIX 消息和对账单看到它做了什么。

读：fix.to.<券商>（我们发来的 FIX 新单 / 撤单）、md.exchange（交易所的真实价格，用来撮合）
写：fix.from.<券商>（确认、成交、拒单、撤单回报，FIX 原文）
每个交易日结束：把当天所有成交写成对账单 CSV（data/statements/），和真实的 prime broker 文件一样。
"""

import csv
import os
import random

import fix
from chaos import ChaosClient
from common import Bus, consumer, ensure_topics, log, new_id, now, poll
from config import BROKERS, LOT, OUR_COMP_ID

SERVICE = "broker-sim"
STATEMENT_DIR = os.getenv("STATEMENT_DIR", "data/statements")

bus = None
chaos = ChaosClient()
true_price = {}
books = {b: {"book": {}, "trades": []} for b in BROKERS}


def report(b, o, exec_type, ord_status, text=None, last_qty=None, last_px=None, exec_id=None):
    fields = [("37", o["order_id"]), ("11", o["cl"]), ("17", exec_id or new_id("EX", 10)), ("150", exec_type),
              ("39", ord_status), ("1", o["pm"]), ("55", o["symbol"]), ("54", fix.side_code(o["side"])),
              ("38", o["qty"]), ("32", last_qty), ("31", last_px), ("14", o["cum"]),
              ("151", 0 if exec_type in ("4", "8") else o["qty"] - o["cum"]), ("58", text)]
    send(b, "8", o["cl"], fields)


def send(b, msg_type, cl, fields):
    bus.send(f"fix.from.{b}", cl, fix.build(msg_type, b, OUR_COMP_ID, fields))


def on_order(b, raw):
    f = fix.parse(raw)
    bk = books[b]
    if f.get("35") == "D":                                       # 新单
        o = {"cl": f["11"], "symbol": f["55"], "side": "BUY" if f["54"] == "1" else "SELL", "qty": int(f["38"]),
             "limit": float(f["44"]), "cum": 0, "pm": f.get("1"), "order_id": new_id(b[:2] + "-", 7), "open": True}
        if chaos.active("symbol_halt", symbol=o["symbol"]):
            o["open"] = False
            report(b, o, "8", "8", text="SYMBOL_HALTED")
            return
        bk["book"][o["cl"]] = o
        report(b, o, "0", "0")
    elif f.get("35") == "F":                                     # 撤单
        o = bk["book"].get(f["41"])
        if o and o["open"]:
            o["open"] = False
            report(b, o, "4", "4")
        else:
            send(b, "9", f["41"], [("11", f.get("11")), ("41", f["41"]), ("39", "2" if o else "8"), ("434", "1"),
                                   ("58", "TOO_LATE_TO_CANCEL" if o else "UNKNOWN_ORDER")])


def match(b):
    """撮合：交易所价格到了限价就成交（75% 机会全部成交，否则成交一半）。"""
    bk = books[b]
    for o in list(bk["book"].values()):
        px = true_price.get(o["symbol"])
        if not o["open"] or px is None or chaos.active("symbol_halt", symbol=o["symbol"]):
            continue
        if (o["side"] == "BUY" and px <= o["limit"]) or (o["side"] == "SELL" and px >= o["limit"]):
            leaves = o["qty"] - o["cum"]
            qty = leaves if random.random() < 0.75 else max(LOT, leaves // 2 // LOT * LOT)
            o["cum"] += qty
            o["open"] = o["cum"] < o["qty"]
            exec_id = new_id("EX", 10)
            bk["trades"].append({"exec_id": exec_id, "account": o["pm"], "symbol": o["symbol"], "side": o["side"],
                                 "qty": qty, "price": round(px, 4), "cl_ord_id": o["cl"],
                                 "exec_time": now().isoformat(timespec="seconds")})
            if chaos.active("lost_exec_report", broker=b):
                continue                                         # 券商成交了，但这条回报丢了
            report(b, o, "F", "1" if o["open"] else "2", last_qty=qty, last_px=round(px, 4), exec_id=exec_id)
    # 已经结束的单不用再留着
    for cl in [cl for cl, o in bk["book"].items() if not o["open"]]:
        if len(bk["book"]) > 5000:
            bk["book"].pop(cl)


def write_statements(trade_date):
    os.makedirs(STATEMENT_DIR, exist_ok=True)
    for b, bk in books.items():
        path = os.path.join(STATEMENT_DIR, f"{b}_{trade_date}_{now():%H%M}.csv")
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["exec_id", "account", "symbol", "side", "qty", "price",
                                               "cl_ord_id", "exec_time"])
            w.writeheader()
            w.writerows(bk["trades"])
        log("file-drop", "INFO", "statement_received", broker=b, file=path, trades=len(bk["trades"]))
        bk["trades"] = []


def main():
    global bus
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    c_md = consumer(SERVICE, ["md.exchange"], from_end=True, group=f"{SERVICE}-md")
    c_in = {b: consumer(SERVICE, [f"fix.to.{b}"], group=f"{SERVICE}-{b}") for b in BROKERS}
    log(SERVICE, "INFO", "start", brokers=list(BROKERS))
    while True:
        m = poll(c_md, 0.2, SERVICE)
        if m:
            v = m[2]
            if v["type"] == "bar":
                true_price.update(v["prices"])
                for b in BROKERS:
                    match(b)
            elif v["type"] == "eod":
                write_statements(v["trade_date"])
        for b in BROKERS:
            if chaos.active("broker_silent", broker=b):
                continue                                         # 券商系统卡住了：我们的单堆在它门口没人处理
            while True:
                m = poll(c_in[b], 0.02, SERVICE)
                if not m:
                    break
                on_order(b, m[2])
        bus.flush()


if __name__ == "__main__":
    main()
