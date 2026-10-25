"""
EMS 服务（tw-ems）：执行算法 TWAP
================================================
读：orders.parent（通过风控的母单）、md.bars（行情 + 时钟）、exec.reports（FIX 网关翻译好的券商回报）、ops.commands（撤母单）
写：exec.orders（要发的子单 / 撤单 → FIX 网关）、orders.events（CHILD_SENT、CHILD_ACK、FILL、PARENT_DONE……）

每根 K 线：给每个母单发下一个子单（母单拆成 8 份，限价 = 现价 ± 5 个基点）；
子单 2 根 K 线还没成交完就撤掉，剩下的下一根 K 线重新下。
和某家券商的 FIX 连接断了，就先不往那家发。
"""

import math
from datetime import timedelta

from chaos import ChaosClient
from common import Bus, connect, consumer, emit, ensure_topics, heartbeat, is_stale, log, new_id, now, poll
from config import BROKERS, CHILD_LIMIT_BPS, CHILD_TIMEOUT_BARS, LOT, ROUTING, TWAP_SLICES

SERVICE = "ems"

bus = None
chaos = ChaosClient()
bar = 0
last_price = {}
parents = {}                          # 还在执行的母单
children = {}                         # 还没结束的子单（cl_ord_id → 子单）
session_up = {b: True for b in BROKERS}
recently_done = {}                    # 刚结束的子单：券商晚到的回报（比如撤单太晚被拒）不算"不认识的单"


def avg(c):
    return round(c["notional"] / c["filled"], 4) if c["filled"] else None


def child_snap(c, reason=None):
    return {"status": c["status"], "filled_qty": c["filled"], "avg_price": avg(c), "reason": reason}


def finish_child(c):
    children.pop(c["id"], None)
    recently_done[c["id"]] = c["status"]
    while len(recently_done) > 5000:
        recently_done.pop(next(iter(recently_done)))
    p = parents.get(c["parent"])
    if p and p.get("open_child") == c["id"]:
        p["open_child"] = None


# ============================================================
# 券商回报（FIX 网关已经把 FIX 原文翻译成字典）
# ============================================================
def on_report(r):
    if r.get("type") == "session":
        session_up[r["broker"]] = r["status"] == "UP"
        return
    broker, msg_type, f = r["broker"], r["msg_type"], r["fields"]
    cl_ord_id = f.get("41") if msg_type == "9" else f.get("11")
    c = children.get(cl_ord_id)
    if c is None and cl_ord_id in recently_done:
        log(SERVICE, "INFO", "late_report_for_done_child", broker=broker, cl_ord_id=cl_ord_id, msg_type=msg_type,
            exec_type=f.get("150"), our_status=recently_done[cl_ord_id], text=f.get("58"))
        return
    if c is None:
        log(SERVICE, "WARN", "unknown_cl_ord_id", broker=broker, cl_ord_id=cl_ord_id, msg_type=msg_type)
        return
    p = parents.get(c["parent"])
    if msg_type == "9":                                          # 撤单被拒
        if f.get("39") == "2":                                   # 券商说这单早就全部成交了
            c["status"] = "FILLED"
            emit(bus, "ems", "CANCEL_REJECTED", c["parent"], c["id"], f.get("58"), level="WARN",
                 child=child_snap(c, f"cancel rejected {f.get('58')}: broker reports order filled"))
            finish_child(c)                                      # 母单按我们自己记到的成交量继续执行
        elif f.get("58") == "UNKNOWN_ORDER":                     # 券商根本不知道这张单（比如券商那边重启过）
            c["status"] = "CANCELLED"
            emit(bus, "ems", "CANCEL_REJECTED", c["parent"], c["id"], f.get("58"), level="WARN",
                 child=child_snap(c, "cancel rejected: broker does not know this order"))
            finish_child(c)
        else:
            c["status"] = c.pop("status_before_cancel", c["status"])
            c["cancel_sent"] = False
            emit(bus, "ems", "CANCEL_REJECTED", c["parent"], c["id"], f.get("58"), level="WARN",
                 child=child_snap(c, f"cancel rejected: {f.get('58')}"))
        return

    exec_type = f.get("150")
    if exec_type == "0":                                         # 券商确认收到
        c["status"] = "NEW"
        emit(bus, "ems", "CHILD_ACK", c["parent"], c["id"], f"broker order {f.get('37')}", child=child_snap(c))
    elif exec_type == "8":                                       # 券商拒单
        c["status"] = "REJECTED"
        emit(bus, "ems", "CHILD_REJECTED", c["parent"], c["id"], f.get("58"), level="WARN",
             child=child_snap(c, f.get("58")))
        finish_child(c)
    elif exec_type == "4":                                       # 撤单成功
        c["status"] = "CANCELLED"
        emit(bus, "ems", "CHILD_CANCELLED", c["parent"], c["id"], f"leaves {f.get('151')}", child=child_snap(c))
        finish_child(c)
    elif exec_type == "F":                                       # 成交
        qty, px, cum = int(f["32"]), float(f["31"]), int(f["14"])
        if cum != c["filled"] + qty:                             # 券商的累计数和我们对不上
            log(SERVICE, "WARN", "cum_qty_mismatch", cl_ord_id=c["id"], broker=broker,
                ours=c["filled"] + qty, broker_cum=cum)
        c["filled"] += qty
        c["notional"] += qty * px
        c["status"] = "FILLED" if f.get("39") == "2" else "PARTIALLY_FILLED"
        parent = None
        if p:
            p["filled"] += qty
            p["notional"] += qty * px
            p_status = "DONE" if p["filled"] >= p["qty"] else "WORKING"
            parent = {"status": p_status, "filled_qty": p["filled"], "avg_price": round(p["notional"] / p["filled"], 4)}
        emit(bus, "ems", "FILL", c["parent"], c["id"], f"{qty} @ {px}", qty=qty, price=px,
             child=child_snap(c), parent=parent,
             execution={"exec_id": f["17"], "child_id": c["id"], "parent_id": c["parent"], "pm_id": c["pm"],
                        "broker": broker, "symbol": c["symbol"], "side": c["side"], "qty": qty, "price": px})
        if p and p["filled"] >= p["qty"]:
            emit(bus, "ems", "PARENT_DONE", p["id"], detail=f"{p['filled']} @ {p['notional'] / p['filled']:.4f}")
            parents.pop(p["id"], None)
        if c["status"] == "FILLED":
            finish_child(c)


# ============================================================
# TWAP
# ============================================================
def send_cancel(c):
    c["cancel_sent"] = True
    c["status_before_cancel"] = c["status"]
    c["status"] = "PENDING_CANCEL"
    bus.send("exec.orders", c["id"], {"action": "cancel", "broker": c["broker"], "cl_ord_id": new_id("CX", 8),
                                      "orig_cl_ord_id": c["id"], "symbol": c["symbol"], "side": c["side"],
                                      "qty": c["qty"]})


def ems_step():
    for p in list(parents.values()):
        if chaos.active("algo_stall", pm=p["pm"]):
            continue
        b = ROUTING[p["symbol"]]
        if not session_up[b]:
            continue                                             # 连接断了，先不发
        c = children.get(p.get("open_child"))
        if c:
            if c.get("cancel_sent"):
                continue
            if bar - c["sent_bar"] >= CHILD_TIMEOUT_BARS or bar < c["sent_bar"]:   # 行情服务重启后 bar 会从 0 重新数
                send_cancel(c)
                emit(bus, "ems", "CHILD_CANCEL_SENT", p["id"], c["id"], f"not filled within {CHILD_TIMEOUT_BARS} bars",
                     child=child_snap(c))
            continue
        remaining = p["qty"] - p["filled"]
        if remaining <= 0 or p["symbol"] not in last_price:
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
        emit(bus, "ems", "CHILD_SENT", p["id"], cid, f"slice {p['slices']}/{TWAP_SLICES} {qty} @ {limit} to {b}",
             child_new={"pm_id": p["pm"], "broker": b, "symbol": p["symbol"], "side": p["side"], "qty": qty,
                        "limit_price": limit})
        bus.send("exec.orders", cid, {"action": "new", "broker": b, "cl_ord_id": cid, "pm_id": p["pm"],
                                      "symbol": p["symbol"], "side": p["side"], "qty": qty, "limit": limit})


def on_parent(m):
    parents[m["parent_id"]] = {"id": m["parent_id"], "pm": m["pm_id"], "symbol": m["symbol"], "side": m["side"],
                               "qty": int(m["qty"]), "filled": 0, "notional": 0.0, "slices": 0, "open_child": None}


def on_command(cmd):
    """值班的人用 tw cancel 撤母单：把它和它的子单一起撤掉。"""
    if cmd.get("cmd") != "cancel":
        return
    pid = cmd.get("parent_id")
    p = parents.pop(pid, None)
    for c in [c for c in children.values() if c["parent"] == pid]:
        if not c.get("cancel_sent"):
            send_cancel(c)
            emit(bus, "ems", "CHILD_CANCEL_SENT", pid, c["id"], "manual cancel by ops", child=child_snap(c))
    reason = f"manual cancel by {cmd.get('by', 'ops')}: {cmd.get('reason', '')}"
    emit(bus, "ems", "PARENT_CANCELLED", pid, detail=reason if p else "not working (already gone)", level="WARN",
         parent={"status": "CANCELLED", "reject_reason": reason} if p else None)


def startup():
    """重启后：10 分钟内还在执行的母单接着做；更早的取消。券商那边还挂着的子单全部撤掉。"""
    db = connect(SERVICE)
    cur = db.cursor()
    cutoff = now() - timedelta(minutes=10)
    cur.execute("SELECT parent_id, pm_id, symbol, side, qty, filled_qty, COALESCE(avg_price, 0), updated_at, "
                "(SELECT COUNT(*) FROM child_orders c WHERE c.parent_id = p.parent_id) "
                "FROM parent_orders p WHERE status = 'WORKING'")
    for pid, pm, symbol, side, qty, filled, avg_px, updated, slices in cur.fetchall():
        if updated > cutoff:
            parents[pid] = {"id": pid, "pm": pm, "symbol": symbol, "side": side, "qty": int(qty),
                            "filled": int(filled), "notional": int(filled) * float(avg_px), "slices": int(slices),
                            "open_child": None}
        else:
            emit(bus, "ems", "PARENT_CANCELLED", pid, detail="stale after restart",
                 parent={"status": "CANCELLED", "reject_reason": "stale after restart"})
    cur.execute("SELECT child_id, parent_id, pm_id, broker, symbol, side, qty, limit_price, status, filled_qty, "
                "COALESCE(avg_price, 0) FROM child_orders "
                "WHERE status IN ('PENDING_NEW', 'NEW', 'PARTIALLY_FILLED', 'PENDING_CANCEL')")
    for cid, pid, pm, b, symbol, side, qty, limit, status, filled, avg_px in cur.fetchall():
        c = {"id": cid, "parent": pid, "pm": pm, "broker": b, "symbol": symbol, "side": side, "qty": int(qty),
             "limit": float(limit), "status": status, "filled": int(filled), "notional": int(filled) * float(avg_px),
             "sent_bar": 0}
        children[cid] = c
        if pid in parents:
            parents[pid]["open_child"] = cid
        send_cancel(c)
        emit(bus, "ems", "CHILD_CANCEL_SENT", pid, cid, "ems restart: cancel orphaned child", child=child_snap(c))
    db.close()
    bus.flush()
    log(SERVICE, "INFO", "start", working_parents=len(parents), open_children=len(children))


def main():
    global bus, bar
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    startup()
    c = consumer(SERVICE, ["md.bars", "orders.parent", "exec.reports", "ops.commands"], from_end=True)
    while True:
        m = poll(c, 1.0, SERVICE)
        if m is None:
            continue
        topic, _, v = m
        if topic == "md.bars":
            if v["type"] == "bar" and not is_stale(v):
                bar = v["bar"]
                last_price.update(v["prices"])
                ems_step()
                heartbeat(bus, SERVICE, bar=bar, working_parents=len(parents), open_children=len(children))
        elif topic == "orders.parent":
            on_parent(v)
        elif topic == "exec.reports":
            on_report(v)
        elif topic == "ops.commands":
            on_command(v)
        bus.flush()


if __name__ == "__main__":
    main()
