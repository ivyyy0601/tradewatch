"""
OMS 服务（tw-oms）：PM 策略 + 订单管理 + 交易前风控
================================================
读：md.bars（行情）、orders.events（成交，用来更新持仓）、ops.commands（暂停 / 恢复 PM）
写：orders.events（PARENT_NEW、RISK_PASS、RISK_REJECT）、orders.parent（通过风控的母单 → EMS）、ops.state

每 5 根 K 线：每个 PM 的策略给出目标仓位 → 目标 − 现有持仓 = 母单 → 风控检查 → 通过就交给 EMS。
重启后从数据库恢复持仓、PM 开关、借券额度和还在执行的母单。
"""

from collections import defaultdict
from datetime import timedelta

from chaos import ChaosClient
from common import Bus, connect, consumer, emit, ensure_topics, heartbeat, is_stale, log, new_id, now, poll
from config import DAILY_LOCATES, MAX_ORDER_NOTIONAL, MIN_ORDER_NOTIONAL, PMS, REBALANCE_EVERY
from strategies import STRATEGIES

SERVICE = "oms"

bus = None
chaos = ChaosClient()
last_price = {}
hist = defaultdict(list)
positions = defaultdict(int)          # (pm, symbol) → 我们记的持仓
parents = {}                          # 还在执行的母单：id → {pm, symbol, side, qty, filled}
paused = {}                           # pm → 暂停原因
locates = dict(DAILY_LOCATES)
bad_deploy_on = set()


# ============================================================
# 风控
# ============================================================
def projected():
    """持仓 + 还没执行完的母单 = 如果全部做完会是多少。"""
    pos = defaultdict(int, positions)
    for p in parents.values():
        pos[(p["pm"], p["symbol"])] += (1 if p["side"] == "BUY" else -1) * max(0, p["qty"] - p["filled"])
    return pos


def set_locates(symbol, available):
    locates[symbol] = available
    bus.send("ops.state", symbol, {"kind": "locates", "symbol": symbol, "available": available, "ts": now()})


def risk_check(pm_id, symbol, side, qty, price):
    cfg = PMS[pm_id]
    if pm_id in paused:
        return "PM_PAUSED"
    if qty * price > MAX_ORDER_NOTIONAL:
        return f"MAX_ORDER_NOTIONAL ({qty * price:,.0f} > {MAX_ORDER_NOTIONAL:,})"
    pos = projected()
    before = pos[(pm_id, symbol)]
    after = before + (qty if side == "BUY" else -qty)
    if abs(after * price) > cfg["max_position"]:
        return f"POSITION_LIMIT ({abs(after * price):,.0f} > {cfg['max_position']:,})"
    pos[(pm_id, symbol)] = after
    gross = sum(abs(q * last_price[s]) for (p, s), q in pos.items() if p == pm_id and s in last_price)
    net = sum(q * last_price[s] for (p, s), q in pos.items() if p == pm_id and s in last_price)
    if gross > cfg["max_gross"]:
        return f"GROSS_LIMIT ({gross:,.0f} > {cfg['max_gross']:,})"
    if abs(net) > cfg["max_net"]:
        return f"NET_LIMIT ({abs(net):,.0f} > {cfg['max_net']:,})"
    new_short = max(0, -after) - max(0, -before)
    if new_short > 0:
        if new_short > locates.get(symbol, 0):
            return f"NO_LOCATE (need {new_short}, available {locates.get(symbol, 0)})"
        set_locates(symbol, locates[symbol] - new_short)
    return None


# ============================================================
# OMS
# ============================================================
def create_parent(pm_id, symbol, side, qty, price, retry=False):
    pid = new_id("P", 8)
    emit(bus, "oms", "PARENT_NEW", pid, detail=f"{pm_id} {side} {qty} {symbol} @ {price}" + (" (resubmit)" if retry else ""),
         parent_new={"pm_id": pm_id, "symbol": symbol, "side": side, "qty": qty, "ref_price": price})
    reason = risk_check(pm_id, symbol, side, qty, price)
    if reason:
        emit(bus, "risk", "RISK_REJECT", pid, detail=reason, level="WARN", pm_id=pm_id, symbol=symbol,
             parent={"status": "REJECTED", "reject_reason": reason})
        return
    emit(bus, "risk", "RISK_PASS", pid, parent={"status": "WORKING"})
    parents[pid] = {"pm": pm_id, "symbol": symbol, "side": side, "qty": qty, "filled": 0}
    bus.send("orders.parent", pid, {"parent_id": pid, "pm_id": pm_id, "symbol": symbol, "side": side,
                                    "qty": qty, "ref_price": price, "ts": now()})
    if not retry and chaos.active("duplicate_parent", pm=pm_id):
        log("oms", "WARN", "submit_ack_timeout_resubmitting", parent_id=pid, pm_id=pm_id)
        create_parent(pm_id, symbol, side, qty, price, retry=True)


def oms_step():
    working = {(p["pm"], p["symbol"]) for p in parents.values()}
    for pm_id, cfg in PMS.items():
        if any(s not in last_price for s in cfg["symbols"]):
            continue
        if pm_id in paused:
            continue                                             # 暂停的 PM 不再生成新单（已有的母单 EMS 会继续执行）
        targets = STRATEGIES[cfg["strategy"]](hist, last_price, cfg)
        bad = pm_id in bad_deploy_on
        for symbol, target in targets.items():
            if bad:
                target *= 40                                     # 新版本的 bug：单位算错
            if (pm_id, symbol) in working:
                continue
            diff = target - positions[(pm_id, symbol)]
            if abs(diff) * last_price[symbol] < MIN_ORDER_NOTIONAL:
                continue
            create_parent(pm_id, symbol, "BUY" if diff > 0 else "SELL", abs(diff), last_price[symbol])


def check_chaos():
    """策略程序的"发版"：新版本上线时会打一条重启日志（真实系统也会有）。"""
    f = chaos.active("bad_deploy")
    want = {f["pm"]} if f else set()
    for pm in want - bad_deploy_on:
        log(f"strategy-{pm}", "INFO", "strategy_restarted", pm_id=pm, version="3.7.0",
            detail="deployed by release pipeline")
    bad_deploy_on.clear()
    bad_deploy_on.update(want)
    f = chaos.active("locate_shortage")
    if f and locates.get(f["symbol"]):
        set_locates(f["symbol"], 0)


# ============================================================
# 收到的消息
# ============================================================
def on_event(e):
    t = e.get("type")
    if t == "FILL" and e.get("execution"):
        x = e["execution"]
        sign = 1 if x["side"] == "BUY" else -1
        positions[(x["pm_id"], x["symbol"])] += sign * int(x["qty"])
        p = parents.get(e["parent_id"])
        if p:
            p["filled"] += int(x["qty"])
    elif t in ("PARENT_DONE", "PARENT_CANCELLED"):
        parents.pop(e["parent_id"], None)


def on_command(cmd):
    pm = cmd.get("pm_id")
    if cmd.get("cmd") not in ("pause", "resume") or pm not in PMS:
        return
    by, reason = cmd.get("by", "?"), cmd.get("reason", "")
    if cmd["cmd"] == "pause":
        paused[pm] = reason
        log(SERVICE, "WARN", "pm_paused", pm_id=pm, by=by, reason=reason)
    else:
        paused.pop(pm, None)
        log(SERVICE, "INFO", "pm_resumed", pm_id=pm, by=by, reason=reason)
    bus.send("ops.state", pm, {"kind": "control", "pm_id": pm, "paused": cmd["cmd"] == "pause",
                               "reason": reason, "changed_by": by, "ts": now()})


def startup():
    for pm_id, c in PMS.items():
        bus.send("ops.state", pm_id, {"kind": "pm", "pm_id": pm_id, **{k: c[k] for k in
                 ("name", "strategy", "capital", "max_gross", "max_net", "max_position")}})
    db = connect(SERVICE)
    cur = db.cursor()
    cur.execute("SELECT pm_id, symbol, qty FROM positions")
    for pm_id, symbol, qty in cur.fetchall():
        positions[(pm_id, symbol)] = int(qty)
    cur.execute("SELECT pm_id, reason FROM control WHERE paused")
    for pm_id, reason in cur.fetchall():
        paused[pm_id] = reason or ""
    cur.execute("SELECT symbol, available FROM locates")
    for symbol, available in cur.fetchall():
        locates[symbol] = int(available)
    # 还在执行的母单（10 分钟内还有动静的），EMS 会接着执行；更早的 EMS 会取消
    cur.execute("SELECT parent_id, pm_id, symbol, side, qty, filled_qty FROM parent_orders "
                "WHERE status = 'WORKING' AND updated_at > %s", (now() - timedelta(minutes=10),))
    for pid, pm_id, symbol, side, qty, filled in cur.fetchall():
        parents[pid] = {"pm": pm_id, "symbol": symbol, "side": side, "qty": int(qty), "filled": int(filled)}
    db.close()
    log(SERVICE, "INFO", "start", pms=list(PMS), positions=len(positions), working_parents=len(parents),
        paused=list(paused))


def main():
    global bus
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    startup()
    c = consumer(SERVICE, ["md.bars", "orders.events", "ops.commands"], from_end=True)
    while True:
        m = poll(c, 1.0, SERVICE)
        if m is None:
            continue
        topic, _, v = m
        if topic == "md.bars":
            if v["type"] == "sod":
                for s, n in DAILY_LOCATES.items():
                    set_locates(s, n)
                log("ops", "INFO", "start_of_day", trade_date=v["trade_date"], locates=dict(locates))
            elif v["type"] == "bar" and not is_stale(v):
                for s, px in v["prices"].items():
                    last_price[s] = px
                    hist[s].append(px)
                    del hist[s][:-300]
                check_chaos()
                if v["bar"] % REBALANCE_EVERY == 0:
                    oms_step()
                heartbeat(bus, SERVICE, bar=v["bar"], working_parents=len(parents))
        elif topic == "orders.events":
            on_event(v)
        elif topic == "ops.commands":
            on_command(v)
        bus.flush()


if __name__ == "__main__":
    main()
