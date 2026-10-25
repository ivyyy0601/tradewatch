"""
FIX 网关（tw-fix-gateway）：我们和券商之间的"翻译 + 电话线"
================================================
读：exec.orders（EMS 要发的子单 / 撤单）、fix.from.<券商>（券商发来的 FIX 原文）
写：fix.to.<券商>（发给券商的 FIX 原文）、exec.reports（翻译好的回报 → EMS）、fix.log（每条消息存档）、ops.state（连接状态、心跳）

和每家券商有一条 FIX 连接（session）。连接断了：
  - 收不到券商的消息（它们堆在 Kafka 的 fix.from.<券商> 里，重连后一次性收到）
  - 告诉 EMS 先别往这家发单
"""

import time

import fix
from chaos import ChaosClient
from common import Bus, consumer, ensure_topics, heartbeat, log, new_id, now, poll
from config import BROKERS, OUR_COMP_ID

SERVICE = "fix-gateway"

bus = None
chaos = ChaosClient()
session_up = {b: True for b in BROKERS}
pending_out = {b: [] for b in BROKERS}          # 断线期间要发的消息，重连后补发
last_msg_in = {b: None for b in BROKERS}


def record(broker, direction, msg_type, cl_ord_id, raw):
    bus.send("fix.log", broker, {"uid": new_id("M", 16), "ts": now(), "broker": broker, "direction": direction,
                                 "msg_type": msg_type, "cl_ord_id": cl_ord_id, "raw": raw})


def session_state(b, **extra):
    bus.send("ops.state", b, {"kind": "fix_session", "broker": b, "broker_name": BROKERS[b],
                              "status": "UP" if session_up[b] else "DOWN", "ts": now(), **extra})


def logon(b):
    """登录：我们发 35=A，券商回 35=A（这里在网关内部完成）。"""
    out = fix.build("A", OUR_COMP_ID, b, [("98", "0"), ("108", "30")])
    record(b, "OUT", "A", None, out)
    log(SERVICE, "INFO", "fix_out", broker=b, raw=out)
    back = fix.build("A", b, OUR_COMP_ID, [("98", "0"), ("108", "30")])
    record(b, "IN", "A", None, back)
    log(SERVICE, "INFO", "fix_in", broker=b, raw=back)


def send_to_broker(o):
    b = o["broker"]
    if o["action"] == "new":
        fields = [("11", o["cl_ord_id"]), ("1", o["pm_id"]), ("55", o["symbol"]), ("54", fix.side_code(o["side"])),
                  ("38", o["qty"]), ("40", "2"), ("44", o["limit"]), ("59", "0")]
        msg_type, cl = "D", o["cl_ord_id"]
    else:
        fields = [("11", o["cl_ord_id"]), ("41", o["orig_cl_ord_id"]), ("55", o["symbol"]),
                  ("54", fix.side_code(o["side"])), ("38", o["qty"])]
        msg_type, cl = "F", o["orig_cl_ord_id"]
    if not session_up[b]:
        pending_out[b].append(o)
        log(SERVICE, "WARN", "queued_session_down", broker=b, cl_ord_id=cl)
        return
    raw = fix.build(msg_type, OUR_COMP_ID, b, fields)
    record(b, "OUT", msg_type, cl, raw)
    log(SERVICE, "INFO", "fix_out", broker=b, cl_ord_id=cl, raw=raw)
    bus.send(f"fix.to.{b}", cl, raw)


def receive_from_broker(b, raw):
    f = fix.parse(raw)
    msg_type = f.get("35")
    cl = f.get("41") if msg_type == "9" else f.get("11")
    last_msg_in[b] = now()
    record(b, "IN", msg_type, cl, raw)
    log(SERVICE, "INFO", "fix_in", broker=b, cl_ord_id=cl, raw=raw)
    if msg_type in ("8", "9"):
        bus.send("exec.reports", cl, {"type": "report", "broker": b, "msg_type": msg_type, "fields": f, "ts": now()})


def check_sessions():
    f = chaos.active("broker_session_down")
    for b in BROKERS:
        down = bool(f and f["broker"] == b)
        if down and session_up[b]:
            session_up[b] = False
            log(SERVICE, "ERROR", "fix_session_lost", broker=b, detail="heartbeat timeout, TCP connection reset")
            session_state(b)
            bus.send("exec.reports", b, {"type": "session", "broker": b, "status": "DOWN", "ts": now()})
        elif not down and not session_up[b]:
            session_up[b] = True
            log(SERVICE, "INFO", "fix_session_restored", broker=b)
            logon(b)
            session_state(b)
            bus.send("exec.reports", b, {"type": "session", "broker": b, "status": "UP", "ts": now()})
            for o in pending_out[b]:
                send_to_broker(o)
            pending_out[b] = []


def main():
    global bus
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    c_out = consumer(SERVICE, ["exec.orders"], group=f"{SERVICE}-out")
    c_in = {b: consumer(SERVICE, [f"fix.from.{b}"], group=f"{SERVICE}-{b}") for b in BROKERS}
    for b in BROKERS:
        logon(b)
        session_state(b, last_heartbeat=now())
        bus.send("exec.reports", b, {"type": "session", "broker": b, "status": "UP", "ts": now()})
    log(SERVICE, "INFO", "start", brokers=list(BROKERS))
    last_hb = 0.0
    while True:
        check_sessions()
        busy = False
        m = poll(c_out, 0.2, SERVICE)
        if m:
            send_to_broker(m[2])
            busy = True
        for b in BROKERS:
            if not session_up[b]:
                continue                                  # 线断了：券商的消息先堆在 Kafka 里
            m = poll(c_in[b], 0.05, SERVICE)
            if m:
                receive_from_broker(b, m[2])
                busy = True
        if time.time() - last_hb >= 15:                   # 心跳：每 15 秒告诉大家连接还活着
            for b in BROKERS:
                if session_up[b]:
                    session_state(b, last_heartbeat=now(), last_msg_in=last_msg_in[b])
            heartbeat(bus, SERVICE, sessions={b: ("UP" if u else "DOWN") for b, u in session_up.items()})
            last_hb = time.time()
        if busy:
            bus.p.poll(0)
        else:
            bus.flush()


if __name__ == "__main__":
    main()
