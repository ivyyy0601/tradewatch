"""
order-store（tw-order-store）：把所有消息写进数据库，数据库是唯一的"真相来源"
================================================
读：orders.events、fix.log、md.bars、ops.state
写：PostgreSQL（parent_orders、child_orders、executions、order_events、fix_messages、market_data、
    locates、control、fix_sessions、pms、service_heartbeats）

只有它往这些表里写。如果它卡住了，交易还在继续，但数据库和值班屏幕会停在旧的数据上，
消息会堆在 Kafka 里（consumer lag 变大）；它恢复后会把积压的消息补写进去。
每批消息先写进数据库并提交，再告诉 Kafka"这批读完了"：中途崩溃也不会丢消息，重读的消息靠唯一编号去重。
"""

import json
import os
import time
from datetime import datetime

from chaos import ChaosClient
from common import connect, consumer, ensure_topics, log, now, poll

SERVICE = "order-store"
TOPICS = ["orders.events", "fix.log", "md.bars", "ops.state"]

chaos = ChaosClient()


def write_event(cur, e):
    pid, cid, ts = e.get("parent_id"), e.get("child_id"), e["ts"]
    if "parent_new" in e:
        p = e["parent_new"]
        cur.execute("INSERT INTO parent_orders (parent_id, pm_id, symbol, side, qty, ref_price, algo, status, "
                    "created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, 'TWAP', 'NEW', %s, %s) "
                    "ON CONFLICT (parent_id) DO NOTHING",
                    (pid, p["pm_id"], p["symbol"], p["side"], p["qty"], p["ref_price"], ts, ts))
    if "child_new" in e:
        c = e["child_new"]
        cur.execute("INSERT INTO child_orders (child_id, parent_id, pm_id, broker, symbol, side, qty, limit_price, "
                    "status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING_NEW', %s, %s) "
                    "ON CONFLICT (child_id) DO NOTHING",
                    (cid, pid, c["pm_id"], c["broker"], c["symbol"], c["side"], c["qty"], c["limit_price"], ts, ts))
    if "execution" in e:
        x = e["execution"]
        cur.execute("INSERT INTO executions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (exec_id) DO NOTHING",
                    (x["exec_id"], x["child_id"], x["parent_id"], x["pm_id"], x["broker"], x["symbol"], x["side"],
                     x["qty"], x["price"], ts))
    if e.get("child"):
        c = e["child"]
        cur.execute("UPDATE child_orders SET status = %s, filled_qty = %s, avg_price = %s, "
                    "reason = COALESCE(%s, reason), updated_at = %s WHERE child_id = %s",
                    (c["status"], c["filled_qty"], c["avg_price"], c.get("reason"), ts, cid))
    if e.get("parent"):
        p = e["parent"]
        cur.execute("UPDATE parent_orders SET status = COALESCE(%s, status), filled_qty = COALESCE(%s, filled_qty), "
                    "avg_price = COALESCE(%s, avg_price), reject_reason = COALESCE(%s, reject_reason), "
                    "updated_at = %s WHERE parent_id = %s",
                    (p.get("status"), p.get("filled_qty"), p.get("avg_price"), p.get("reject_reason"), ts, pid))
    cur.execute("INSERT INTO order_events (parent_id, child_id, service, event_type, detail, ts, event_uid) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_uid) DO NOTHING",
                (pid, cid, e["service"], e["type"], e.get("detail"), ts, e["eid"]))


def write_state(cur, s):
    k = s.get("kind")
    if k == "heartbeat":
        cur.execute("INSERT INTO service_heartbeats VALUES (%s, %s, %s) ON CONFLICT (service) DO UPDATE SET "
                    "last_seen = EXCLUDED.last_seen, detail = EXCLUDED.detail", (s["service"], s["ts"], s.get("detail")))
    elif k == "locates":
        cur.execute("INSERT INTO locates VALUES (%s, %s, %s) ON CONFLICT (symbol) DO UPDATE SET "
                    "available = EXCLUDED.available, updated_at = EXCLUDED.updated_at",
                    (s["symbol"], s["available"], s["ts"]))
    elif k == "control":
        cur.execute("INSERT INTO control VALUES (%s, %s, %s, %s, %s) ON CONFLICT (pm_id) DO UPDATE SET "
                    "paused = EXCLUDED.paused, reason = EXCLUDED.reason, changed_by = EXCLUDED.changed_by, "
                    "changed_at = EXCLUDED.changed_at", (s["pm_id"], s["paused"], s["reason"], s["changed_by"], s["ts"]))
    elif k == "fix_session":
        cur.execute("INSERT INTO fix_sessions (broker, broker_name, status, last_heartbeat, last_msg_in, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (broker) DO UPDATE SET status = EXCLUDED.status, "
                    "last_heartbeat = COALESCE(EXCLUDED.last_heartbeat, fix_sessions.last_heartbeat), "
                    "last_msg_in = COALESCE(EXCLUDED.last_msg_in, fix_sessions.last_msg_in), "
                    "updated_at = EXCLUDED.updated_at",
                    (s["broker"], s["broker_name"], s["status"], s.get("last_heartbeat"), s.get("last_msg_in"), s["ts"]))
    elif k == "pm":
        cur.execute("INSERT INTO pms VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pm_id) DO UPDATE SET "
                    "name = EXCLUDED.name, strategy = EXCLUDED.strategy, capital = EXCLUDED.capital, "
                    "max_gross = EXCLUDED.max_gross, max_net = EXCLUDED.max_net, max_position = EXCLUDED.max_position",
                    (s["pm_id"], s["name"], s["strategy"], s["capital"], s["max_gross"], s["max_net"], s["max_position"]))


def write(cur, topic, v):
    if topic == "orders.events":
        write_event(cur, v)
    elif topic == "fix.log":
        cur.execute("INSERT INTO fix_messages (ts, broker, direction, msg_type, cl_ord_id, raw, msg_uid) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (msg_uid) DO NOTHING",
                    (v["ts"], v["broker"], v["direction"], v["msg_type"], v.get("cl_ord_id"), v["raw"], v["uid"]))
    elif topic == "md.bars" and v.get("type") == "bar":
        for s in v.get("updated", []):
            cur.execute("INSERT INTO market_data VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (v["ts"], s, v["prices"][s], v.get("volumes", {}).get(s)))
    elif topic == "ops.state":
        write_state(cur, v)


def main():
    db = connect(SERVICE)
    cur = db.cursor()
    with open(os.path.join(os.path.dirname(__file__), "..", "sql", "02_kafka.sql")) as fh:
        cur.execute(fh.read())                              # 新增的表和字段（可以重复执行）
    db.autocommit = False
    ensure_topics(SERVICE)
    c = consumer(SERVICE, TOPICS, auto_commit=False)
    log(SERVICE, "INFO", "start", topics=TOPICS)
    last_slow_log = 0.0
    while True:
        stall = chaos.active("store_stall")
        if stall:                                           # 数据库写不进去（比如被锁住了），只能干等
            if time.time() - last_slow_log >= 60:
                waited = int((now() - datetime.fromisoformat(stall["start"])).total_seconds())
                log(SERVICE, "WARN", "db_commit_slow", waited_s=waited, detail="waiting for lock on order_events")
                last_slow_log = time.time()
            time.sleep(1)
            continue
        n, t0 = 0, time.time()
        while n < 500 and time.time() - t0 < 0.5:
            m = poll(c, 0.1, SERVICE)
            if m is None:
                continue
            topic, _, v = m
            try:
                cur.execute("SAVEPOINT msg")
                write(cur, topic, v)
                cur.execute("RELEASE SAVEPOINT msg")
            except Exception as e:                          # noqa: BLE001  一条坏消息不能卡住所有人
                cur.execute("ROLLBACK TO SAVEPOINT msg")
                log(SERVICE, "ERROR", "write_failed", topic=topic, error=str(e).strip()[:300],
                    message=json.dumps(v, default=str)[:300])
            n += 1
        cur.execute("INSERT INTO service_heartbeats VALUES (%s, %s, %s) ON CONFLICT (service) DO UPDATE SET "
                    "last_seen = EXCLUDED.last_seen, detail = EXCLUDED.detail",
                    (SERVICE, now(), json.dumps({"batch": n})))
        db.commit()                                         # 先写进数据库
        if n:
            c.commit(asynchronous=False)                    # 再告诉 Kafka 这批读完了


if __name__ == "__main__":
    main()
