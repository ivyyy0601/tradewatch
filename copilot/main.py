"""
AI 值班助手（tw-copilot）
================================================
读：Kafka ops.alerts（monitor 每触发 / 解决一条告警就发一条；./tw investigate、./tw approve 也发到这里）
写：数据库 incidents / investigations / investigation_steps / action_proposals；Telegram；docs/incidents/drafts/
执行：只有人批准的建议，才会往 ops.commands 发命令

流水线：告警 → 归并成事故 → 等一会儿（让相关告警归进来）→ 取证（剧本）→ AI 诊断 → 复核 → 通知 / 建议 → 关闭后起草事故记录
没有 ANTHROPIC_API_KEY 也能跑：只用规则判断，照样归并、取证、通知
"""

import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

from common import Bus, consumer, ensure_topics, heartbeat, log, poll
from copilot import approvals, config, correlate, db, investigate, llm, notify

SERVICE = "copilot"


def telegram_loop():
    """单独一个线程：接收 Telegram 上"批准 / 拒绝"按钮的点击"""
    conn = db.open_rw()
    cur = conn.cursor()
    offset = 0
    while True:
        try:
            offset, clicks = notify.updates(offset)
            for cb_id, data, who, msg_id in clicks:
                parts = data.split(":")
                if len(parts) != 3 or parts[0] != "ap" or not parts[1].isdigit():
                    notify.answer(cb_id, "unknown button")
                    continue
                msg = approvals.decide(cur, int(parts[1]), parts[2] == "y", who)
                notify.answer(cb_id, msg)
                notify.edit(msg_id, f"{msg}（{who}）")
        except Exception as e:                                   # noqa: BLE001
            log(SERVICE, "WARN", "telegram_poll_failed", error=str(e)[:200])
            time.sleep(10)


def handle_event(rw, ro, ev):
    kind = ev.get("event")
    if kind == "fired":
        correlate.ingest(rw, ev)
    elif kind == "resolved":
        closed = correlate.mark_resolved(rw, int(ev["alert_id"]), correlate._ts(ev["resolved_at"])) \
            if db.one(rw, "SELECT 1 AS x FROM incident_alerts WHERE alert_id = %s", (int(ev["alert_id"]),)) else None
        if closed:
            investigate.write_draft(rw, closed)
    elif kind == "investigate":
        iid = ev.get("incident_id")
        if not iid and ev.get("alert_id"):
            a = db.one(rw, "SELECT alert_id, rule, key, severity, pm_id, detail, fired_at, resolved_at FROM alerts "
                           "WHERE alert_id = %s", (int(ev["alert_id"]),))
            if not a:
                log(SERVICE, "WARN", "investigate_unknown_alert", alert_id=ev["alert_id"])
                return
            iid, _, _ = correlate.ingest(rw, a, source="live")
        inc = db.one(rw, "SELECT * FROM incidents WHERE incident_id = %s", (int(iid),))
        if not inc:
            log(SERVICE, "WARN", "investigate_unknown_incident", incident_id=iid)
            return
        as_of = None
        if inc["status"] != "open":                              # 已经结束的事故：按当时的数据调查
            as_of = correlate._ts(inc["last_alert_at"]) + timedelta(seconds=config.DEBOUNCE_S)
        investigate.run(rw, ro, int(iid), trigger="manual", as_of=as_of)
    elif kind == "proposal_decided":
        pass                                                     # 下面的定时任务会执行已批准的建议


def main():
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    rw_conn, ro_conn = db.open_rw(), db.open_ro()
    rw, ro = rw_conn.cursor(), ro_conn.cursor()
    db.ensure_schema(rw)
    c = consumer(SERVICE, ["ops.alerts"])
    log(SERVICE, "INFO", "start", ai="on" if llm.enabled() else "off (no ANTHROPIC_API_KEY)", model=config.MODEL,
        autonomy=config.AUTONOMY, telegram="on" if notify.enabled() else "off")
    if notify.enabled():
        threading.Thread(target=telegram_loop, daemon=True).start()
    last_sync = last_hb = 0.0
    while True:
        try:
            m = poll(c, 1.0, SERVICE)
            if m:
                handle_event(rw, ro, m[2] if isinstance(m[2], dict) else {})
            if time.time() - last_sync >= 20:
                _, closed = correlate.sync(rw)
                for iid in closed:
                    investigate.write_draft(rw, iid)
                approvals.process(rw, ro, bus)
                for row in investigate.due(rw):
                    investigate.run(rw, ro, row["incident_id"])
                last_sync = time.time()
            if time.time() - last_hb >= 30:
                heartbeat(bus, SERVICE, ai=llm.enabled(), spent_today=round(investigate.spent_today(rw), 3))
                bus.flush()
                last_hb = time.time()
        except Exception as e:                                   # noqa: BLE001  一次出错不能让值班助手停下来
            log(SERVICE, "ERROR", "copilot_cycle_failed", error=str(e)[:300], trace=traceback.format_exc()[-800:])
            time.sleep(5)
            try:
                rw_conn.close()
                ro_conn.close()
            except Exception:                                    # noqa: BLE001
                pass
            rw_conn, ro_conn = db.open_rw(), db.open_ro()
            rw, ro = rw_conn.cursor(), ro_conn.cursor()


if __name__ == "__main__":
    main()
