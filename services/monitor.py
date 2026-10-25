"""
监控服务（tw-monitor）：公司的告警系统
================================================
每 30 秒跑一遍规则（大部分是一条 SQL），发现问题：
  → 写进 alerts 表 → 发 Telegram（没配置就只打日志）
  → P1 的"下错单"类问题自动暂停这个 PM（kill switch），恢复必须由人用 tw resume 操作
问题消失后告警自动标记为已解决。

另外：
  - 每 30 秒记录每个服务在 Kafka 上还有多少条消息没读（consumer lag）
  - 券商对账单到了以后，生成当天的复盘报告并对账（tools/report.py）

严重级别：P1 = 可能在亏钱或者持仓不对，立刻处理；P2 = 交易受影响，15 分钟内处理；P3 = 记录，复盘时看
告警只告诉你"症状"，原因要你自己用日志、SQL、tw trace 去查。
"""

import json
import os
import time
import urllib.parse
import urllib.request

from confluent_kafka import Consumer, TopicPartition

from common import KAFKA, Bus, connect, ensure_topics, log, now
from config import BROKERS
from tools import report

SERVICE = "monitor"
INTERVAL = int(os.getenv("MONITOR_INTERVAL", "30"))
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
LAG_LIMIT = int(os.getenv("LAG_LIMIT", "100"))

# 每个服务（consumer group）读哪些 topic
LAG_GROUPS = {
    "order-store": ["orders.events", "fix.log", "md.bars", "ops.state"],
    "oms": ["md.bars", "orders.events", "ops.commands"],
    "ems": ["md.bars", "orders.parent", "exec.reports", "ops.commands"],
    "fix-gateway-out": ["exec.orders"],
    **{f"fix-gateway-{b}": [f"fix.from.{b}"] for b in BROKERS},
    **{f"broker-sim-{b}": [f"fix.to.{b}"] for b in BROKERS},
}

# 规则：名字、级别、自动动作、SQL（返回 key, pm_id, detail 三列）、下一步建议
RULES = [
    ("market_data_stale", "P2", None, """
        SELECT symbol, NULL, symbol || ' 行情 ' || ROUND(EXTRACT(EPOCH FROM now() - MAX(ts))) || ' 秒没有更新'
        FROM market_data WHERE ts > now() - interval '1 day'
        GROUP BY symbol HAVING MAX(ts) < now() - interval '3 minutes'
     """, "行情源、行情服务，还是写数据库的 order-store 出了问题？看 tw status 的心跳和 lag"),

    ("fix_session_down", "P2", None, """
        SELECT broker, NULL, broker || ' FIX 连接断开（' || to_char(updated_at, 'HH24:MI:SS') || ' 起）'
        FROM fix_sessions WHERE status = 'DOWN'
     """, "影响哪些股票、哪些 PM？fix-gateway 日志里断线前后发生了什么？"),

    ("child_no_ack", "P2", None, """
        SELECT broker, NULL, broker || ' 有 ' || COUNT(*) || ' 个子单发出 2 分钟以上还没收到确认（最早 '
               || to_char(MIN(created_at), 'HH24:MI:SS') || '）'
        FROM child_orders WHERE status = 'PENDING_NEW' AND created_at < now() - interval '2 minutes'
        GROUP BY broker
     """, "fix_messages 里有没有发出去（OUT）？券商有没有回（IN）？连接是不是正常？"),

    ("broker_rejects", "P2", None, """
        SELECT c.broker || ':' || COALESCE(split_part(c.reason, ' ', 1), '?'), NULL,
               c.broker || ' 最近 10 分钟拒了 ' || COUNT(*) || ' 个子单，原因 ' || COALESCE(c.reason, '?')
        FROM child_orders c
        WHERE c.status = 'REJECTED' AND c.updated_at > now() - interval '10 minutes'
        GROUP BY c.broker, c.reason HAVING COUNT(*) >= 3
     """, "拒单原因（FIX tag 58）是什么？集中在哪只股票？要不要联系券商？"),

    ("risk_reject_spike", "P3", None, """
        SELECT pm_id, pm_id, pm_id || ' 最近 15 分钟 ' || rej || '/' || n || ' 个母单被风控拒绝'
        FROM (SELECT pm_id, COUNT(*) AS n, COUNT(*) FILTER (WHERE status = 'REJECTED') AS rej
              FROM parent_orders WHERE created_at > now() - interval '15 minutes' GROUP BY pm_id) t
        WHERE rej >= 5 AND rej * 2 >= n
     """, "按 reject_reason 分组看：是正常的限额 / 借券用完，还是策略在下不正常的单？"),

    ("order_size_anomaly", "P1", "pause", """
        WITH recent AS (
            SELECT pm_id, MAX(qty * ref_price) AS x FROM parent_orders
            WHERE created_at > now() - interval '10 minutes' AND reject_reason IS DISTINCT FROM 'PM_PAUSED'
            GROUP BY pm_id),
        base AS (
            SELECT pm_id, AVG(qty * ref_price) AS mu, STDDEV_SAMP(qty * ref_price) AS sd, COUNT(*) AS n
            FROM parent_orders
            WHERE created_at BETWEEN now() - interval '6 hours' AND now() - interval '10 minutes'
              AND reject_reason IS DISTINCT FROM 'PM_PAUSED'
            GROUP BY pm_id)
        SELECT r.pm_id, r.pm_id, r.pm_id || ' 最近 10 分钟最大母单 ' || TO_CHAR(r.x, 'FM999,999,999') || ' 美元，平时平均 '
               || TO_CHAR(b.mu, 'FM999,999,999') || '（z = ' || ROUND((r.x - b.mu) / NULLIF(b.sd, 0), 1) || '）'
        FROM recent r JOIN base b USING (pm_id)
        WHERE b.n >= 20 AND r.x > 2 * b.mu AND (r.x - b.mu) / NULLIF(b.sd, 0) > 4
     """, "这个 PM 的策略最近有没有发版或者重启？看 strategy 日志和它的母单数量"),

    ("duplicate_parent", "P1", "pause", """
        SELECT a.pm_id, a.pm_id, a.pm_id || ' ' || a.symbol || ' ' || a.side || ' ' || a.qty || ' 在 '
               || to_char(a.created_at, 'HH24:MI:SS') || ' 出现重复母单 ' || a.parent_id || ' / ' || b.parent_id
        FROM parent_orders a JOIN parent_orders b
          ON a.pm_id = b.pm_id AND a.symbol = b.symbol AND a.side = b.side AND a.qty = b.qty
         AND a.parent_id < b.parent_id AND ABS(EXTRACT(EPOCH FROM a.created_at - b.created_at)) < 5
        WHERE a.created_at > now() - interval '10 minutes'
     """, "两张单是不是都在执行？持仓有没有因此翻倍？oms 日志里有没有重发？"),

    ("parent_stalled", "P2", None, """
        SELECT p.pm_id, p.pm_id, p.pm_id || ' 有 ' || COUNT(*) || ' 个母单 5 分钟以上没有发出新的子单（例：'
               || MIN(p.parent_id) || '）'
        FROM parent_orders p
        WHERE p.status = 'WORKING' AND p.created_at < now() - interval '5 minutes'
          AND NOT EXISTS (SELECT 1 FROM child_orders c WHERE c.parent_id = p.parent_id
                          AND c.created_at > now() - interval '5 minutes')
        GROUP BY p.pm_id
     """, "是只有这个 PM，还是所有 PM？连接正常吗？用 tw trace 看母单最后一件事是什么"),

    ("service_heartbeat_stale", "P2", None, """
        SELECT service, NULL, service || ' 已经 ' || ROUND(EXTRACT(EPOCH FROM now() - last_seen)) || ' 秒没有心跳'
        FROM service_heartbeats WHERE last_seen < now() - interval '3 minutes' AND service <> 'monitor'
     """, "docker compose ps 看容器在不在；它真的挂了，还是只是心跳没写进数据库？"),

    ("kafka_consumer_lag", "P2", None, """
        SELECT group_id || ':' || topic, NULL, group_id || ' 在 ' || topic || ' 上积压了 ' || lag || ' 条消息'
        FROM kafka_lag WHERE ts = (SELECT MAX(ts) FROM kafka_lag) AND lag > %(lag_limit)s
     """, "这个服务还活着吗？它的日志里有没有报错或变慢？"),

    ("slippage_high", "P3", None, """
        SELECT pm_id, pm_id, pm_id || ' 最近 30 分钟平均滑点 ' || ROUND(AVG(bps)) || ' bps（' || COUNT(*) || ' 个母单）'
        FROM (SELECT pm_id, CASE side WHEN 'BUY' THEN 1 ELSE -1 END * (avg_price - ref_price) / ref_price * 1e4 AS bps
              FROM parent_orders WHERE status = 'DONE' AND updated_at > now() - interval '30 minutes') t
        GROUP BY pm_id HAVING AVG(bps) > 50
     """, "成交价比下单时的价格差多少？是行情问题还是执行问题？"),
]
STICKY = {"recon_break"}          # 这些告警不会自动解决，要人确认后用 tw resolve 关掉


# ============================================================
# 告警
# ============================================================
def telegram(text):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text}).encode()
        urllib.request.urlopen(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, timeout=5)
    except Exception as e:                                       # noqa: BLE001
        log(SERVICE, "WARN", "telegram_failed", error=str(e))


def fire(cur, bus, rule, key, severity, pm_id, detail, action=None, hint=""):
    cur.execute("SELECT alert_id FROM alerts WHERE rule = %s AND key = %s AND resolved_at IS NULL", (rule, key))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE alerts SET detail = %s WHERE alert_id = %s", (detail, row[0]))
        return False
    cur.execute("INSERT INTO alerts (rule, key, severity, pm_id, detail, fired_at) VALUES (%s, %s, %s, %s, %s, %s) "
                "RETURNING alert_id", (rule, key, severity, pm_id, detail, now()))
    alert_id = cur.fetchone()[0]
    taken = "无"
    if action == "pause" and pm_id:
        bus.send("ops.commands", pm_id, {"cmd": "pause", "pm_id": pm_id, "by": "monitor",
                                         "reason": f"auto kill switch: {rule} (alert {alert_id})", "ts": now()})
        bus.flush()
        taken = f"已自动暂停 {pm_id}（kill switch），恢复要人工执行 tw resume"
    log(SERVICE, "ERROR" if severity == "P1" else "WARN", "alert_fired", alert_id=alert_id, rule=rule,
        severity=severity, key=key, pm_id=pm_id, detail=detail, action=taken)
    telegram(f"[{severity}] {rule}  {key}\n{detail}\n自动动作：{taken}\n下一步：{hint}\nrunbook: docs/runbooks/{rule}.md")
    return True


def resolve_missing(cur, active):
    cur.execute("SELECT alert_id, rule, key, severity FROM alerts WHERE resolved_at IS NULL")
    for alert_id, rule, key, severity in cur.fetchall():
        if rule in STICKY or (rule, key) in active:
            continue
        cur.execute("UPDATE alerts SET resolved_at = %s WHERE alert_id = %s", (now(), alert_id))
        log(SERVICE, "INFO", "alert_resolved", alert_id=alert_id, rule=rule, key=key)
        if severity in ("P1", "P2"):
            telegram(f"[RESOLVED] {rule}  {key}")


def run_rules(cur, bus):
    active = set()
    for name, severity, action, sql, hint in RULES:
        try:
            cur.execute(sql, {"lag_limit": LAG_LIMIT})
            rows = cur.fetchall()
        except Exception as e:                                   # noqa: BLE001  一条规则坏了不影响其他规则
            log(SERVICE, "ERROR", "rule_failed", rule=name, error=str(e).strip()[:300])
            continue
        for key, pm_id, detail in rows:
            active.add((name, str(key)))
            fire(cur, bus, name, str(key), severity, pm_id, detail, action, hint)
    resolve_missing(cur, active)


# ============================================================
# Kafka consumer lag
# ============================================================
_lag_consumers = {}


def record_lag(cur):
    t, total = now(), {}
    for group, topics in LAG_GROUPS.items():
        c = _lag_consumers.get(group)
        if c is None:
            c = _lag_consumers[group] = Consumer({"bootstrap.servers": KAFKA, "group.id": group,
                                                  "enable.auto.commit": False})
        try:
            committed = c.committed([TopicPartition(tp, 0) for tp in topics], timeout=5)
            for tp in committed:
                if tp.offset < 0:                                # 这个服务还没读过这个 topic
                    continue
                _, high = c.get_watermark_offsets(TopicPartition(tp.topic, 0), timeout=5)
                lag = max(0, high - tp.offset)
                cur.execute("INSERT INTO kafka_lag VALUES (%s, %s, %s, %s)", (t, group, tp.topic, lag))
                total[group] = total.get(group, 0) + lag
        except Exception as e:                                   # noqa: BLE001
            log(SERVICE, "WARN", "lag_check_failed", group=group, error=str(e))
    cur.execute("DELETE FROM kafka_lag WHERE ts < now() - interval '3 days'")
    return total


# ============================================================
# 每日复盘
# ============================================================
def maybe_report(cur, bus, lag):
    if lag.get("order-store", 0) > 20:
        return                                                   # 数据库还没追上，现在对账会误报
    for suffix in report.pending_sessions():
        path, breaks = report.generate(suffix)
        log(SERVICE, "INFO", "daily_report_written", file=path, recon_breaks=sum(breaks.values()))
        for broker, n in breaks.items():
            if n:
                fire(cur, bus, "recon_break", f"{suffix}:{broker}", "P1", None,
                     f"{broker} 对账单（{suffix}）和我们的成交记录有 {n} 处对不上，详见 {path}",
                     hint="哪几笔对不上？是我们漏记还是券商多记？看 recon_breaks 表和 fix_messages")
        telegram(f"[REPORT] 复盘报告 {suffix} 已生成：{path}")


def main():
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    db = connect(SERVICE)
    cur = db.cursor()
    for attempt in range(60):                                    # 等 order-store 建好新表
        cur.execute("SELECT to_regclass('kafka_lag') IS NOT NULL AND to_regclass('service_heartbeats') IS NOT NULL")
        if cur.fetchone()[0]:
            break
        time.sleep(2)
    log(SERVICE, "INFO", "start", rules=[r[0] for r in RULES], interval_s=INTERVAL,
        telegram="on" if TG_TOKEN and TG_CHAT else "off")
    while True:
        t0 = time.time()
        try:
            lag = record_lag(cur)
            run_rules(cur, bus)
            maybe_report(cur, bus, lag)
            cur.execute("INSERT INTO service_heartbeats VALUES (%s, %s, %s) ON CONFLICT (service) DO UPDATE SET "
                        "last_seen = EXCLUDED.last_seen, detail = EXCLUDED.detail",
                        (SERVICE, now(), json.dumps({"lag": lag})))
        except Exception as e:                                   # noqa: BLE001
            log(SERVICE, "ERROR", "monitor_cycle_failed", error=str(e).strip()[:300])
            try:
                db.close()
            except Exception:                                    # noqa: BLE001
                pass
            db = connect(SERVICE)
            cur = db.cursor()
        time.sleep(max(1.0, INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    main()
