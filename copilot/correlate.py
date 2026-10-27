"""归并：把相关的告警合成一个事故（纯代码，不用 AI）
规则：时间相近（COPILOT_CORRELATE_WINDOW_MIN 分钟内）+ 拓扑上是同一个部件或直接上下游"""

from datetime import datetime, timedelta, timezone

from common import log
from copilot import config, db, topology

SERVICE = "copilot"
RANK = {"P1": 1, "P2": 2, "P3": 3}


def _ts(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(v))


def _arr(v):
    """TEXT[] 列：真驱动返回 list，有的环境返回 '{a,b}' 字符串"""
    if isinstance(v, (list, tuple)):
        return list(v)
    s = str(v or "").strip("{}")
    return [x.strip('"') for x in s.split(",") if x]


def ingest(cur, alert, now=None, source="live"):
    """收进一条告警。返回 (incident_id, 是不是新事故, 要不要重新调查)"""
    now = now or datetime.now(timezone.utc)
    aid = int(alert["alert_id"])
    linked = db.one(cur, "SELECT incident_id FROM incident_alerts WHERE alert_id = %s", (aid,))
    if linked:
        if alert.get("resolved_at"):
            mark_resolved(cur, aid, _ts(alert["resolved_at"]))
        return linked["incident_id"], False, False

    fired = _ts(alert["fired_at"])
    comps = sorted(topology.components(alert))
    window = timedelta(minutes=config.CORRELATE_WINDOW_MIN)
    candidates = db.rows(cur, """
        SELECT incident_id, components, severity, investigated_at FROM incidents
        WHERE source = %s AND last_alert_at >= %s AND opened_at <= %s AND opened_at >= %s
          AND (status = 'open' OR resolved_at >= %s)
        ORDER BY last_alert_at DESC""",
        (source, (fired - window).isoformat(), (fired + window).isoformat(),
         (fired - timedelta(minutes=config.MAX_INCIDENT_SPAN_MIN)).isoformat(), (fired - window).isoformat()))
    target = next((c for c in candidates if topology.related(comps, _arr(c["components"]))), None)
    debounce_at = (now if source == "live" else fired) + timedelta(seconds=config.DEBOUNCE_S)

    if target is None:
        title = f"{alert['rule']} {alert.get('key') or ''}".strip()
        r = db.one(cur, """
            INSERT INTO incidents (title, severity, components, source, opened_at, last_alert_at, investigate_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING incident_id""",
            (title, alert["severity"], comps, source, fired.isoformat(), fired.isoformat(), debounce_at.isoformat()))
        iid, is_new, reinvestigate = r["incident_id"], True, False
        log(SERVICE, "INFO", "incident_opened", incident_id=iid, title=title, components=comps)
    else:
        iid, is_new = target["incident_id"], False
        merged = sorted(set(_arr(target["components"])) | set(comps))
        worse = RANK.get(alert["severity"], 9) < RANK.get(target["severity"], 9)
        reinvestigate = bool(target["investigated_at"]) and worse      # 已经调查过，又来了更严重的告警 → 重新调查
        cur.execute("""
            UPDATE incidents SET components = %s, last_alert_at = GREATEST(last_alert_at, %s),
                   severity = CASE WHEN %s THEN %s ELSE severity END, status = 'open', resolved_at = NULL,
                   investigate_after = CASE WHEN %s THEN %s ELSE investigate_after END
            WHERE incident_id = %s""",
            (merged, fired.isoformat(), worse, alert["severity"], reinvestigate, debounce_at.isoformat(), iid))
        log(SERVICE, "INFO", "alert_joined_incident", incident_id=iid, alert_id=aid, rule=alert["rule"])

    cur.execute("""
        INSERT INTO incident_alerts (incident_id, alert_id, rule, key, severity, pm_id, detail, fired_at, resolved_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
        (iid, aid, alert["rule"], str(alert.get("key") or ""), alert["severity"], alert.get("pm_id"),
         alert.get("detail"), fired.isoformat(), _ts(alert["resolved_at"]).isoformat() if alert.get("resolved_at") else None))
    if alert.get("resolved_at"):
        mark_resolved(cur, aid, _ts(alert["resolved_at"]))
    return iid, is_new, reinvestigate


def mark_resolved(cur, alert_id, resolved_at):
    """一条告警解决了；如果事故里所有告警都解决了，事故就关闭。返回关闭的事故号（没关闭返回 None）"""
    cur.execute("UPDATE incident_alerts SET resolved_at = %s WHERE alert_id = %s AND resolved_at IS NULL",
                (resolved_at.isoformat(), alert_id))
    r = db.one(cur, """
        SELECT i.incident_id, i.status,
               COUNT(*) FILTER (WHERE ia.resolved_at IS NULL) AS open_alerts, MAX(ia.resolved_at) AS last_resolved
        FROM incident_alerts ia JOIN incidents i USING (incident_id)
        WHERE ia.incident_id = (SELECT incident_id FROM incident_alerts WHERE alert_id = %s)
        GROUP BY i.incident_id, i.status""", (alert_id,))
    if r and r["status"] == "open" and int(r["open_alerts"]) == 0:
        cur.execute("UPDATE incidents SET status = 'resolved', resolved_at = %s WHERE incident_id = %s",
                    (_ts(r["last_resolved"]).isoformat(), r["incident_id"]))
        log(SERVICE, "INFO", "incident_resolved", incident_id=r["incident_id"])
        return r["incident_id"]
    return None


def sync(cur, now=None):
    """和 alerts 表对一遍账（安全网）：Kafka 事件漏了、copilot 重启期间的告警、tw resolve 手动关的告警都能补上
    返回 (新收进的告警数, 刚关闭的事故号列表)"""
    now = now or datetime.now(timezone.utc)
    new = db.rows(cur, """
        SELECT a.alert_id, a.rule, a.key, a.severity, a.pm_id, a.detail, a.fired_at, a.resolved_at FROM alerts a
        WHERE NOT EXISTS (SELECT 1 FROM incident_alerts ia WHERE ia.alert_id = a.alert_id)
          AND (a.resolved_at IS NULL OR a.fired_at > %s::timestamptz - interval '30 minutes')
        ORDER BY a.fired_at, a.alert_id""", (now.isoformat(),))
    for a in new:
        ingest(cur, a, now)
    closed = []
    for r in db.rows(cur, """
        SELECT ia.alert_id, a.resolved_at FROM incident_alerts ia JOIN alerts a USING (alert_id)
        WHERE ia.resolved_at IS NULL AND a.resolved_at IS NOT NULL"""):
        c = mark_resolved(cur, r["alert_id"], _ts(r["resolved_at"]))
        if c:
            closed.append(c)
    return len(new), closed


def alerts_of(cur, incident_id):
    return db.rows(cur, "SELECT * FROM incident_alerts WHERE incident_id = %s ORDER BY fired_at, alert_id", (incident_id,))
