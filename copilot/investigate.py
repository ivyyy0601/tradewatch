"""一次完整的调查：取证 → 诊断（AI 或规则）→ 复核 → 记录 → 建议 → 通知"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

from common import log
from copilot import agent, approvals, baseline, collect, config, correlate, db, llm, notify, report, tools

SERVICE = "copilot"


def spent_today(cur):
    r = db.one(cur, "SELECT COALESCE(SUM(cost_usd), 0) AS c FROM investigations WHERE started_at > date_trunc('day', now())")
    return float(r["c"]) if r else 0.0


def run(rw, ro, incident_id, trigger="auto", as_of=None, notify_user=True, use_ai=True):
    """返回这次调查的记录（dict）"""
    t0 = time.time()
    incident = db.one(rw, "SELECT * FROM incidents WHERE incident_id = %s", (incident_id,))
    if not incident:
        raise ValueError(f"no incident {incident_id}")
    alerts = correlate.alerts_of(rw, incident_id)
    as_of = as_of or datetime.now(timezone.utc)
    evidence = collect.gather(ro, incident, alerts, as_of)
    base_cat, base_why = baseline.classify(alerts, evidence)

    reason = ""
    if not use_ai:
        reason = "disabled for this run"
    elif not llm.enabled():
        reason = "没有配置 ANTHROPIC_API_KEY"
    elif spent_today(rw) >= config.DAILY_BUDGET_USD:
        reason = f"今天的 AI 预算 ${config.DAILY_BUDGET_USD} 已用完"
    mode = "baseline" if reason else "ai"
    inv = db.one(rw, """
        INSERT INTO investigations (incident_id, mode, trigger, as_of, started_at, model, evidence)
        VALUES (%s, %s, %s, %s, now(), %s, %s) RETURNING investigation_id""",
        (incident_id, mode, trigger, as_of.isoformat(), config.MODEL if mode == "ai" else None, db.as_json(evidence)))
    inv_id = inv["investigation_id"]
    log(SERVICE, "INFO", "investigation_started", investigation_id=inv_id, incident_id=incident_id, mode=mode,
        trigger=trigger, as_of=as_of.isoformat())

    ctx = tools.Ctx(cur_ro=ro, cur_rw=rw, as_of=as_of, investigation_id=inv_id)
    usage = llm.Usage()
    error = None
    raw_conf = None
    if mode == "ai":
        try:
            rep, transcript = agent.diagnose(evidence, ctx, usage)
            raw_conf = rep["confidence"]
            try:
                v = agent.verify(evidence, rep, transcript, usage)
                rep["verification"] = v
                rep["confidence"] = min(rep["confidence"], v["confidence"])    # 复核只能降低可信度
            except llm.LLMError as e:
                rep["verification"] = {"verdict": "skipped", "issues": [f"复核失败：{str(e)[:150]}"]}
        except llm.LLMError as e:
            error = str(e)[:500]
            log(SERVICE, "ERROR", "ai_failed", investigation_id=inv_id, error=error)
            mode = "baseline"
            rep = agent.fallback_report(alerts, evidence, f"AI 调用失败：{error[:200]}")
    else:
        rep = agent.fallback_report(alerts, evidence, reason)
    rep["baseline_category"] = base_cat
    rep["baseline_reason"] = base_why

    seconds = time.time() - t0
    rw.execute("""
        UPDATE investigations SET mode = %s, finished_at = now(), category = %s, root_cause = %s, confidence = %s,
               confidence_raw = %s, report = %s, input_tokens = %s, output_tokens = %s, cost_usd = %s, error = %s
        WHERE investigation_id = %s""",
        (mode, rep["category"], rep.get("root_cause"), rep["confidence"], raw_conf, db.as_json(rep),
         usage.input_tokens, usage.output_tokens, usage.cost_usd, error, inv_id))
    rw.execute("UPDATE incidents SET investigated_at = now() WHERE incident_id = %s", (incident_id,))

    proposals = ctx.proposals if (trigger != "replay" and config.AUTONOMY == "suggest") else []
    ids = approvals.create(rw, incident_id, inv_id, proposals) if proposals else []
    result = {"investigation_id": inv_id, "incident_id": incident_id, "mode": mode, "model": config.MODEL if mode == "ai" else None,
              "category": rep["category"], "confidence": rep["confidence"], "confidence_raw": raw_conf, "report": rep,
              "steps": ctx.steps, "cost_usd": usage.cost_usd, "seconds": seconds, "error": error}
    log(SERVICE, "INFO", "investigation_done", investigation_id=inv_id, incident_id=incident_id, mode=mode,
        category=rep["category"], confidence=rep["confidence"], baseline=base_cat, steps=ctx.steps,
        cost_usd=usage.cost_usd, seconds=round(seconds, 1))
    if notify_user and trigger != "replay":
        notify.send(report.telegram(incident, alerts, result, proposals))
        approvals.notify_new(rw, ids)
    return result


def due(rw):
    """到时间该调查的事故（等了 debounce 秒，相关告警已经归进来了）"""
    return db.rows(rw, """
        SELECT incident_id FROM incidents
        WHERE source = 'live' AND investigate_after <= now()
          AND (investigated_at IS NULL OR investigated_at < investigate_after)
          AND opened_at > now() - make_interval(mins => %s)
        ORDER BY investigate_after LIMIT 5""", (config.MAX_AGE_MIN,))


def write_draft(rw, incident_id):
    """事故关闭后起草事故记录，写到 docs/incidents/drafts/"""
    incident = db.one(rw, "SELECT * FROM incidents WHERE incident_id = %s", (incident_id,))
    if not incident or incident["source"] != "live":
        return None
    alerts = correlate.alerts_of(rw, incident_id)
    inv = db.one(rw, "SELECT * FROM investigations WHERE incident_id = %s ORDER BY investigation_id DESC LIMIT 1",
                 (incident_id,))
    if inv and isinstance(inv.get("report"), str):
        inv["report"] = json.loads(inv["report"])
    steps = db.rows(rw, "SELECT step_no, tool, args FROM investigation_steps WHERE investigation_id = %s ORDER BY step_no",
                    (inv["investigation_id"],)) if inv else []
    props = db.rows(rw, "SELECT * FROM action_proposals WHERE incident_id = %s ORDER BY proposal_id", (incident_id,))
    folder = os.path.join(config.DOCS_DIR, "incidents", "drafts")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"INC-{incident_id}.md")
    with open(path, "w") as fh:
        fh.write(report.markdown(incident, alerts, inv, steps, props))
    log(SERVICE, "INFO", "incident_draft_written", incident_id=incident_id, file=path)
    return path
