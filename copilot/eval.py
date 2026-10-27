"""
评估：AI 值班助手到底准不准？
================================================
chaos 服务会把每次真实发生的故障写进 data/answers.jsonl。拿它当标准答案，看：
  - 检测率：故障期间有没有出现事故（告警）
  - AI 准确率：AI 判断的原因类别对不对
  - 规则准确率：只按告警类型猜（baseline），作为对照
  - 速度、费用

用法（在服务器上，通过 ./tw）：
  ./tw ai-eval                      对照答案打分（不调用 AI，不花钱）
  ./tw ai-backfill 48               用过去 48 小时的历史告警补建事故（copilot 上线之前的告警）
  ./tw ai-replay 48 [条数] [--force] 对过去 48 小时没被 AI 调查过的事故，用当时的数据重新调查（会调用 AI，花钱）
⚠️ ai-eval 会显示真实答案。练习排查时，先自己写完事故记录再跑
"""

import json
import sys
from datetime import datetime, timedelta, timezone

from copilot import baseline, config, correlate, db, investigate, topology

# chaos 的故障种类 → 应该判断出的原因类别
EXPECTED = {
    "broker_session_down": "broker_connectivity",
    "broker_silent": "broker_unresponsive",
    "bad_deploy": "strategy_bug_or_bad_deploy",
    "duplicate_parent": "duplicate_order",
    "lost_exec_report": "lost_execution_report",
    "symbol_halt": "symbol_halt_or_broker_reject",
    "stale_feed": "market_data_feed",
    "locate_shortage": "locate_shortage",
    "algo_stall": "execution_algo_stalled",
    "store_stall": "persistence_lag",
}


def relevant(fault, components):
    """这个事故和这次故障有没有关系（故障影响的是哪个 PM / 券商 / 股票）"""
    comps = set(correlate._arr(components))
    if not comps:
        return True
    k = fault["kind"]
    if k in ("duplicate_parent", "bad_deploy", "algo_stall"):
        return f"pm:{fault.get('pm')}" in comps
    if k in ("broker_session_down", "broker_silent", "lost_exec_report"):
        b = fault.get("broker")
        return f"broker:{b}" in comps or any(c.startswith("recon:") and c.endswith(f":{b}") for c in comps)
    if k == "symbol_halt":
        return f"broker:{topology.topo()['routing'].get(fault.get('symbol'), '')}" in comps
    if k == "stale_feed":
        return "market-data" in comps
    if k == "locate_shortage":
        return any(c.startswith("pm:") for c in comps)
    return True                                                  # store_stall 等：影响全系统


def load_answers(path=None):
    out = []
    try:
        with open(path or config.ANSWERS) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    a = json.loads(line)
                    a["start"] = datetime.fromisoformat(a["start"])
                    a["end"] = datetime.fromisoformat(a["end"])
                    out.append(a)
                except (ValueError, KeyError, TypeError):
                    continue
    except OSError:
        pass
    return out


def backfill(rw, hours):
    """用历史告警补建事故（按告警发生的时间顺序归并）"""
    rows = db.rows(rw, """
        SELECT a.alert_id, a.rule, a.key, a.severity, a.pm_id, a.detail, a.fired_at, a.resolved_at FROM alerts a
        WHERE a.fired_at > now() - make_interval(hours => %s)
          AND NOT EXISTS (SELECT 1 FROM incident_alerts ia WHERE ia.alert_id = a.alert_id)
        ORDER BY a.fired_at, a.alert_id""", (int(hours),))
    for a in rows:
        correlate.ingest(rw, a, now=correlate._ts(a["fired_at"]), source="backfill")
    n = db.one(rw, "SELECT COUNT(*) AS n FROM incidents WHERE source = 'backfill'")["n"]
    print(f"补建了 {len(rows)} 条告警；现在一共有 {n} 个补建的事故")


def replay(rw, ro, hours, limit=10, force=False):
    """用事故当时的数据重新调查（as_of = 事故开始后 debounce 秒）"""
    rows = db.rows(rw, """
        SELECT i.incident_id, i.opened_at FROM incidents i
        WHERE i.opened_at > now() - make_interval(hours => %s)
          AND (%s OR NOT EXISTS (SELECT 1 FROM investigations v WHERE v.incident_id = i.incident_id AND v.mode = 'ai'))
        ORDER BY i.opened_at LIMIT %s""", (int(hours), bool(force), int(limit)))
    print(f"要回放 {len(rows)} 个事故")
    for r in rows:
        as_of = correlate._ts(r["opened_at"]) + timedelta(seconds=config.DEBOUNCE_S)
        res = investigate.run(rw, ro, r["incident_id"], trigger="replay", as_of=as_of, notify_user=False)
        print(f"  INC-{r['incident_id']}: {res['mode']} → {res['category']} (conf {res['confidence']:.2f}, "
              f"${res['cost_usd']:.3f}, {res['seconds']:.0f}s)")


def score(rw, write=True):
    answers = load_answers()
    if not answers:
        print(f"没有找到答案文件 {config.ANSWERS}（chaos 每次故障结束后才会写一条）")
        return []
    first = db.one(rw, "SELECT MIN(opened_at) AS t FROM incidents")
    results = []
    for a in answers:
        start, end = a["start"], a["end"]
        if first and first["t"] and end < correlate._ts(first["t"]) - timedelta(hours=1):
            continue                                             # 比最早的事故还早很多，copilot 那时还没在跑
        expected = EXPECTED.get(a["kind"], "unknown")
        incs = db.rows(rw, """
            SELECT i.incident_id, i.opened_at, i.components FROM incidents i
            WHERE i.opened_at BETWEEN %s::timestamptz - interval '1 minute' AND %s::timestamptz + interval '15 minutes'
            ORDER BY i.opened_at""", (start.isoformat(), end.isoformat()))
        incs = [i for i in incs if relevant(a, i["components"])]
        detected = bool(incs)
        ai_pred = ai_ok = base_pred = base_ok = minutes = cost = None
        matched = None
        if incs:
            ai_ok, base_ok = False, False
            for inc in incs:
                alerts = correlate.alerts_of(rw, inc["incident_id"])
                inv = db.one(rw, """SELECT category, finished_at, trigger, cost_usd, evidence FROM investigations
                                    WHERE incident_id = %s AND mode = 'ai' ORDER BY investigation_id DESC LIMIT 1""",
                             (inc["incident_id"],))
                ev = inv["evidence"] if inv else None
                if isinstance(ev, str):
                    ev = json.loads(ev)
                b, _ = baseline.classify(alerts, ev)
                if base_pred is None:
                    base_pred = b
                if b == expected:
                    base_ok, base_pred = True, b
                if inv:
                    if ai_pred is None or inv["category"] == expected:
                        ai_pred, cost = inv["category"], float(inv["cost_usd"] or 0)
                        first_auto = db.one(rw, """SELECT finished_at FROM investigations WHERE incident_id = %s
                                                   AND mode = 'ai' AND trigger = 'auto' AND finished_at IS NOT NULL
                                                   ORDER BY investigation_id LIMIT 1""", (inc["incident_id"],))
                        if first_auto:
                            minutes = round((correlate._ts(first_auto["finished_at"]) - start).total_seconds() / 60, 1)
                    if inv["category"] == expected:
                        ai_ok, matched = True, inc["incident_id"]
            if ai_pred is None:
                ai_ok = None                                     # 这些事故还没被 AI 调查过
        matched = matched or (incs[0]["incident_id"] if incs else None)
        results.append({"kind": a["kind"], "start": start, "end": end, "expected": expected, "incident_id": matched,
                        "detected": detected, "ai_predicted": ai_pred, "ai_correct": ai_ok,
                        "baseline_predicted": base_pred, "baseline_correct": base_ok,
                        "minutes": minutes, "cost": cost})
    if write and results:
        rw.execute("DELETE FROM copilot_eval")
        for r in results:
            rw.execute("""INSERT INTO copilot_eval (fault_kind, fault_start, fault_end, expected, incident_id, detected,
                          ai_predicted, ai_correct, baseline_predicted, baseline_correct, minutes_to_diagnosis, cost_usd)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (r["kind"], r["start"].isoformat(), r["end"].isoformat(), r["expected"], r["incident_id"],
                        r["detected"], r["ai_predicted"], r["ai_correct"], r["baseline_predicted"], r["baseline_correct"],
                        r["minutes"], r["cost"]))
    print_table(results)
    return results


def _pct(n, d):
    return f"{n}/{d} ({100 * n / d:.0f}%)" if d else "-"


def print_table(results):
    print(f"{'故障':22} {'开始':16} {'检测':4} {'应该是':28} {'AI 判断':28} {'规则判断':28} {'用时(分)':8}")
    for r in results:
        mark = lambda ok: "✓" if ok else ("✗" if ok is False else " ")
        print(f"{r['kind']:22} {r['start'].astimezone().strftime('%m-%d %H:%M'):16} {'是' if r['detected'] else '否':4} "
              f"{r['expected']:28} {mark(r['ai_correct'])} {str(r['ai_predicted'] or '-'):26} "
              f"{mark(r['baseline_correct'])} {str(r['baseline_predicted'] or '-'):26} {str(r['minutes'] or '-'):8}")
    det = [r for r in results if r["detected"]]
    ai = [r for r in det if r["ai_correct"] is not None]
    print()
    print(f"检测率      {_pct(len(det), len(results))}   （故障期间有没有出现事故）")
    print(f"AI 准确率   {_pct(sum(1 for r in ai if r['ai_correct']), len(ai))}   （在检测到、并且被 AI 调查过的事故里）")
    print(f"规则准确率  {_pct(sum(1 for r in det if r['baseline_correct']), len(det))}   （只按告警类型猜，对照组）")
    mins = sorted(r["minutes"] for r in ai if r["minutes"] is not None)
    if mins:
        print(f"从故障开始到 AI 给出结论：中位数 {mins[len(mins) // 2]} 分钟")
    costs = [r["cost"] for r in ai if r["cost"] is not None]
    if costs:
        print(f"平均每次调查费用：${sum(costs) / len(costs):.3f}")


def main(argv):
    rw_conn, ro_conn = db.open_rw(), db.open_ro()
    rw, ro = rw_conn.cursor(), ro_conn.cursor()
    db.ensure_schema(rw)
    cmd = argv[0] if argv else "score"
    if cmd == "score":
        score(rw)
    elif cmd == "backfill":
        backfill(rw, int(argv[1]) if len(argv) > 1 else 48)
    elif cmd == "replay":
        args = [a for a in argv[1:] if not a.startswith("--")]
        replay(rw, ro, int(args[0]) if args else 24, int(args[1]) if len(args) > 1 else 10, "--force" in argv)
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
