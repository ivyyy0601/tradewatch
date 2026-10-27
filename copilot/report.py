"""把调查结果排成给人看的文字：Telegram 消息、./tw incident、事故记录草稿"""

from copilot import baseline

SEV_ICON = {"P1": "🔴", "P2": "🟡", "P3": "⚪"}


def _conf(v):
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return "-"


def telegram(incident, alerts, inv, proposals=()):
    r = inv.get("report") or {}
    cat = r.get("category", "unknown")
    head = f"🔎 INC-{incident['incident_id']} {SEV_ICON.get(incident['severity'], '')}{incident['severity']} {incident['title']}"
    if len(alerts) > 1:
        head += f"（共 {len(alerts)} 条相关告警）"
    lines = [head, "", f"结论：{r.get('summary', '')}",
             f"类别：{cat}（{baseline.CATEGORIES.get(cat, '')}）",
             f"可信度：{_conf(inv.get('confidence'))}"
             + (f"（复核前 {_conf(inv.get('confidence_raw'))}）" if inv.get("confidence_raw") not in (None, inv.get("confidence")) else "")]
    if r.get("root_cause"):
        lines += ["", "原因：" + r["root_cause"]]
    if r.get("impact"):
        lines += ["影响：" + r["impact"]]
    if r.get("evidence"):
        lines += ["", "证据："] + [f"• {e}" for e in r["evidence"][:5]]
    if r.get("symptoms_not_causes"):
        lines += ["", "只是结果、不是原因："] + [f"• {e}" for e in r["symptoms_not_causes"][:3]]
    if r.get("next_steps"):
        lines += ["", "下一步："] + [f"{i}. {s}" for i, s in enumerate(r["next_steps"][:5], 1)]
    if r.get("ask_humans"):
        lines += ["", "需要问人："] + [f"• {s}" for s in r["ask_humans"][:3]]
    v = r.get("verification")
    if v and v.get("issues"):
        lines += ["", f"复核（{v.get('verdict', '')}）："] + [f"• {s}" for s in v["issues"][:3]]
    if proposals:
        lines += ["", "AI 建议的操作（需要你批准，见下一条消息）："] + [f"• {p['action']} {p['target']}" for p in proposals]
    meta = f"{inv.get('mode')} · {inv.get('model') or '-'} · 工具 {inv.get('steps', 0)} 次 · ${float(inv.get('cost_usd') or 0):.3f}"
    if inv.get("seconds") is not None:
        meta += f" · {inv['seconds']:.0f} 秒"
    lines += ["", meta, f"规则判断：{r.get('baseline_category', '-')}", f"详情：./tw incident {incident['incident_id']}"]
    return "\n".join(lines)


def markdown(incident, alerts, inv, steps=(), proposals=()):
    """事故记录草稿（docs/incidents/drafts/INC-<id>.md）"""
    r = (inv or {}).get("report") or {}
    out = [f"# INC-{incident['incident_id']}：{incident['title']}", "",
           "> AI 起草的事故记录，**需要值班的人审核修改**后再移到 docs/incidents/", "",
           "| 项目 | 内容 |", "|---|---|",
           f"| 级别 | {incident['severity']} |",
           f"| 开始 | {incident['opened_at']} |",
           f"| 结束 | {incident.get('resolved_at') or '未结束'} |",
           f"| 原因类别 | {r.get('category', '-')}（{baseline.CATEGORIES.get(r.get('category'), '')}） |",
           f"| AI 可信度 | {_conf((inv or {}).get('confidence'))} |", "",
           "## 现象（告警）", ""]
    out += [f"- {a['fired_at']} [{a['severity']}] {a['rule']} {a['key']}：{a.get('detail') or ''}"
            + (f"（{a['resolved_at']} 解决）" if a.get("resolved_at") else "") for a in alerts]
    out += ["", "## 影响", "", r.get("impact") or "（待补充）", "",
            "## 原因", "", r.get("root_cause") or "（待补充）", "", "## 证据", ""]
    out += [f"- {e}" for e in r.get("evidence", [])] or ["（待补充）"]
    if r.get("hypotheses"):
        out += ["", "## 考虑过的假设", ""]
        out += [f"- {h.get('category')}（{_conf(h.get('confidence'))}）：{h.get('statement')}" for h in r["hypotheses"]]
    out += ["", "## 处理", ""]
    out += [f"- 建议 #{p['proposal_id']} {p['action']} {p['target']}：{p['status']}"
            + (f"（{p['decided_by']}）" if p.get("decided_by") else "") for p in proposals] or ["（待补充：你做了什么、谁批准的）"]
    out += ["", "## 后续", ""] + [f"- [ ] {s}" for s in r.get("next_steps", [])] + ["- [ ] 要不要改告警规则 / runbook？"]
    if steps:
        out += ["", "## AI 调查过程", ""] + [f"{s['step_no']}. {s['tool']} {s.get('args') or ''}" for s in steps]
    return "\n".join(out) + "\n"
