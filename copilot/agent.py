"""AI 诊断：假设驱动的 agent 循环 + 复核（复核只能降低可信度，不能提高）"""

import json

from common import log
from copilot import baseline, collect, config, db, llm, tools

SERVICE = "copilot"

REPORT_TOOL = {
    "name": "submit_report",
    "description": "提交调查结论。调查结束时必须调用一次。所有文字用简体中文，专有名词（PM、FIX、topic 名、表名）保留英文",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {"type": "string", "description": "一句话结论（给手机通知看，40 字以内）"},
            "category": {"type": "string", "enum": list(baseline.CATEGORIES), "description": "最可能的根本原因类别"},
            "root_cause": {"type": "string", "description": "根本原因，具体到哪个部件、从什么时候开始、为什么"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1, "description": "对 category 的把握，0~1"},
            "hypotheses": {
                "type": "array", "description": "考虑过的 2~3 个假设（包括被排除的），按可信度从高到低",
                "items": {"type": "object", "properties": {
                    "category": {"type": "string", "enum": list(baseline.CATEGORIES)},
                    "statement": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "evidence_for": {"type": "array", "items": {"type": "string"}},
                    "evidence_against": {"type": "array", "items": {"type": "string"}}},
                    "required": ["category", "statement", "confidence"]}},
            "impact": {"type": "string", "description": "影响范围：哪些 PM / 股票 / 券商 / 多少单，有没有可能的损失或持仓错误"},
            "evidence": {"type": "array", "items": {"type": "string"}, "description": "关键证据，每条引用具体的数字、时间、单号"},
            "symptoms_not_causes": {"type": "array", "items": {"type": "string"}, "description": "哪些告警只是结果、不是原因"},
            "next_steps": {"type": "array", "items": {"type": "string"}, "description": "值班的人下一步该做什么，按顺序"},
            "ask_humans": {"type": "array", "items": {"type": "string"}, "description": "系统数据回答不了、需要去问人的问题（问 PM / 开发 / 券商）"},
        },
        "required": ["summary", "category", "root_cause", "confidence", "hypotheses", "impact", "evidence", "next_steps"],
    },
}

VERDICT_TOOL = {
    "name": "submit_verdict",
    "description": "提交复核结果",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["agree", "partially_agree", "disagree"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1, "description": "你认为结论成立的可信度"},
            "issues": {"type": "array", "items": {"type": "string"}, "description": "结论的漏洞：没有证据支持的说法、和证据矛盾的地方、没排除的其他解释"},
            "alternative_category": {"type": "string", "enum": list(baseline.CATEGORIES) + [""]},
        },
        "required": ["verdict", "confidence", "issues"],
    },
}


def system_prompt():
    qs = []
    for name, q in db.queries().items():
        params = ", ".join(f"{k}:{v.get('type', 'text')}" + (f"={v['default']}" if "default" in v else "")
                           for k, v in q["params"].items())
        qs.append(f"- {name}({params})：{q['description']}")
    cats = "\n".join(f"- {k}：{v}" for k, v in baseline.CATEGORIES.items())
    return f"""你是一家多 PM 对冲基金（模拟环境 TradeWatch）的 trade support 值班助手，负责调查告警的根本原因。
你的结论会发给值班的人，他们据此决定是否暂停 PM、撤单、联系券商。错误但自信的结论比"不确定"更危险。

## 系统
交易链路：market-data（行情）→ oms（策略 + 交易前风控，C#）→ ems（TWAP 执行算法，C#）→ fix-gateway（FIX 网关）→ 券商 NBRG（SNDK、MU）/ HRBR（MSFT、GOOGL）。
服务之间只通过 Kafka 传消息；order-store 把所有事件写进 PostgreSQL；monitor 每 30 秒跑告警规则。
3 个 PM：PM_ALPHA 动量、PM_BETA 均值回归、PM_GAMMA MU/SNDK 配对。风控限额：单个母单最多 75 万美元，另有单股、总敞口、净敞口上限和借券（卖空）额度。
P1 的 order_size_anomaly / duplicate_parent 会自动暂停 PM（kill switch）。

## 调查方法（像有经验的值班员一样）
1. 先读证据包：告警、按剧本自动收集的查询结果和日志、拓扑、runbook。
2. 区分症状和原因：一个原因经常同时触发好几条告警。例如 order-store 卡住时，数据库停在旧数据上，行情和心跳会"看起来"同时过期，但服务本身可能是好的；券商断线会导致子单没确认、母单不动。
3. 提出 2~3 个候选假设，再用工具去找能区分它们的证据。优先找能排除某个假设的证据。
4. 每个结论都要有具体证据（数字、时间、单号、日志行）。证据不足就降低 confidence，或者选 unknown，不要编。
5. 工具预算最多 {config.MAX_STEPS} 次，证据足够就提前结束。最后必须调用 submit_report。
6. 只有在明确有继续扩大损失的风险时，才用 propose_action 建议操作（人会决定是否批准）。已经被暂停的 PM 不要再建议暂停。永远不能建议恢复 PM。

## 注意
- 所有查询都以 as_of（调查时间点）为准往前看；回放过去的事故时，pm_state、fix_sessions、heartbeats、locates 是当前值，只能参考。
- 数据库时间带时区；日志时间是本地时间。
- 母单金额 = 数量 × 价格：异常大的母单要分清是数量变大还是价格跳变。
- 策略程序的日志在 oms 里，service 字段是 strategy-<PM>（例如重启、发版记录）。
- 风控拒单（RISK_REJECT）说明风控在正常工作；NO_LOCATE 一般是借券额度用完。
- 对账差异 MISSING_IN_OURS = 券商有这笔成交、我们没有记到。

## 原因类别（category 必须从这里选）
{cats}

## 命名查询库（run_query 只能用这些；参数写在 params 里）
{chr(10).join(qs)}

所有输出文字用简体中文。"""


def _validate_report(r):
    r = dict(r or {})
    if r.get("category") not in baseline.CATEGORIES:
        r["category"] = "unknown"
    try:
        r["confidence"] = max(0.0, min(1.0, float(r.get("confidence", 0))))
    except (TypeError, ValueError):
        r["confidence"] = 0.0
    for k in ("evidence", "next_steps", "ask_humans", "symptoms_not_causes"):
        v = r.get(k) or []
        r[k] = [str(x)[:400] for x in (v if isinstance(v, list) else [v])][:10]
    r["hypotheses"] = [h for h in (r.get("hypotheses") or []) if isinstance(h, dict)][:4]
    for k in ("summary", "root_cause", "impact"):
        r[k] = str(r.get(k) or "")[:1500]
    return r


def diagnose(evidence, ctx, usage):
    """agent 循环：AI 调用只读工具调查，最后提交结论。返回 (report, transcript)"""
    system = system_prompt()
    tool_specs = tools.specs(("read", "suggest") if config.AUTONOMY == "suggest" else ("read",)) + [REPORT_TOOL]
    intro = ("调查下面这个事故。证据包是按剧本自动收集的，先读完再决定还需要查什么。\n"
             f"调查时间点 as_of = {ctx.as_of.isoformat()}\n\n<evidence>\n"
             + json.dumps(evidence, ensure_ascii=False, default=str) + "\n</evidence>")
    messages = [{"role": "user", "content": intro}]
    transcript = []
    nudged = False
    for _ in range(config.MAX_STEPS + 3):
        force = ctx.steps >= config.MAX_STEPS or nudged
        resp = llm.call(system, messages, tools=tool_specs, usage=usage,
                        tool_choice={"type": "tool", "name": "submit_report"} if force else {"type": "auto"})
        messages.append({"role": "assistant", "content": resp["content"]})
        uses = llm.tool_uses(resp)
        if not uses:
            nudged = True
            messages.append({"role": "user", "content": "请调用 submit_report 提交你的结论。"})
            continue
        results, report = [], None
        for u in uses:
            if u["name"] == "submit_report":
                report = u.get("input") or {}
                results.append({"type": "tool_result", "tool_use_id": u["id"], "content": "已收到"})
                continue
            ok, text = tools.execute(ctx, u["name"], u.get("input") or {})
            transcript.append({"tool": u["name"], "args": u.get("input"), "ok": ok, "result": text[:1500]})
            log(SERVICE, "INFO", "tool_called", investigation_id=ctx.investigation_id, tool=u["name"], ok=ok,
                chars=len(text))
            results.append({"type": "tool_result", "tool_use_id": u["id"], "content": text, "is_error": not ok})
        if report is not None:
            return _validate_report(report), transcript
        messages.append({"role": "user", "content": results})
    raise llm.LLMError("agent did not submit a report")


def verify(evidence, report, transcript, usage):
    """换一个角色专门找结论的漏洞。返回 verdict（confidence 由调用方取较小值）"""
    system = ("你是一个严格的复核员（资深 trade support）。另一个值班助手刚交了一份事故结论。"
              "你的任务是找漏洞：哪些说法没有证据、哪些和证据矛盾、有没有没排除的更简单的解释、是不是把症状当成了原因。"
              "只根据给你的证据判断，不要补充新的事实。用简体中文。最后调用 submit_verdict。")
    small = dict(evidence)
    small = json.loads(json.dumps(small, ensure_ascii=False, default=str))
    collect.shrink(small, limit=12000)
    content = ("<evidence>\n" + json.dumps(small, ensure_ascii=False) + "\n</evidence>\n\n<tool_calls>\n"
               + json.dumps(transcript, ensure_ascii=False)[:8000] + "\n</tool_calls>\n\n<report>\n"
               + json.dumps(report, ensure_ascii=False) + "\n</report>")
    resp = llm.call(system, [{"role": "user", "content": content}], tools=[VERDICT_TOOL], usage=usage,
                    tool_choice={"type": "tool", "name": "submit_verdict"}, max_tokens=1500)
    uses = llm.tool_uses(resp)
    v = (uses[0].get("input") if uses else None) or {}
    try:
        v["confidence"] = max(0.0, min(1.0, float(v.get("confidence", report["confidence"]))))
    except (TypeError, ValueError):
        v["confidence"] = report["confidence"]
    v["issues"] = [str(x)[:300] for x in (v.get("issues") or [])][:6]
    return v


def fallback_report(alerts, evidence, reason):
    """没有 AI 时的报告：规则判断 + 证据摘要"""
    cat, why = baseline.classify(alerts, evidence)
    notes = [s["note"] for s in evidence.get("sections", []) if "note" in s]
    return {
        "summary": f"（规则判断）{baseline.CATEGORIES[cat]}",
        "category": cat,
        "root_cause": f"规则判断：{baseline.CATEGORIES[cat]}（依据：{why}）。这不是 AI 的结论，请人工确认。",
        "confidence": 0.3 if cat != "unknown" else 0.0,
        "hypotheses": [{"category": cat, "statement": why, "confidence": 0.3}],
        "impact": "未分析（没有调用 AI）",
        "evidence": collect.summary_text(evidence).splitlines(),
        "symptoms_not_causes": [],
        "next_steps": notes + [f"按 runbook 排查：docs/runbooks/{a['rule']}.md" for a in alerts[:3]],
        "ask_humans": [],
        "ai_disabled_reason": reason,
    }
