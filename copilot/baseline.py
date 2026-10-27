"""规则判断（不用 AI 的基准）：只看事故里有哪些告警，按经验规则猜原因类别
两个用途：① 没有 API key / 超预算时的兜底 ② 评估时和 AI 比，看 AI 到底有没有比简单规则强"""

# 原因类别：固定的几种，AI 也必须从这里选（这样才能自动评估对不对）
CATEGORIES = {
    "broker_connectivity": "和券商的 FIX 连接断了",
    "broker_unresponsive": "连接是通的，但券商不回应（不确认、不成交）",
    "lost_execution_report": "券商的成交回报丢了，我们少记了成交",
    "symbol_halt_or_broker_reject": "券商拒单：股票停牌或券商侧限制",
    "strategy_bug_or_bad_deploy": "策略程序有 bug / 新版本有问题，在下不正常的单",
    "duplicate_order": "同一张单被重复提交",
    "market_data_feed": "行情源 / 行情服务出了问题，价格停住或延迟",
    "locate_shortage": "借券额度用完，卖空被拒",
    "execution_algo_stalled": "执行算法（EMS）卡住，母单不再发子单",
    "persistence_lag": "写数据库的 order-store 卡住 / 积压，数据库停在旧数据",
    "risk_limits_normal": "正常的风控限额起作用（不是故障）",
    "unknown": "证据不足，无法判断",
}

# 按优先级：前面的规则命中就用前面的
_RULES = [
    (lambda r, a: "kafka_consumer_lag" in r and any(x["key"].startswith("order-store") for x in a.get("kafka_consumer_lag", [])),
     "persistence_lag", "order-store 在 Kafka 上积压"),
    (lambda r, a: "fix_session_down" in r, "broker_connectivity", "有 fix_session_down 告警"),
    (lambda r, a: "duplicate_parent" in r, "duplicate_order", "有 duplicate_parent 告警"),
    (lambda r, a: "order_size_anomaly" in r, "strategy_bug_or_bad_deploy", "有 order_size_anomaly 告警"),
    (lambda r, a: "broker_rejects" in r, "symbol_halt_or_broker_reject", "有 broker_rejects 告警"),
    (lambda r, a: "child_no_ack" in r, "broker_unresponsive", "子单没有确认，但连接没断"),
    (lambda r, a: "market_data_stale" in r and len(r) >= 1 and "service_heartbeat_stale" in r
     and len(a.get("market_data_stale", [])) >= 4, "persistence_lag", "所有股票行情和心跳同时停住"),
    (lambda r, a: "market_data_stale" in r, "market_data_feed", "有 market_data_stale 告警"),
    (lambda r, a: "parent_stalled" in r, "execution_algo_stalled", "有 parent_stalled 告警"),
    (lambda r, a: "risk_reject_spike" in r and a.get("_no_locate"), "locate_shortage", "风控拒单原因里有 NO_LOCATE"),
    (lambda r, a: "recon_break" in r, "lost_execution_report", "有 recon_break 告警"),
    (lambda r, a: "service_heartbeat_stale" in r, "persistence_lag", "服务心跳过期"),
    (lambda r, a: "risk_reject_spike" in r, "risk_limits_normal", "风控拒单增多"),
]


def classify(alerts, evidence=None):
    """返回 (类别, 理由)"""
    by_rule = {}
    for a in alerts:
        by_rule.setdefault(a["rule"], []).append(a)
    rules = set(by_rule)
    # 风控拒单的具体原因在证据包里（按次数最多的原因判断）
    if evidence and "risk_reject_spike" in rules:
        for sec in evidence.get("sections", []):
            if sec.get("title", "").startswith("query risk_rejects"):
                counts = {}
                for r in sec.get("rows", []):
                    if r.get("reason") != "PM_PAUSED":
                        counts[r.get("reason")] = counts.get(r.get("reason"), 0) + int(r.get("n") or 0)
                if counts and max(counts, key=counts.get) == "NO_LOCATE":
                    by_rule["_no_locate"] = True
    for test, cat, why in _RULES:
        try:
            if test(rules, by_rule):
                return cat, why
        except Exception:                                        # noqa: BLE001
            continue
    return "unknown", "没有匹配的规则"
