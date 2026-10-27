"""系统拓扑：告警落在哪些部件上、两条告警是不是相关、某个部件对应哪个容器"""

import os

import yaml

from copilot import config

_T = None


def topo():
    global _T
    if _T is None:
        with open(os.path.join(config.HERE, "topology.yaml")) as fh:
            _T = yaml.safe_load(fh)
    return _T


def key_parts(key):
    parts = (key or "").split(":")
    return parts[0], (parts[1] if len(parts) > 1 else "")


def context(alert):
    """一条告警里能用来填剧本 / 拓扑模板的值"""
    key = str(alert.get("key") or "")
    k0, k1 = key_parts(key)
    rule = alert.get("rule", "")
    ctx = {"rule": rule, "key": key, "key0": k0, "key1": k1, "pm_id": alert.get("pm_id") or "",
           "broker": "", "service": "", "symbol": ""}
    t = topo()
    if rule in ("fix_session_down", "child_no_ack"):
        ctx["broker"] = key
    elif rule == "broker_rejects":
        ctx["broker"] = k0
    elif rule == "recon_break":
        ctx["broker"] = k1
    elif rule == "market_data_stale":
        ctx["symbol"] = key
        ctx["broker"] = t["routing"].get(key, "")
    elif rule == "service_heartbeat_stale":
        ctx["service"] = key
    elif rule == "kafka_consumer_lag":
        ctx["service"] = t["consumer_groups"].get(k0, k0)
        if ctx["service"].startswith("broker:"):
            ctx["broker"] = ctx["service"].split(":", 1)[1]
    return ctx


def components(alert):
    """这条告警落在哪些拓扑节点上（归并用）"""
    ctx = context(alert)
    out = set()
    for tpl in topo()["rule_components"].get(ctx["rule"], []):
        c = tpl.format(**ctx)
        if c.startswith("group:"):
            c = topo()["consumer_groups"].get(c[6:], c[6:])
        if c.startswith("symbol:"):
            broker = topo()["routing"].get(c[7:])
            if broker:
                out.add(f"broker:{broker}")
            continue
        if c and not c.endswith(":"):
            out.add(c)
    return out


def neighbours(comp):
    c = topo()["components"].get(comp, {})
    return set(c.get("upstream", [])) | set(c.get("downstream", []))


def related(a_components, b_components):
    """两组部件是同一个、或者直接上下游 → 认为可能是同一件事"""
    a, b = set(a_components), set(b_components)
    if a & b:
        return True
    near_a = set().union(*[neighbours(x) for x in a]) if a else set()
    near_b = set().union(*[neighbours(x) for x in b]) if b else set()
    return bool(near_a & b or near_b & a)


def container(comp):
    c = topo()["components"].get(comp)
    if c and c.get("container"):
        return c["container"]
    return "tw-" + comp


def describe(comps):
    """给 AI 看的拓扑摘要：涉及的部件和它们的上下游"""
    t = topo()["components"]
    out = {}
    for c in sorted(comps):
        if c in t:
            node = t[c]
            out[c] = {k: node[k] for k in ("what", "upstream", "downstream", "symbols") if k in node}
            for n in node.get("upstream", []) + node.get("downstream", []):
                if n in t and n not in out:
                    out[n] = {"what": t[n].get("what", "")}
    return out
