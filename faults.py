"""
意外模拟器：相当于"现实世界里会发生的事"
================================================
⚠️ 练习排查的时候不要看这个文件，它就是答案。

按真实时间随机安排：一件事结束后，隔一段随机的时间再发生下一件，
每件持续随机的几分钟，影响随机的券商 / PM / 股票。
结束后把真实情况写进 data/answers.jsonl。你排查完、写好事故记录后再对照。
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

from config import BROKERS, PMS, SYMBOLS

rng = random.SystemRandom()

KINDS = [
    "broker_session_down", "broker_silent", "bad_deploy", "duplicate_parent", "lost_exec_report",
    "symbol_halt", "stale_feed", "locate_shortage", "algo_stall",
]
GAP_MIN = (int(os.getenv("FAULT_GAP_MIN", "20")), int(os.getenv("FAULT_GAP_MAX", "60")))   # 两件事之间隔多少分钟
DURATION_MIN = (5, 20)                                                                     # 每件事持续多少分钟


def _now():
    return datetime.now(timezone.utc)


class Faults:
    def __init__(self, answers_path="data/answers.jsonl"):
        self.path = answers_path
        self.current = None
        self.next_at = _now() + timedelta(minutes=rng.randint(*GAP_MIN))

    def tick(self):
        """每根 K 线调用一次：到时间就开始或结束一件事。返回需要打的"系统日志"。"""
        now, logs = _now(), []
        if self.current and now >= self.current["end"]:
            logs += self._on_end(self.current)
            self._write(self.current)
            self.current = None
            self.next_at = now + timedelta(minutes=rng.randint(*GAP_MIN))
        if not self.current and now >= self.next_at:
            kind = rng.choice(KINDS)
            self.current = {
                "kind": kind, "start": now, "end": now + timedelta(minutes=rng.randint(*DURATION_MIN)),
                "broker": rng.choice(list(BROKERS)), "pm": rng.choice(list(PMS)), "symbol": rng.choice(SYMBOLS),
            }
            logs += self._on_start(self.current)
        return logs

    def active(self, kind, **match):
        f = self.current
        if f and f["kind"] == kind and all(f[k] == v for k, v in match.items()):
            return f
        return None

    # ---- 有些事真实系统会留下日志线索，有些完全没有 ----
    def _on_start(self, f):
        if f["kind"] == "broker_session_down":
            return [("fix-gateway", "ERROR", "fix_session_lost",
                     {"broker": f["broker"], "detail": "heartbeat timeout, TCP connection reset"})]
        if f["kind"] == "bad_deploy":
            return [(f"strategy-{f['pm']}", "INFO", "strategy_restarted",
                     {"pm_id": f["pm"], "version": "3.7.0", "detail": "deployed by release pipeline"})]
        return []

    def _on_end(self, f):
        if f["kind"] == "broker_session_down":
            return [("fix-gateway", "INFO", "fix_session_restored", {"broker": f["broker"]})]
        return []

    def _write(self, f):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        rec = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in f.items()}
        with open(self.path, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
