"""
故障模拟器：相当于"现实世界里会发生的意外"
================================================
⚠️ 练习排查的时候不要看这个文件，它就是答案。

每个交易日开盘时随机决定：今天出不出事、出什么事、几点开始、持续多久、影响谁。
事情结束后，把真实发生的情况写进 data/answers.jsonl。
你自己排查完、写好事故记录之后，再打开那个文件对照。
"""

import json
import os
import random
from datetime import datetime, timedelta

rng = random.SystemRandom()          # 用系统随机数，连 seed 都猜不到

KINDS = ["broker_outage", "slow_broker", "bad_deploy", "duplicate_send",
         "lost_fill_report", "symbol_halt", "stale_feed"]
STRATEGIES = ["momo", "meanrev"]
SYMBOLS = ["SNDK", "MU", "MSFT", "GOOGL"]


class Faults:
    def __init__(self, answers_path="data/answers.jsonl"):
        self.path = answers_path
        self.today = None
        self.logged_start = False

    def new_day(self, date):
        self._finish()
        self.today, self.logged_start = None, False
        if rng.random() < 0.2:                       # 大约 1/5 的日子平安无事
            return
        kind = rng.choice(KINDS)
        start = datetime(2000, 1, 1, 9, 30) + timedelta(minutes=rng.randint(15, 330))
        minutes = 390 if kind == "bad_deploy" else rng.randint(10, 40)   # 坏版本一直到收盘才发现
        end = min(start + timedelta(minutes=minutes), datetime(2000, 1, 1, 16, 0))
        self.today = {"date": str(date), "kind": kind, "start": f"{start:%H:%M}", "end": f"{end:%H:%M}",
                      "strategy": rng.choice(STRATEGIES), "symbol": rng.choice(SYMBOLS)}

    def active(self, kind, ts):
        f = self.today
        if f and f["kind"] == kind and f["start"] <= ts.strftime("%H:%M") < f["end"]:
            return f
        return None

    def system_logs(self, ts):
        """有些意外，真实系统会留下日志线索（比如连接断开），有些完全没有。"""
        f, hm = self.today, ts.strftime("%H:%M")
        if not f:
            return []
        out = []
        if hm == f["start"] and not self.logged_start:
            self.logged_start = True
            if f["kind"] == "broker_outage":
                out.append(("broker-gateway", "ERROR", "fix_heartbeat_timeout",
                            {"session": "BROKER-1", "detail": "no heartbeat for 30s, reconnecting"}))
            if f["kind"] == "bad_deploy":
                out.append((f"strategy-{f['strategy']}", "INFO", "strategy_restarted",
                            {"version": "2.4.0", "detail": "deploy by release pipeline"}))
        if hm == f["end"] and f["kind"] == "broker_outage":
            out.append(("broker-gateway", "INFO", "fix_session_restored", {"session": "BROKER-1"}))
        if hm >= f["end"]:
            self._finish()
        return out

    def _finish(self):
        """意外结束了，把真实情况写进答案文件。"""
        if self.today and not self.today.get("written"):
            self.today["written"] = True
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "a") as fh:
                fh.write(json.dumps({k: v for k, v in self.today.items() if k != "written"}) + "\n")
