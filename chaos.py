"""
意外模拟器：相当于"现实世界里会发生的事"
================================================
⚠️ 练习排查的时候不要看这个文件，也不要看 data/chaos/ 和 data/answers.jsonl，它们就是答案。

作为一个单独的服务运行（tw-chaos），按真实时间随机安排：
一件事结束后，隔一段随机的时间再发生下一件，每件持续随机的几分钟，影响随机的券商 / PM / 股票。
当前正在发生的事写在 data/chaos/current.json，其他服务读它来决定自己要不要"出问题"。
结束后把真实情况写进 data/answers.jsonl。你排查完、写好事故记录后再对照。
"""

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from config import BROKERS, PMS, SYMBOLS

rng = random.SystemRandom()

KINDS = [
    "broker_session_down", "broker_silent", "bad_deploy", "duplicate_parent", "lost_exec_report",
    "symbol_halt", "stale_feed", "locate_shortage", "algo_stall", "store_stall",
]
GAP_MIN = (int(os.getenv("FAULT_GAP_MIN", "20")), int(os.getenv("FAULT_GAP_MAX", "60")))       # 两件事之间隔多少分钟
DURATION_MIN = (int(os.getenv("FAULT_DURATION_MIN", "5")), int(os.getenv("FAULT_DURATION_MAX", "20")))
STATE_FILE = os.getenv("CHAOS_STATE", "data/chaos/current.json")
ANSWERS = os.getenv("CHAOS_ANSWERS", "data/answers.jsonl")


def _now():
    return datetime.now(timezone.utc)


class ChaosClient:
    """其他服务用：现在有没有某种意外正在发生？"""

    def __init__(self, path=STATE_FILE):
        self.path = path
        self._cache, self._read_at = None, 0.0

    def current(self):
        if time.time() - self._read_at > 1:
            try:
                with open(self.path) as fh:
                    self._cache = json.load(fh) or None
            except (OSError, json.JSONDecodeError):
                self._cache = None
            self._read_at = time.time()
        return self._cache

    def active(self, kind, **match):
        f = self.current()
        if f and f["kind"] == kind and all(f.get(k) == v for k, v in match.items()):
            return f
        return None


def _write_state(f):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(f, fh, default=str)
    os.replace(tmp, STATE_FILE)                     # 一次性替换，别的服务不会读到写了一半的文件


def _write_answer(f):
    os.makedirs(os.path.dirname(ANSWERS) or ".", exist_ok=True)
    rec = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in f.items()}
    with open(ANSWERS, "a") as fh:
        fh.write(json.dumps(rec) + "\n")


def main():
    print(json.dumps({"ts": _now().isoformat(), "level": "INFO", "service": "chaos", "msg": "start"}), flush=True)
    current = None
    _write_state(None)
    next_at = _now() + timedelta(minutes=rng.randint(*GAP_MIN))
    while True:
        t = _now()
        if current and t >= current["end"]:
            _write_answer(current)
            current = None
            _write_state(None)
            next_at = t + timedelta(minutes=rng.randint(*GAP_MIN))
        if not current and t >= next_at:
            current = {
                "kind": rng.choice(KINDS), "start": t, "end": t + timedelta(minutes=rng.randint(*DURATION_MIN)),
                "broker": rng.choice(list(BROKERS)), "pm": rng.choice(list(PMS)), "symbol": rng.choice(SYMBOLS),
            }
            _write_state(current)
        time.sleep(5)


if __name__ == "__main__":
    main()
