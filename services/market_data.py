"""
行情服务（tw-market-data）
================================================
回放真实的美股分钟数据（data/us_1m.csv），打上"现在"的真实时间，每 1 分钟发一根 K 线：
  - md.exchange：真实市场价格（只给模拟券商用，代表"交易所里真正的价格"）
  - md.bars：我们的系统收到的行情（策略、风控、EMS 用；行情源出问题时这里的价格会停住）
每个"交易日"开始发 sod（start of day），结束发 eod（end of day）。历史数据放完就从头再放。
"""

import os
import time

import pandas as pd

from chaos import ChaosClient
from common import Bus, ensure_topics, heartbeat, log, now

SERVICE = "market-data"
CSV_FILE = os.getenv("CSV_FILE", "data/us_1m.csv")
SPEED = float(os.getenv("SPEED", "1"))           # 1 = 真实速度；10 = 快 10 倍（练习用）


def main():
    ensure_topics(SERVICE)
    bus = Bus(SERVICE)
    chaos = ChaosClient()
    df = pd.read_csv(CSV_FILE)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
    days = sorted(df["ts"].dt.date.unique())
    log(SERVICE, "INFO", "start", file=CSV_FILE, days=len(days), speed=SPEED)
    bar, published = 0, {}
    while True:
        for day in days:
            trade_date = now().strftime("%Y%m%d")
            for topic in ("md.exchange", "md.bars"):
                bus.send(topic, "session", {"type": "sod", "trade_date": trade_date, "ts": now()})
            log(SERVICE, "INFO", "start_of_day", trade_date=trade_date, replay_day=str(day))
            for _, group in df[df["ts"].dt.date == day].groupby("ts"):
                t0 = time.time()
                stale = chaos.active("stale_feed")
                true_px, volumes, updated = {}, {}, []
                for row in group.itertuples():
                    true_px[row.symbol] = round(float(row.close), 4)
                    volumes[row.symbol] = int(row.volume)
                    if stale and stale["symbol"] == row.symbol and row.symbol in published:
                        continue                                  # 我们这边的行情没更新（行情源卡住了）
                    published[row.symbol] = true_px[row.symbol]
                    updated.append(row.symbol)
                ts = now()
                bus.send("md.exchange", "bar", {"type": "bar", "bar": bar, "ts": ts, "prices": true_px})
                bus.send("md.bars", "bar", {"type": "bar", "bar": bar, "ts": ts, "prices": dict(published),
                                            "updated": updated, "volumes": volumes})
                heartbeat(bus, SERVICE, bar=bar)
                bus.flush()
                bar += 1
                time.sleep(max(0.0, 60 / SPEED - (time.time() - t0)))
            trade_date = now().strftime("%Y%m%d")
            for topic in ("md.exchange", "md.bars"):
                bus.send(topic, "session", {"type": "eod", "trade_date": trade_date, "ts": now()})
            bus.flush()
            log(SERVICE, "INFO", "end_of_day", trade_date=trade_date)


if __name__ == "__main__":
    main()
