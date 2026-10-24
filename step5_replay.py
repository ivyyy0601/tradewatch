"""
第 5 步：回放历史行情（美股）
------------------------------------------------
目标：理解三件事
  1. 回放 = 按时间顺序，把历史数据一分钟一分钟"重新播放"，下游以为市场正在交易
  2. 可以加速：比如 SPEED=60，真实的 1 分钟只等 1 秒
  3. 数据里的空档就是休市：收盘到第二天开盘、周末，回放时直接跳过

美股正常交易时间：美东 9:30–16:00，没有午休。

行情通过 on_tick() 发出去。现在只是打印，以后换成发到 Kafka 的 md.ticks，
策略、风控、监控都不用改。

先运行 download_data.py 下载数据，再运行：
    python step5_replay.py            # 回放第一个交易日的前 30 分钟，看看效果
    python step5_replay.py --all      # 回放全部数据
"""

import sys
import time

import pandas as pd

CSV_FILE = "data/us_1m.csv"
SPEED = 60            # 加速倍数：60 = 真实 1 分钟只等 1 秒
SESSION_GAP = 5       # 两条数据相隔超过 5 分钟，就当作休市（收盘、过夜、周末）


def on_tick(tick):
    """收到一条行情。现在打印，以后改成：send('md.ticks', tick)"""
    print(f"    {tick['symbol']:<6} {tick['price']:>10.2f}  量 {tick['volume']:>8}")


def replay(df, max_minutes=None):
    last_ts = None
    minutes_played = 0

    # 按分钟分组：同一分钟里，4 只股票的价格一起发出
    for ts, group in df.groupby("ts"):
        if last_ts is not None:
            gap = (ts - last_ts).total_seconds() / 60
            if gap > SESSION_GAP:
                # 数据中断 = 收盘了，不真的等十几个小时，直接跳到下一个开盘
                print(f"\n  🔔 收盘 {last_ts:%m-%d %H:%M}  →  下一次开盘 {ts:%m-%d %H:%M}（跳过 {gap / 60:.1f} 小时）")
            else:
                time.sleep(gap * 60 / SPEED)        # 按加速后的速度等待

        print(f"\n[{ts:%m-%d %H:%M} 美东]")
        for row in group.itertuples():
            on_tick({"ts": ts.isoformat(), "symbol": row.symbol,
                     "price": row.close, "volume": int(row.volume)})

        last_ts = ts
        minutes_played += 1
        if max_minutes and minutes_played >= max_minutes:
            print(f"\n（演示只回放前 {max_minutes} 分钟，加 --all 回放全部）")
            break


if __name__ == "__main__":
    df = pd.read_csv(CSV_FILE)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
    print(f"读到 {len(df)} 条行情，{df['symbol'].nunique()} 只股票：{', '.join(sorted(df['symbol'].unique()))}")
    print(f"时间范围：{df['ts'].min():%m-%d %H:%M} 到 {df['ts'].max():%m-%d %H:%M}（美东）")
    print(f"回放速度：{SPEED} 倍（真实 1 分钟 = {60 / SPEED:.0f} 秒）")
    replay(df, max_minutes=None if "--all" in sys.argv else 30)
