"""
下载美股的历史分钟数据，存成 CSV，给回放用
------------------------------------------------
数据来源：Yahoo Finance（通过 yfinance 库，免费，只能拿到最近约 7 天的 1 分钟数据）
只下载正常交易时段：美东时间 9:30–16:00

先安装：pip install yfinance
运行：  python download_data.py
结果：  data/us_1m.csv
"""

import os

import pandas as pd
import yfinance as yf

SYMBOLS = {
    "SNDK":  "闪迪 SanDisk",
    "MU":    "美光 Micron",
    "MSFT":  "微软 Microsoft",
    "GOOGL": "谷歌 Alphabet",
}

frames = []
for symbol, name in SYMBOLS.items():
    df = yf.Ticker(symbol).history(period="5d", interval="1m")
    if df.empty:
        print(f"  ⚠️ {symbol} {name}：没有拿到数据")
        continue
    df = df.reset_index()                       # 时间从索引变成一列
    df["symbol"] = symbol
    df = df.rename(columns={"Datetime": "ts", "Open": "open", "High": "high",
                            "Low": "low", "Close": "close", "Volume": "volume"})
    df["ts"] = df["ts"].dt.tz_convert("America/New_York")   # 统一用美东时间
    frames.append(df[["ts", "symbol", "open", "high", "low", "close", "volume"]])
    print(f"  ✅ {symbol} {name}：{len(df)} 条，{df['ts'].min():%m-%d} 到 {df['ts'].max():%m-%d}")

os.makedirs("data", exist_ok=True)
all_data = pd.concat(frames).sort_values(["ts", "symbol"])
all_data.to_csv("data/us_1m.csv", index=False)
print(f"\n共 {len(all_data)} 条，已保存到 data/us_1m.csv")
