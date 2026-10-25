"""
交易系统的配置：PM、券商、路由、风控参数
（相当于公司里的参考数据，一般由风控和运营团队维护）
"""

SYMBOLS = ["SNDK", "MU", "MSFT", "GOOGL"]

# 三个 PM 团队，各自一种策略、一笔资金、一套限额（美元）
PMS = {
    "PM_ALPHA": {"name": "Alpha Momentum", "strategy": "momentum", "symbols": SYMBOLS,
                 "capital": 2_000_000, "max_gross": 1_600_000, "max_net": 1_000_000, "max_position": 450_000},
    "PM_BETA":  {"name": "Beta Mean Reversion", "strategy": "meanrev", "symbols": SYMBOLS,
                 "capital": 2_000_000, "max_gross": 1_600_000, "max_net": 1_000_000, "max_position": 450_000},
    "PM_GAMMA": {"name": "Gamma Pairs MU/SNDK", "strategy": "pairs", "symbols": ["MU", "SNDK"],
                 "capital": 1_500_000, "max_gross": 1_000_000, "max_net": 450_000, "max_position": 500_000},
}

# 两家券商（虚构的名字）
BROKERS = {
    "NBRG": "Northbridge Securities",
    "HRBR": "Harbor Capital Markets",
}
ROUTING = {"SNDK": "NBRG", "MU": "NBRG", "MSFT": "HRBR", "GOOGL": "HRBR"}   # 哪只股票发给哪家券商
OUR_COMP_ID = "TRADEWATCH"

# 每天开盘时券商给的可借券数量（卖空用）
DAILY_LOCATES = {"SNDK": 3_000, "MU": 5_000, "MSFT": 8_000, "GOOGL": 10_000}

# OMS / 风控
REBALANCE_EVERY = 5            # 每 5 根 K 线（行情分钟）策略重新算一次目标仓位
MIN_ORDER_NOTIONAL = 25_000    # 目标和现在差不到 2.5 万美元，就不下单
MAX_ORDER_NOTIONAL = 750_000   # 单个母单最多 75 万美元（防手滑）

# EMS / TWAP 执行算法
TWAP_SLICES = 8                # 母单拆成 8 个子单
CHILD_LIMIT_BPS = 5            # 子单限价 = 当前价 ± 5 个基点
CHILD_TIMEOUT_BARS = 2         # 子单 2 根 K 线还没成交完就撤掉，剩下的重新下
LOT = 10                       # 股数取 10 的整数倍
