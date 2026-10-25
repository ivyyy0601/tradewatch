"""
PM 的策略：看价格历史，给出"目标仓位"（每只股票想持有多少股，负数 = 卖空）
策略只说"想要什么"，怎么买卖由 OMS 和执行算法负责。
"""

import math
import statistics


def _shares(notional, price, lot=10):
    return int(round(notional / price / lot)) * lot


def momentum(hist, prices, pm):
    """动量：过去 30 分钟涨得越多，越想持有多头；跌得越多，越想做空。"""
    targets = {}
    for s in pm["symbols"]:
        h = hist[s]
        if len(h) < 31:
            continue
        r = h[-1] / h[-31] - 1
        signal = max(-1.0, min(1.0, r / 0.01))            # 涨 1% 就满仓
        targets[s] = _shares(signal * pm["max_position"] * 0.5, prices[s])
    return targets


def meanrev(hist, prices, pm):
    """均值回归：价格比 60 分钟均价高出越多（z 分数），越想做空，反之做多。"""
    targets = {}
    for s in pm["symbols"]:
        h = hist[s][-60:]
        if len(h) < 60:
            continue
        sd = statistics.pstdev(h)
        if sd == 0:
            continue
        z = (h[-1] - statistics.fmean(h)) / sd
        signal = max(-1.0, min(1.0, -z / 2))
        targets[s] = _shares(signal * pm["max_position"] * 0.5, prices[s])
    return targets


_pair_state = {}


def pairs(hist, prices, pm):
    """配对交易：MU 和 SNDK 通常一起涨跌。log(MU/SNDK) 偏离平时太多，就买便宜的、卖贵的，
    两边金额相等（市场中性），等它回到平时的关系再平仓。"""
    a, b = "MU", "SNDK"
    if len(hist[a]) < 60 or len(hist[b]) < 60:
        return {}
    ratio = [math.log(x / y) for x, y in zip(hist[a][-60:], hist[b][-60:])]
    sd = statistics.pstdev(ratio)
    if sd == 0:
        return {}
    z = (ratio[-1] - statistics.fmean(ratio)) / sd
    state = _pair_state.get("pos", 0)                     # 1 = 买 MU 卖 SNDK，-1 = 反过来
    if z > 1.5:
        state = -1                                        # MU 相对贵了：卖 MU、买 SNDK
    elif z < -1.5:
        state = 1
    elif abs(z) < 0.3:
        state = 0                                         # 回到正常关系，平仓
    _pair_state["pos"] = state
    leg = pm["max_position"] * 0.8
    return {a: _shares(state * leg, prices[a]), b: _shares(-state * leg, prices[b])}


STRATEGIES = {"momentum": momentum, "meanrev": meanrev, "pairs": pairs}
