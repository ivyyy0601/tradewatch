"""
FIX 4.4 消息（简化版）：和真实券商通信用的格式，tag=value，用分隔符隔开
这里用 | 代替真实的 SOH 字符（\\x01），方便阅读

常用 tag：
  35 消息类型  D=新单  F=撤单  8=执行回报  9=撤单被拒  A=登录  5=登出  0=心跳
  11 ClOrdID（我们的子单号）  41 原 ClOrdID  37 券商订单号  17 成交编号
  55 股票  54 方向(1=买 2=卖)  38 数量  44 限价  40 订单类型(2=限价)
  150 ExecType  0=已确认 F=成交 4=已撤 8=拒绝
  39 OrdStatus  0=新 1=部分成交 2=全部成交 4=已撤 8=拒绝
  32 本次成交量  31 本次成交价  14 累计成交量  151 剩余数量  58 文字说明
"""

from datetime import datetime, timezone

_seq = {}


def build(msg_type, sender, target, fields):
    _seq[(sender, target)] = _seq.get((sender, target), 0) + 1
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
    body = [("35", msg_type), ("49", sender), ("56", target), ("34", _seq[(sender, target)]), ("52", ts)]
    body += [(t, v) for t, v in fields if v is not None]
    text = "|".join(f"{t}={v}" for t, v in body) + "|"
    head = f"8=FIX.4.4|9={len(text)}|"
    checksum = sum((head + text).replace("|", "\x01").encode()) % 256
    return f"{head}{text}10={checksum:03d}|"


def side_code(side):
    return "1" if side == "BUY" else "2"
