# 处理手册：recon_break（券商对账单和我们的成交对不上）

## 怎么识别
- 告警 `recon_break <对账单>:<券商>`，P1，**不会自动解决**
- 详情在 `data/reports/report_<日期_时间>.md` 和 `recon_breaks` 表

## 先做什么（止损）
- `MISSING_IN_OURS`（券商有、我们没有）最严重：我们的持仓是错的，PM 看到的仓位和风险都不对，先告诉 manager

## 怎么确认原因
```sql
SELECT kind, COUNT(*) FROM recon_breaks WHERE broker = 'HRBR' AND trade_date = '20261025_1608' GROUP BY kind;
-- 拿一个对不上的成交编号去 FIX 原文里搜：回报到底有没有到我们这里
SELECT ts, direction, raw FROM fix_messages WHERE raw LIKE '%17=E998877%';
```
- FIX 原文里有 → 回报到了，是我们后面处理丢了（看 ems / order-store）
- FIX 原文里没有 → 回报没到我们这里（券商没发 / 断线期间丢了）。看那段时间有没有 `fix_session_down`
- 差异数量特别大（几百笔）、而且集中在某个时间段：多半是系统性原因（比如券商模拟器重启、断线），不是零星丢单

## 怎么处理
- 和券商核对，确认后补录成交（真实公司里由运营做）
- 查清楚后 `./tw resolve <编号> <原因和处理>`

## 要通知谁
- manager、PM（持仓可能不对）、运营 / 券商
