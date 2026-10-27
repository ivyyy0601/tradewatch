# 处理手册：order_size_anomaly（母单金额异常大）

## 怎么识别
- 告警 `order_size_anomaly <PM>`，P1；monitor 已经**自动暂停**了这个 PM（kill switch）
- 告警内容里有：最近 10 分钟最大母单金额、平时平均、z 分数
- 注意：PM 平时下单大小很固定时标准差很小，z 分数会特别大，不一定代表特别严重

## 先做什么（止损）
- 确认 PM 真的停了：`./tw status` 里是 PAUSED、执行中母单为 0
- 有执行中的异常大单 → 和 manager 确认后 `./tw cancel`
- **原因没查清楚之前不要恢复**

## 怎么确认原因
母单金额 = 数量 × 价格，先分清是哪个变了：
```sql
-- 每 15 分钟的母单大小
SELECT date_trunc('hour', created_at) AS h, COUNT(*), ROUND(AVG(qty)) AS avg_qty, MAX(qty) AS max_qty,
       ROUND(MAX(qty * ref_price)) AS max_notional, COUNT(*) FILTER (WHERE status = 'REJECTED') AS rejected
FROM parent_orders WHERE pm_id = 'PM_ALPHA' AND created_at > now() - interval '12 hours' GROUP BY 1 ORDER BY 1;
```
- 数量突然变大、价格正常 → 策略程序的问题。查策略有没有重启 / 发版：
```bash
docker compose logs --since 3h oms | grep -i strategy
```
- 价格跳变 → 查行情（market_data、market-data 日志）
- 大单被风控拒绝（REJECTED）说明第一道防线挡住了，没有真的发出去

## 怎么处理
- 策略问题：找开发 / PM 确认，回滚版本或修复后再恢复
- 恢复前确认：最近几张母单大小已经正常、PM 同意

## 要通知谁
- 立刻告诉 PM 和 manager（PM 被暂停了，仓位还在）
