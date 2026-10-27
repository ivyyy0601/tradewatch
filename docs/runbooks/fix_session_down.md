# 处理手册：fix_session_down（和券商的 FIX 连接断了）

## 怎么识别
- 告警 `fix_session_down <券商>`；网页 / `./tw status` 的 FIX 连接一栏变成 DOWN
- 经常同时出现：`child_no_ack`（子单没确认）、`parent_stalled`（母单不动）、`fix-gateway-<券商>` 的 Kafka 积压
- 这些都是**结果**，原因是连接断了

## 先做什么（止损）
- 一般不用暂停 PM：断线期间 fix-gateway 会把要发的单排队，券商的回报堆在 Kafka 里，恢复后自动补上
- 如果断线超过 15 分钟、而且这家券商负责的股票有大单在执行，告诉 manager，考虑撤单

## 怎么确认原因
```bash
docker compose logs --since 30m fix-gateway | grep -E "fix_session_lost|queued_session_down|fix_session_restored"
```
```sql
-- 断线期间发给这家券商的子单现在是什么状态
SELECT status, COUNT(*) FROM child_orders
WHERE broker = 'HRBR' AND created_at > now() - interval '30 minutes' GROUP BY status;
```
- 每家券商负责的股票：NBRG = SNDK、MU；HRBR = MSFT、GOOGL

## 怎么处理
- 等连接自动恢复（告警会自动 RESOLVED）
- 恢复后确认：排队的子单补发了（日志有 fix_out），回报回来了（子单不再是 PENDING_NEW）
- 断线期间可能丢回报：收盘对账时注意这家券商有没有 `recon_break`

## 要通知谁
- 超过 5 分钟：告诉 manager；超过 15 分钟：联系券商
