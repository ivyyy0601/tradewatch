# TradeWatch 值班手册

你是这家（模拟的）多 PM 对冲基金的 **Trade Support**。交易系统一直在运行，会随机出意外。
你的工作：**发现问题 → 判断严重程度 → 止损 / 上报 → 找到原因 → 记录**。

> ⚠️ 不要打开 `faults.py` 和 `data/answers.jsonl`，它们是答案。自己写完事故记录再对照。

---

## 1. 公司是怎么交易的

```
3 个 PM（各自的策略、资金、限额）
   │ 每 5 分钟给出"目标仓位"
   ▼
OMS：目标 - 现有持仓 = 母单（parent order）
   ▼
交易前风控：PM 是否暂停 / 单笔金额上限 / 单只股票仓位上限 / 总敞口 / 净敞口 / 卖空是否借到券
   ▼
EMS + TWAP 算法：母单拆成 8 个子单（child order），一个一个发；子单 2 分钟没成交完就撤单重下
   ▼ FIX
券商：NBRG（SNDK、MU）、HRBR（MSFT、GOOGL）
   ▼ FIX 回报
成交记账（executions）→ 持仓、敞口、盈亏
   ▼ 每个交易日结束
券商发来当天的成交对账单：data/statements/*.csv
```

| PM | 策略 | 交易 |
|---|---|---|
| PM_ALPHA | 动量 | SNDK、MU、MSFT、GOOGL |
| PM_BETA | 均值回归 | SNDK、MU、MSFT、GOOGL |
| PM_GAMMA | MU / SNDK 配对（市场中性） | MU、SNDK |

行情是历史真实分钟数据，但打上**现在的真实时间**，1 分钟一根 K 线，24 小时不停。

---

## 2. 你的工具

### 值班屏幕（Grafana）
VS Code 连上服务器 → 下方 **PORTS** → Forward 端口 `3000` → 浏览器打开 http://localhost:3000
（用户名 `admin`，密码是服务器 `.env` 里的 `GRAFANA_PASSWORD`）。首页就是"TradeWatch 值班屏幕"。

### 日志平台（Loki）
Grafana 左侧菜单 **Explore** → 数据源选 **Loki**。常用查询（LogQL）：

```
{container="tw-engine"}                                  所有日志
{container="tw-engine", level="ERROR"}                   只看错误
{container="tw-engine", service="fix-gateway"}           FIX 网关的日志
{container="tw-engine"} |= "C1A2B3C4D5"                  包含某个子单号的所有日志
{container="tw-engine"} | json | parent_id="P1234ABCD"   按母单号筛选
sum by (level) (count_over_time({container="tw-engine"}[5m]))   每 5 分钟各级别日志数量
```

### 数据库（PostgreSQL）
```bash
cd /opt/tradewatch
docker compose exec postgres psql -U tradewatch
```
| 表 | 内容 |
|---|---|
| `pms` | PM 和限额 |
| `parent_orders` | 母单，`reject_reason` 是风控拒绝的原因 |
| `child_orders` | 子单（ClOrdID），`reason` 是券商拒单等原因 |
| `executions` | **我们**记到的每一笔成交 |
| `order_events` | 每笔订单发生过的每件事（按 `parent_id` / `child_id` 查） |
| `fix_messages` | 收发的每条 FIX 消息（`raw` 是原文） |
| `fix_sessions` | 和券商的连接状态、心跳、最后一次回报时间 |
| `market_data` | 行情 |
| `locates` | 今天还能借多少股来卖空 |
| `control` | PM 暂停开关 |
| `alerts` | 留给你的监控脚本写告警 |
| 视图 `positions` / `pm_exposure` / `latest_prices` | 持仓、敞口和盈亏、最新价 |

### 原始日志、对账单
```bash
docker compose logs --since 30m engine | grep '"level": "ERROR"'
ls -lt data/statements/          # 券商对账单
```

### 值班人员可以做的操作（写进数据库，系统会执行）
```sql
-- 暂停一个 PM（风控会拒绝它之后的所有母单）
INSERT INTO control VALUES ('PM_ALPHA', true, '原因', 'Ivy', now())
ON CONFLICT (pm_id) DO UPDATE SET paused = true, reason = EXCLUDED.reason,
  changed_by = EXCLUDED.changed_by, changed_at = now();

-- 恢复
UPDATE control SET paused = false, reason = '原因', changed_by = 'Ivy', changed_at = now()
WHERE pm_id = 'PM_ALPHA';

-- 撤掉一个卡住的母单（连同它的子单）
UPDATE parent_orders SET status = 'CANCEL_REQUESTED' WHERE parent_id = 'P1234ABCD';
```
真实工作里，暂停或恢复 PM 之前要先和 PM / manager 沟通，事后记录谁、什么时候、为什么。

---

## 3. 排查方法：五个问题

| | 问题 | 例子 |
|---|---|---|
| ① | **现象**是什么？ | 屏幕上哪里不对？哪个数字异常？ |
| ② | 从**什么时候**开始？ | 一个时间点突然开始，还是慢慢变差？ |
| ③ | **影响范围**多大？ | 哪个 PM、哪只股票、哪家券商、多少笔单？ |
| ④ | 断在链路的**哪一段**？ | 策略 → OMS → 风控 → EMS → FIX → 券商 → 回报 → 记账 |
| ⑤ | 原因属于哪一类？ | 输入（参数、数据）/ 依赖方（券商、行情）/ 我们自己的系统 |

先止损再查原因：如果损失可能继续扩大（例如持续在下错的单），先暂停，再排查。
分清**原因**和**症状**：一个原因可能同时触发好几个异常。

---

## 4. FIX 速查

| tag | 意思 | | tag | 意思 |
|---|---|---|---|---|
| 35 | 消息类型 D=新单 F=撤单 8=回报 9=撤单被拒 A=登录 | | 150 | ExecType 0=确认 F=成交 4=已撤 8=拒绝 |
| 11 | ClOrdID（子单号） | | 39 | OrdStatus 0=新 1=部分成交 2=全部成交 4=已撤 8=拒绝 |
| 41 | 要撤的原 ClOrdID | | 32 / 31 | 本次成交量 / 价格 |
| 37 | 券商订单号 | | 14 / 151 | 累计成交量 / 剩余量 |
| 17 | 成交编号（对账用） | | 58 | 文字说明（拒单原因等） |
| 55 / 54 / 38 / 44 | 股票 / 方向(1买 2卖) / 数量 / 限价 | | 1 | 账户（PM） |

---

## 5. 一个班次

1. **接班**：看上一班的交接记录（`docs/handover/`），看屏幕，确认 FIX 连接、行情、各 PM 状态
2. **盘中**：盯屏幕，处理异常，每个事件写一份事故记录（`docs/incidents/`）
3. **每个交易日结束**：拿券商对账单和我们的 `executions` 对账
4. **交班**：写交接记录

---

## 6. 你要自己做的东西（按顺序）

- [ ] 第一次值班演练：只用屏幕、日志、SQL，找出发生过的意外，写事故记录
- [ ] 写常用排查 SQL，存在 `my_tools/queries.sql`
- [ ] 写监控脚本 `my_tools/monitor.py`：定时跑你的 SQL，有问题写进 `alerts` 表并发 Telegram
- [ ] 写对账脚本 `my_tools/recon.py`：券商对账单 vs `executions`
- [ ] 每类问题写一份处理手册（`docs/runbooks/`）
- [ ] 在 Grafana 里给值班屏幕加你自己的面板
