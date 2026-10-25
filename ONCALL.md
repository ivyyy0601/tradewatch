# TradeWatch 值班手册

你是这家（模拟的）多 PM 对冲基金的 **Trade Support**。交易系统一直在运行，会随机出意外。
你的工作：**发现问题 → 判断严重程度 → 止损 / 上报 → 找到原因 → 记录**。

> ⚠️ 不要打开 `chaos.py`、`data/chaos/` 和 `data/answers.jsonl`，它们是答案。自己写完事故记录再对照。

---

## 1. 公司是怎么交易的

交易系统拆成几个服务（每个是一个 Docker 容器），服务之间**只通过 Kafka 传消息**：

```
 tw-market-data ──md.bars──▶ tw-oms（3 个 PM 的策略 + OMS + 交易前风控，C# / .NET）
                                  │ orders.parent（通过风控的母单）
                                  ▼
                             tw-ems（TWAP：母单拆成 8 个子单，2 分钟没成交完就撤了重下，C# / .NET）
                                  │ exec.orders            ▲ exec.reports
                                  ▼                        │
                             tw-fix-gateway（把子单翻译成 FIX，和券商保持连接）
                                  │ fix.to.<券商>          ▲ fix.from.<券商>
                                  ▼                        │
                             tw-broker-sim（券商 NBRG：SNDK、MU；HRBR：MSFT、GOOGL）
                                  └─ 每个交易日结束：对账单 data/statements/*.csv

 每个服务都把"发生了什么"发到 orders.events / fix.log / ops.state
        └──▶ tw-order-store 把它们写进 PostgreSQL（数据库是唯一的真相来源）

 tw-monitor：每 30 秒查数据库和 Kafka → 告警（alerts 表 + Telegram）；对账单到了就出复盘报告
 tw-console：值班控制台网页（ASP.NET 后端 + Angular 前端）
 tw-chaos：现实世界，随机制造意外（不归你管）
```

**用什么语言写的：** 交易核心（oms、ems）和值班网页用 **C# / .NET 8**（网页前端是 **Angular**），
其他服务和工具用 **Python**，和很多基金的分工一样：交易系统用 C#，支持工具和分析用 Python。
C# 代码在 `dotnet/`，网页前端在 `web/`。

| PM | 策略 | 交易 |
|---|---|---|
| PM_ALPHA | 动量 | SNDK、MU、MSFT、GOOGL |
| PM_BETA | 均值回归 | SNDK、MU、MSFT、GOOGL |
| PM_GAMMA | MU / SNDK 配对（市场中性） | MU、SNDK |

行情是历史真实分钟数据，但打上**现在的真实时间**，1 分钟一根 K 线，24 小时不停。

**一张单在系统里的经历：**
`PARENT_NEW → RISK_PASS（或 RISK_REJECT，到此为止）→ CHILD_SENT → CHILD_ACK → FILL → … → PARENT_DONE`

**Kafka 带来的一类新问题：** 如果某个服务卡住了，发给它的消息会**堆在 Kafka 里**（consumer lag 变大），
它恢复后会一次性处理积压的消息。比如 order-store 卡住时，交易还在继续，但数据库、值班屏幕、告警都停在旧数据上。

---

## 2. 你的工具

### 值班命令行 `./tw`（最常用）
在服务器上 `cd /opt/tradewatch` 后：

| 命令 | 作用 |
|---|---|
| `./tw status` | 一屏看完：服务心跳、FIX 连接、行情、Kafka 积压、各 PM 状态和盈亏、未解决的告警 |
| `./tw trace P1234ABCD` | 一张单的完整经历：母单、子单、每个事件、每条 FIX 消息（母单号或子单号都可以） |
| `./tw alerts` / `./tw alerts all` | 未解决的告警 / 最近 50 条 |
| `./tw resolve 12 已确认是券商断线，已恢复` | 关掉告警，写下处理说明 |
| `./tw pause PM_ALPHA 原因` / `./tw resume PM_ALPHA 原因` | 暂停 / 恢复一个 PM（kill switch） |
| `./tw cancel P1234ABCD 原因` | 撤掉一个母单和它的子单 |
| `./tw lag` | 每个服务在每个 topic 上积压了多少条 |
| `./tw report` | 马上生成复盘报告（平时 monitor 会自动生成） |

真实工作里，暂停 / 恢复 PM、撤单之前要先和 PM / manager 沟通，事后记录谁、什么时候、为什么。

### 值班控制台网页（tw-console）
VS Code → **PORTS** → Forward 端口 `8080` → 浏览器打开 http://localhost:8080
一个页面看完所有状态，每 5 秒刷新；可以直接点按钮**暂停 / 恢复 PM、撤母单、关告警**（都要写原因），
点任何一个母单号就能看到它的完整经历。页面右上角填你的名字，操作记录里会写是谁做的。

### 值班屏幕（Grafana）
VS Code 连上服务器 → 下方 **PORTS** → Forward 端口 `3000` → 浏览器打开 http://localhost:3000
（用户名 `admin`，密码是服务器 `.env` 里的 `GRAFANA_PASSWORD`）。首页就是"TradeWatch 值班屏幕"，
最下面是告警、服务心跳、Kafka 积压。

### 日志平台（Loki）
Grafana 左侧菜单 **Explore** → 数据源选 **Loki**。常用查询（LogQL）：

```
{container=~"tw-.+", level=~"WARN|ERROR"}                所有服务的警告和错误
{container="tw-fix-gateway"}                             FIX 网关的日志
{container="tw-oms", service="risk"}                     风控的日志
{container=~"tw-.+"} |= "C1A2B3C4D5"                     包含某个子单号的所有日志（跨所有服务）
{container=~"tw-.+"} | json | parent_id="P1234ABCD"      按母单号筛选
sum by (container) (count_over_time({container=~"tw-.+", level="WARN"}[5m]))   每个服务每 5 分钟的警告数
```

### 命令行看日志
```bash
docker compose ps                                        # 哪些容器在运行、有没有重启过
docker compose logs --tail 50 fix-gateway                # 某个服务最后 50 行
docker compose logs -f oms ems | grep -E "WARN|ERROR"    # 实时盯两个服务的警告
docker compose logs --no-log-prefix --since 1h oms ems fix-gateway order-store | grep P1234ABCD
```

### 数据库（PostgreSQL）
```bash
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
| `control` | PM 暂停开关（谁、什么时候、为什么） |
| `alerts` | 告警（monitor 写的，`note` 是你的处理说明） |
| `service_heartbeats` | 每个服务最后一次心跳 |
| `kafka_lag` | 每 30 秒记录的 Kafka 积压 |
| `recon_breaks` | 对账发现的差异 |
| 视图 `positions` / `pm_exposure` / `latest_prices` | 持仓、敞口和盈亏、最新价 |

### C# 版 OMS / EMS 出问题时：切回 Python 版
两个版本做的事情完全一样，消息格式也一样，可以随时切换：
```bash
docker compose stop oms ems && docker compose --profile python-core up -d oms-py ems-py    # 切到 Python 版
docker compose stop oms-py ems-py && docker compose up -d oms ems                           # 切回 C# 版
```

### 文件
```bash
ls -lt data/statements/          # 券商对账单
ls -lt data/reports/             # 每日复盘报告（含对账结果）
ls -lt backups/                  # 每天的数据库备份
```

---

## 3. 告警规则（monitor）

| 规则 | 级别 | 意思 | 自动动作 |
|---|---|---|---|
| market_data_stale | P2 | 某只股票 3 分钟没有新行情 | |
| fix_session_down | P2 | 和某家券商的 FIX 连接断了 | |
| child_no_ack | P2 | 子单发出 2 分钟还没收到券商确认 | |
| broker_rejects | P2 | 券商 10 分钟内拒了 3 个以上同样原因的子单 | |
| risk_reject_spike | P3 | 某个 PM 15 分钟内一半以上的母单被风控拒绝 | |
| order_size_anomaly | P1 | 某个 PM 的母单金额远超它平时的水平（z 分数 > 4） | 暂停这个 PM |
| duplicate_parent | P1 | 同一个 PM 5 秒内下了两张一模一样的母单 | 暂停这个 PM |
| parent_stalled | P2 | 母单 5 分钟没有发出新的子单 | |
| service_heartbeat_stale | P2 | 某个服务 3 分钟没有心跳 | |
| kafka_consumer_lag | P2 | 某个服务在 Kafka 上积压超过 100 条 | |
| slippage_high | P3 | 某个 PM 30 分钟平均滑点超过 50 bps | |
| recon_break | P1 | 券商对账单和我们的成交对不上（不会自动解决，要 `./tw resolve`） | |

**告警只是症状。** 一个原因常常同时触发好几条告警，你要找出哪一条是原因、哪些只是结果。

---

## 4. 排查方法：五个问题

| | 问题 | 例子 |
|---|---|---|
| ① | **现象**是什么？ | 哪条告警？屏幕上哪个数字异常？ |
| ② | 从**什么时候**开始？ | 一个时间点突然开始，还是慢慢变差？ |
| ③ | **影响范围**多大？ | 哪个 PM、哪只股票、哪家券商、多少笔单？ |
| ④ | 断在链路的**哪一段**？ | market-data → oms → ems → fix-gateway → 券商 → 回报 → order-store |
| ⑤ | 原因属于哪一类？ | 输入（参数、数据）/ 依赖方（券商、行情）/ 我们自己的系统 |

先止损再查原因：如果损失可能继续扩大（例如持续在下错的单），先暂停，再排查。
分清**正常但需要调整**（比如借券额度用完）和**真的坏了**（比如系统在下错的单）。

---

## 5. FIX 速查

| tag | 意思 | | tag | 意思 |
|---|---|---|---|---|
| 35 | 消息类型 D=新单 F=撤单 8=回报 9=撤单被拒 A=登录 | | 150 | ExecType 0=确认 F=成交 4=已撤 8=拒绝 |
| 11 | ClOrdID（子单号） | | 39 | OrdStatus 0=新 1=部分成交 2=全部成交 4=已撤 8=拒绝 |
| 41 | 要撤的原 ClOrdID | | 32 / 31 | 本次成交量 / 价格 |
| 37 | 券商订单号 | | 14 / 151 | 累计成交量 / 剩余量 |
| 17 | 成交编号（对账用） | | 58 | 文字说明（拒单原因等） |
| 55 / 54 / 38 / 44 | 股票 / 方向(1买 2卖) / 数量 / 限价 | | 1 | 账户（PM） |

---

## 6. 一个班次

1. **接班**：看上一班的交接记录（`docs/handover/`），`./tw status`，看屏幕
2. **盘中**：收到告警就处理，每个事件写一份事故记录（`docs/incidents/`）
3. **每个交易日结束**：看 `data/reports/` 里的复盘报告，有对账差异就查清楚
4. **交班**：写交接记录

---

## 7. 你要自己做的东西（按顺序）

- [ ] 值班演练：收到告警后，只用 `./tw`、日志、SQL 找出原因，写事故记录，再和 `data/answers.jsonl` 对照
- [ ] 把常用排查 SQL 存在 `my_tools/queries.sql`
- [ ] 每条告警规则写一份处理手册：`docs/runbooks/<规则名>.md`（Telegram 告警里会给出这个路径）
- [ ] 记录每次"从告警到找到原因"花了多久，面试时用真实数字
- [ ] 在 Grafana 里给值班屏幕加你自己的面板
- [ ] 想一想：monitor 自己挂了怎么办？（提示：谁来监控监控系统）
