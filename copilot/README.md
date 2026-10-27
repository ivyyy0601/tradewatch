# copilot：AI 值班助手

告警来了以后，copilot 像一个有经验的值班员那样处理：先把相关告警归成一个事故，按剧本收集固定的证据，再让 AI 提出假设、用只读工具查证，最后复核结论。整理好的报告发到 Telegram。会影响交易的操作，AI 只能**建议**，必须由人批准。

它不是"把告警丢给 AI 问一句"。能用代码确定做的事都由代码做，AI 只负责它擅长的部分：读证据、提假设、写人话。

## 流水线

```
monitor 触发告警 ──Kafka ops.alerts──▶ copilot
                                        │
 1 归并  correlate.py   时间相近 + 拓扑相关（topology.yaml）的告警 → 一个事故         纯代码
 2 等待                 事故开始后等 60 秒，让相关告警先归进来                       纯代码
 3 取证  collect.py     按剧本（playbooks.yaml）跑命名查询 + 查 Loki 日志 → 证据包    纯代码
 4 诊断  agent.py       AI 读证据包，提 2~3 个假设，用只读工具查证，提交结构化结论    AI
 5 复核  agent.py       换一个角色专门找漏洞；复核只能降低可信度，不能提高           AI
 6 建议  approvals.py   AI 只能建议"暂停 PM / 撤母单"，Telegram 按钮或 ./tw approve   人批准
 7 执行                 批准后往 ops.commands 发命令（和 ./tw pause 一样）             纯代码
 8 沉淀  investigate.py 事故结束后起草事故记录：docs/incidents/drafts/INC-<id>.md     纯代码
 9 评估  eval.py        对照 chaos 的真实答案打分，和规则判断（baseline）比           纯代码
```

没有 `ANTHROPIC_API_KEY` 也能跑：第 4、5 步换成规则判断（baseline.py），其他步骤照常。

## 安全设计

| 规则 | 怎么做到的 |
|---|---|
| AI 不能自己写 SQL | 只能用 `queries.yaml` 里的命名查询；参数有类型检查（PM 名、券商名必须在白名单里） |
| AI 只能读 | 查询走只读事务，每条最多 5 秒；日志通过 Loki HTTP 接口查，不需要 docker 权限 |
| 结果有上限 | 每次工具结果最多 6000 字符，查询最多 60 行，证据包最多 24000 字符 |
| 调用次数有上限 | 一次调查最多 8 次工具调用；每天有费用预算，超了自动改用规则判断 |
| 操作要人批准 | AI 只有 `propose_action`（建议）；真正执行在 approvals.py，只处理人批准过的建议 |
| 硬规则写在代码里 | 只能暂停 PM 或撤母单；**永远不能恢复 PM**；执行前再检查一次目标；30 分钟没人批准就作废 |
| 全程留记录 | 每次调查、每次工具调用（参数、耗时、结果大小）、每个建议和谁批准的，都存在数据库里 |

## 文件

| 文件 | 作用 |
|---|---|
| `main.py` | 服务主循环：读 Kafka ops.alerts，每 20 秒和 alerts 表对一次账、执行已批准的建议、调查到时间的事故 |
| `topology.yaml` / `topology.py` | 系统拓扑：部件、上下游、告警规则落在哪些部件上 |
| `correlate.py` | 告警归并成事故 |
| `playbooks.yaml` / `collect.py` | 每种告警的取证剧本，生成证据包 |
| `queries.yaml` | 命名查询库（AI 能用的全部 SQL） |
| `tools.py` | 工具注册：名字、说明、参数格式、权限级别、审计 |
| `loki.py` | 查日志 |
| `llm.py` | 调用 Claude（直接 HTTP，不依赖 SDK） |
| `agent.py` | 诊断循环、复核、没有 AI 时的报告 |
| `baseline.py` | 原因类别定义 + 规则判断（兜底 + 评估的对照组） |
| `approvals.py` | 建议 → 批准 → 执行 |
| `notify.py` | Telegram：发报告、按钮、收按钮点击 |
| `report.py` | 报告排版（Telegram、事故记录草稿） |
| `investigate.py` | 一次完整调查的编排 |
| `eval.py` | 回填、回放、打分 |

数据库表在 `sql/03_copilot.sql`：incidents、incident_alerts、investigations、investigation_steps、action_proposals、copilot_eval。

## 常用命令

```bash
./tw incidents                 # 最近的事故和 AI 判断
./tw incident 12               # 一个事故的详情：告警、AI 报告、每一步工具调用、建议
./tw investigate 12            # 让 copilot 重新调查事故 12（a55 = 从告警 55 开始）
./tw proposals                 # 等你批准的建议
./tw approve 3 已和 manager 确认  /  ./tw reject 3 不需要
./tw ai-eval                   # 对照真实答案打分（会显示答案）
./tw ai-backfill 48            # 用过去 48 小时的历史告警补建事故
./tw ai-replay 48 10           # 让 AI 用当时的数据重新调查过去的事故（花钱）
docker compose logs --tail 50 copilot
```

## 怎么改进它

- **加一种告警的剧本**：在 `playbooks.yaml` 的 `rules` 下加一段
- **加一条 AI 能用的查询**：在 `queries.yaml` 加一段（只能是 SELECT，必须用 `%(as_of)s` 当时间点）
- **教 AI 怎么处理某种问题**：写 `docs/runbooks/<规则名>.md`，AI 调查时会读
- **看改动有没有变好**：`./tw ai-replay 48 20 --force` 重新调查，再 `./tw ai-eval` 比准确率
