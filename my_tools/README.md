# 你自己写的值班工具放这里

公司的工具已经有了：`./tw`（命令行）、monitor（告警）、每日复盘和对账（data/reports/）。
这里放你在值班中自己积累的东西：

- queries.sql：你常用的排查 SQL（每条写一行注释：什么时候用、看什么）
- 你自己写的小脚本（比如找出某段时间所有被拒的子单、统计每家券商的确认时间……）

在服务器上运行你的脚本（用交易系统同一个镜像，里面有 pandas、psycopg2、confluent-kafka）：
    docker compose run --rm -v ./my_tools:/app/my_tools tools python my_tools/你的脚本.py
直接跑 SQL 文件：
    docker compose exec -T postgres psql -U tradewatch < my_tools/queries.sql
