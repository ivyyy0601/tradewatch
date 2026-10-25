# 你自己写的值班工具放这里

- queries.sql：常用排查 SQL
- monitor.py：监控脚本（定时跑 SQL，写 alerts 表，发 Telegram）
- recon.py：券商对账单 vs executions

在服务器上可以这样运行（用交易系统同一个镜像，里面有 pandas 和 psycopg2）：
    docker compose run --rm -e PYTHONPATH=/app -v ./my_tools:/app/my_tools engine python my_tools/monitor.py
