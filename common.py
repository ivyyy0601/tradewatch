"""
各个服务共用的小工具：连数据库、写日志
"""

import json
import os
import time
from datetime import datetime, timezone

import psycopg2


def log(service, level, msg, **fields):
    """标准格式日志：每行一条 JSON，方便以后搜索和统计。
    例：{"ts": "...", "level": "WARN", "service": "risk-gateway", "msg": "RISK_REJECT", "order_id": "..."}"""
    if os.getenv("TW_QUIET") and level == "INFO":      # 命令行工具不需要这些日志
        return
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "service": service,
        "msg": msg,
        **{k: v for k, v in fields.items() if v is not None},
    }
    print(json.dumps(record, ensure_ascii=False, default=str), flush=True)


def connect(service):
    """连接数据库。数据库刚启动时可能还没准备好，所以失败了会重试。"""
    for attempt in range(1, 31):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),   # docker compose 里数据库的服务名
                port=int(os.getenv("DB_PORT", "5432")),
                dbname="tradewatch",
                user="tradewatch",
                password=os.environ["POSTGRES_PASSWORD"],
            )
            conn.autocommit = True                       # 每条 SQL 执行完立刻生效
            log(service, "INFO", "db_connected")
            return conn
        except psycopg2.OperationalError as e:
            log(service, "WARN", "db_not_ready", attempt=attempt, error=str(e).strip())
            time.sleep(2)
    raise SystemExit("cannot connect to database")
