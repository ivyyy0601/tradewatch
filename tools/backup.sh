#!/bin/sh
# 数据库备份（tw-backup 容器里运行）：每天一次 pg_dump，保留最近 7 天
mkdir -p /backups
while true; do
  f="/backups/tradewatch_$(date +%Y%m%d_%H%M).sql.gz"
  if pg_dump -h postgres -U tradewatch tradewatch | gzip > "$f"; then
    echo "{\"ts\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"level\": \"INFO\", \"service\": \"backup\", \"msg\": \"backup_done\", \"file\": \"$f\", \"bytes\": $(wc -c < "$f")}"
  else
    echo "{\"ts\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"level\": \"ERROR\", \"service\": \"backup\", \"msg\": \"backup_failed\", \"file\": \"$f\"}"
    rm -f "$f"
  fi
  find /backups -name 'tradewatch_*.sql.gz' -mtime +7 -delete
  sleep 86400
done
