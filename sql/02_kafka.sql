-- 拆成多个服务、加上 Kafka 之后新增的表和字段
-- 可以重复执行（IF NOT EXISTS）：新安装时数据库会自动执行；已有的数据库由 order-store 启动时执行

-- 事件、FIX 消息带一个唯一编号：order-store 重启后重读 Kafka 消息也不会重复写入
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS event_uid TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS order_events_event_uid ON order_events (event_uid);
ALTER TABLE fix_messages ADD COLUMN IF NOT EXISTS msg_uid TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS fix_messages_msg_uid ON fix_messages (msg_uid);

-- 每个服务的心跳：最后一次"我还活着"是什么时候
CREATE TABLE IF NOT EXISTS service_heartbeats (
    service    TEXT PRIMARY KEY,
    last_seen  TIMESTAMPTZ NOT NULL,
    detail     TEXT
);

-- Kafka 消费积压（consumer lag）：每个服务还有多少条消息没读（monitor 每 30 秒记一次）
CREATE TABLE IF NOT EXISTS kafka_lag (
    ts        TIMESTAMPTZ NOT NULL,
    group_id  TEXT NOT NULL,
    topic     TEXT NOT NULL,
    lag       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS kafka_lag_ts ON kafka_lag (ts);

-- 每日复盘报告和对账结果
CREATE TABLE IF NOT EXISTS recon_breaks (
    id          BIGSERIAL PRIMARY KEY,
    trade_date  TEXT NOT NULL,
    broker      TEXT NOT NULL,
    exec_id     TEXT NOT NULL,
    kind        TEXT NOT NULL,          -- MISSING_IN_OURS（券商有、我们没有）/ MISSING_AT_BROKER / QTY_OR_PRICE_DIFF
    detail      TEXT,
    found_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (broker, exec_id, kind)
);

-- 告警：同一条规则可以多次触发（保留历史），所以去掉原来的 (rule, key) 唯一限制
ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_rule_key_key;
CREATE INDEX IF NOT EXISTS alerts_open ON alerts (rule, key) WHERE resolved_at IS NULL;
