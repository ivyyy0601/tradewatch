-- TradeWatch 数据库表结构（PostgreSQL）
-- docker compose 第一次启动 PostgreSQL 时会自动执行这个文件

-- 每笔订单一行：当前状态
CREATE TABLE orders (
    order_id     TEXT PRIMARY KEY,
    trace_id     TEXT NOT NULL,
    strategy     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty          INTEGER NOT NULL,
    ref_price    NUMERIC(12, 4) NOT NULL,   -- 下单时看到的价格，算滑点用
    status       TEXT NOT NULL,             -- NEW / APPROVED / ACKED / PARTIALLY_FILLED / FILLED / REJECTED / CANCELLED
    filled_qty   INTEGER NOT NULL DEFAULT 0,
    avg_price    NUMERIC(12, 4),            -- 成交均价
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON orders (strategy, created_at);
CREATE INDEX ON orders (status) WHERE status IN ('APPROVED', 'ACKED', 'PARTIALLY_FILLED');

-- 发生过的每件事：只增不改，排查时的"物流记录"
CREATE TABLE order_events (
    event_id     BIGSERIAL PRIMARY KEY,
    order_id     TEXT NOT NULL REFERENCES orders (order_id),
    trace_id     TEXT NOT NULL,
    service      TEXT NOT NULL,
    event_type   TEXT NOT NULL,             -- NEW / RISK_PASS / RISK_REJECT / ACK / PARTIAL_FILL / FILL / BROKER_REJECT / CANCEL
    qty          INTEGER,
    price        NUMERIC(12, 4),
    reason       TEXT,
    ts           TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON order_events (order_id, event_id);
CREATE INDEX ON order_events (ts);

-- 行情：回放出来的每分钟价格，Grafana 画价格曲线用
CREATE TABLE market_data (
    ts       TIMESTAMPTZ NOT NULL,
    symbol   TEXT NOT NULL,
    price    NUMERIC(12, 4) NOT NULL,
    volume   BIGINT,
    PRIMARY KEY (symbol, ts)
);

-- 券商自己记的持仓，用来和我们自己算的持仓对账
CREATE TABLE broker_positions (
    strategy    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    qty         INTEGER NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (strategy, symbol)
);

-- 告警记录本
CREATE TABLE alerts (
    alert_id     BIGSERIAL PRIMARY KEY,
    rule         TEXT NOT NULL,
    key          TEXT NOT NULL,
    severity     TEXT NOT NULL,             -- P1 / P2 / P3
    strategy     TEXT,
    detail       TEXT NOT NULL,
    fired_at     TIMESTAMPTZ NOT NULL,
    resolved_at  TIMESTAMPTZ,
    note         TEXT,                      -- 值班的人写的处理记录
    UNIQUE (rule, key)
);

-- 开关表：kill switch。只能由人手动恢复
CREATE TABLE control (
    strategy   TEXT PRIMARY KEY,
    paused     BOOLEAN NOT NULL,
    reason     TEXT,
    changed_by TEXT,                        -- monitor（自动）或 人名（手动）
    changed_at TIMESTAMPTZ NOT NULL
);

-- 我们自己算的持仓：从成交事件汇总出来
CREATE VIEW positions AS
SELECT o.strategy, o.symbol,
       SUM(CASE o.side WHEN 'BUY' THEN e.qty ELSE -e.qty END) AS qty
FROM order_events e
JOIN orders o USING (order_id)
WHERE e.event_type IN ('PARTIAL_FILL', 'FILL')
GROUP BY o.strategy, o.symbol;
