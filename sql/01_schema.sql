-- ============================================================
-- TradeWatch 交易数据库（多 PM 对冲基金的简化版）
-- docker compose 第一次启动 PostgreSQL（空数据卷）时自动执行
-- 所有时间都是真实时钟（timestamptz）
-- ============================================================

-- ---------- 参考数据 ----------
CREATE TABLE pms (
    pm_id            TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    strategy         TEXT NOT NULL,
    capital          NUMERIC NOT NULL,       -- 分配的资金（美元）
    max_gross        NUMERIC NOT NULL,       -- 总敞口上限：多头 + |空头|
    max_net          NUMERIC NOT NULL,       -- 净敞口上限：|多头 - 空头|
    max_position     NUMERIC NOT NULL        -- 单只股票的持仓市值上限
);

CREATE TABLE locates (                       -- 当天能借来卖空的股数（开盘时由券商给）
    symbol      TEXT PRIMARY KEY,
    available   INTEGER NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE control (                       -- kill switch：按 PM 暂停
    pm_id       TEXT PRIMARY KEY REFERENCES pms (pm_id),
    paused      BOOLEAN NOT NULL,
    reason      TEXT,
    changed_by  TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL
);

-- ---------- 行情 ----------
CREATE TABLE market_data (
    ts       TIMESTAMPTZ NOT NULL,
    symbol   TEXT NOT NULL,
    price    NUMERIC(14, 4) NOT NULL,
    volume   BIGINT,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX ON market_data (ts);

-- ---------- 订单 ----------
-- 母单：PM 想要的一整笔交易（例如买 2,000 股 MU）
CREATE TABLE parent_orders (
    parent_id      TEXT PRIMARY KEY,
    pm_id          TEXT NOT NULL REFERENCES pms (pm_id),
    symbol         TEXT NOT NULL,
    side           TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty            INTEGER NOT NULL,
    filled_qty     INTEGER NOT NULL DEFAULT 0,
    avg_price      NUMERIC(14, 4),
    ref_price      NUMERIC(14, 4) NOT NULL,    -- 下单时的价格（算滑点用）
    algo           TEXT NOT NULL,              -- TWAP
    status         TEXT NOT NULL,              -- NEW / REJECTED / WORKING / DONE
    reject_reason  TEXT,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON parent_orders (pm_id, created_at);
CREATE INDEX ON parent_orders (status);

-- 子单：执行算法拆出来、真正发给券商的小单（ClOrdID）
CREATE TABLE child_orders (
    child_id     TEXT PRIMARY KEY,             -- FIX 里的 ClOrdID (tag 11)
    parent_id    TEXT NOT NULL REFERENCES parent_orders (parent_id),
    pm_id        TEXT NOT NULL,
    broker       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL,
    qty          INTEGER NOT NULL,
    limit_price  NUMERIC(14, 4) NOT NULL,
    status       TEXT NOT NULL,                -- PENDING_NEW / NEW / PARTIALLY_FILLED / FILLED / PENDING_CANCEL / CANCELLED / REJECTED
    filled_qty   INTEGER NOT NULL DEFAULT 0,
    avg_price    NUMERIC(14, 4),
    reason       TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON child_orders (parent_id);
CREATE INDEX ON child_orders (status, updated_at);

-- 成交：我们收到券商成交回报后记下的每一笔（我们自己的账）
CREATE TABLE executions (
    exec_id    TEXT PRIMARY KEY,               -- FIX tag 17
    child_id   TEXT NOT NULL REFERENCES child_orders (child_id),
    parent_id  TEXT NOT NULL,
    pm_id      TEXT NOT NULL,
    broker     TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    side       TEXT NOT NULL,
    qty        INTEGER NOT NULL,
    price      NUMERIC(14, 4) NOT NULL,
    ts         TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON executions (pm_id, symbol);
CREATE INDEX ON executions (ts);

-- 审计轨迹：订单上发生过的每一件事，只增不改
CREATE TABLE order_events (
    event_id    BIGSERIAL PRIMARY KEY,
    parent_id   TEXT,
    child_id    TEXT,
    service     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    detail      TEXT,
    ts          TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON order_events (parent_id, event_id);
CREATE INDEX ON order_events (child_id, event_id);
CREATE INDEX ON order_events (ts);

-- ---------- FIX ----------
CREATE TABLE fix_sessions (                  -- 和每家券商的连接
    broker          TEXT PRIMARY KEY,
    broker_name     TEXT NOT NULL,
    status          TEXT NOT NULL,           -- UP / DOWN
    last_heartbeat  TIMESTAMPTZ,
    last_msg_in     TIMESTAMPTZ,             -- 最后一次收到券商的业务消息（回报）
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE fix_messages (                  -- 发出和收到的每一条 FIX 消息（心跳除外）
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL,
    broker     TEXT NOT NULL,
    direction  TEXT NOT NULL,                -- OUT = 我们发出 / IN = 券商发来
    msg_type   TEXT NOT NULL,                -- 35=D 新单 / 35=F 撤单 / 35=8 回报 / 35=9 撤单被拒 / 35=A 登录 / 35=5 登出
    cl_ord_id  TEXT,
    raw        TEXT NOT NULL
);
CREATE INDEX ON fix_messages (cl_ord_id);
CREATE INDEX ON fix_messages (ts);

-- ---------- 给你用的 ----------
CREATE TABLE alerts (                        -- 你的监控脚本可以往这里写
    alert_id     BIGSERIAL PRIMARY KEY,
    rule         TEXT NOT NULL,
    key          TEXT NOT NULL,
    severity     TEXT NOT NULL,
    pm_id        TEXT,
    detail       TEXT NOT NULL,
    fired_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ,
    note         TEXT,
    UNIQUE (rule, key)
);

-- ---------- 视图 ----------
CREATE VIEW latest_prices AS
SELECT DISTINCT ON (symbol) symbol, price, ts
FROM market_data ORDER BY symbol, ts DESC;

-- 我们自己算的持仓（从 executions 汇总）
CREATE VIEW positions AS
SELECT pm_id, symbol,
       SUM(CASE side WHEN 'BUY' THEN qty ELSE -qty END) AS qty
FROM executions GROUP BY pm_id, symbol;

-- 每个 PM 的敞口和盈亏（按最新价估值）
CREATE VIEW pm_exposure AS
SELECT p.pm_id,
       COALESCE(SUM(ABS(pos.qty * lp.price)), 0)                     AS gross,
       COALESCE(SUM(pos.qty * lp.price), 0)                          AS net,
       COALESCE((SELECT SUM(CASE e.side WHEN 'BUY' THEN 1 ELSE -1 END * e.qty * (lp2.price - e.price))
                 FROM executions e JOIN latest_prices lp2 USING (symbol)
                 WHERE e.pm_id = p.pm_id), 0)                        AS pnl
FROM pms p
LEFT JOIN positions pos ON pos.pm_id = p.pm_id
LEFT JOIN latest_prices lp ON lp.symbol = pos.symbol
GROUP BY p.pm_id;
