-- AI 值班助手（copilot）用的表。可以重复执行（IF NOT EXISTS）：copilot 每次启动时执行一遍
-- 事故（incident）= 同一个原因引起的一组告警。copilot 以"事故"为单位调查，而不是一条条告警

CREATE TABLE IF NOT EXISTS incidents (
    incident_id      BIGSERIAL PRIMARY KEY,
    status           TEXT NOT NULL DEFAULT 'open',      -- open / resolved
    title            TEXT NOT NULL,
    severity         TEXT NOT NULL,                     -- 组内最严重的告警级别
    components       TEXT[] NOT NULL DEFAULT '{}',      -- 涉及的系统部件（拓扑节点），归并告警用
    source           TEXT NOT NULL DEFAULT 'live',      -- live = 实时；backfill = 用历史告警补建的
    opened_at        TIMESTAMPTZ NOT NULL,
    last_alert_at    TIMESTAMPTZ NOT NULL,
    resolved_at      TIMESTAMPTZ,
    investigate_after TIMESTAMPTZ,                      -- 等一会儿再调查，让相关告警先归进来
    investigated_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS incidents_open ON incidents (status, last_alert_at);

CREATE TABLE IF NOT EXISTS incident_alerts (
    incident_id  BIGINT NOT NULL REFERENCES incidents (incident_id),
    alert_id     BIGINT NOT NULL,
    rule         TEXT NOT NULL,
    key          TEXT NOT NULL,
    severity     TEXT NOT NULL,
    pm_id        TEXT,
    detail       TEXT,
    fired_at     TIMESTAMPTZ NOT NULL,
    resolved_at  TIMESTAMPTZ,
    PRIMARY KEY (incident_id, alert_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS incident_alerts_alert ON incident_alerts (alert_id);

-- 每次调查（一个事故可以被调查多次：自动调查、手动重查、回放）
CREATE TABLE IF NOT EXISTS investigations (
    investigation_id  BIGSERIAL PRIMARY KEY,
    incident_id       BIGINT NOT NULL REFERENCES incidents (incident_id),
    mode              TEXT NOT NULL,                    -- ai / baseline（不用 AI 的规则判断）
    trigger           TEXT NOT NULL DEFAULT 'auto',     -- auto / manual / replay
    as_of             TIMESTAMPTZ NOT NULL,             -- 按哪个时间点的数据调查（回放时是过去的时间）
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    model             TEXT,
    category          TEXT,                             -- 原因类别（固定的几种，评估用）
    root_cause        TEXT,
    confidence        NUMERIC,                          -- 0~1，验证之后的
    confidence_raw    NUMERIC,                          -- 验证之前的
    report            JSONB,
    evidence          JSONB,
    input_tokens      INTEGER NOT NULL DEFAULT 0,
    output_tokens     INTEGER NOT NULL DEFAULT 0,
    cost_usd          NUMERIC NOT NULL DEFAULT 0,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS investigations_incident ON investigations (incident_id, started_at);

-- AI 每一次调用工具都记下来：事后能检查它为什么这么判断
CREATE TABLE IF NOT EXISTS investigation_steps (
    id                BIGSERIAL PRIMARY KEY,
    investigation_id  BIGINT NOT NULL REFERENCES investigations (investigation_id),
    step_no           INTEGER NOT NULL,
    tool              TEXT NOT NULL,
    args              JSONB,
    ok                BOOLEAN NOT NULL,
    result_chars      INTEGER NOT NULL DEFAULT 0,
    duration_ms       INTEGER NOT NULL DEFAULT 0,
    ts                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- AI 只能"建议"操作；人批准以后才执行
CREATE TABLE IF NOT EXISTS action_proposals (
    proposal_id       BIGSERIAL PRIMARY KEY,
    incident_id       BIGINT NOT NULL REFERENCES incidents (incident_id),
    investigation_id  BIGINT REFERENCES investigations (investigation_id),
    action            TEXT NOT NULL,                    -- pause_pm / cancel_parent
    target            TEXT NOT NULL,
    reason            TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending / approved / rejected / executed / expired / failed
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at        TIMESTAMPTZ,
    decided_by        TEXT,
    executed_at       TIMESTAMPTZ,
    note              TEXT
);

-- 评估结果：AI 和规则判断，对照真实答案（data/answers.jsonl）
CREATE TABLE IF NOT EXISTS copilot_eval (
    id                   BIGSERIAL PRIMARY KEY,
    run_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    fault_kind           TEXT NOT NULL,
    fault_start          TIMESTAMPTZ NOT NULL,
    fault_end            TIMESTAMPTZ NOT NULL,
    expected             TEXT NOT NULL,
    incident_id          BIGINT,
    detected             BOOLEAN NOT NULL,
    ai_predicted         TEXT,
    ai_correct           BOOLEAN,
    baseline_predicted   TEXT,
    baseline_correct     BOOLEAN,
    minutes_to_diagnosis NUMERIC,
    cost_usd             NUMERIC
);
