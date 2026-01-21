CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,

    -- Идентификация запроса
    request_id UUID UNIQUE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,

    -- Input данные
    amount NUMERIC(15,2) NOT NULL,
    device_id VARCHAR(255),
    country VARCHAR(10),
    timestamp TIMESTAMP NOT NULL,

    -- Features (все фичи как JSON для ML)
    features JSONB NOT NULL,

    -- Processing
    ruleset_version INT NOT NULL REFERENCES rulesets(version),

    -- Output (результаты скоринга)
    decision VARCHAR(20) NOT NULL,         -- APPROVE, REVIEW, DECLINE
    score NUMERIC(5,2) NOT NULL,           -- 0-100
    reasons TEXT[] NOT NULL,               -- array of reasons

    -- Metadata
    processed_at TIMESTAMP NOT NULL,
    processing_time_ms INT,
    ip_address INET,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска и аналитики
CREATE INDEX idx_audit_log_user_id ON audit_log(user_id);
CREATE INDEX idx_audit_log_transaction_id ON audit_log(transaction_id);
CREATE INDEX idx_audit_log_request_id ON audit_log(request_id);
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at DESC);
CREATE INDEX idx_audit_log_decision ON audit_log(decision);
CREATE INDEX idx_audit_log_ruleset_version ON audit_log(ruleset_version);

-- Composite индексы для часто используемых запросов
CREATE INDEX idx_audit_log_user_created ON audit_log(user_id, created_at DESC);
CREATE INDEX idx_audit_log_user_decision ON audit_log(user_id, decision, created_at DESC);

-- JSONB индекс для поиска по фичам (для ML и аналитики)
CREATE INDEX idx_audit_log_features ON audit_log USING gin (features);