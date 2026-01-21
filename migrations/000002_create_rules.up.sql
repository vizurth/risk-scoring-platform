CREATE TABLE rules (
    id SERIAL PRIMARY KEY,

    -- Идентификация
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Логика правила
    condition_type VARCHAR(50) NOT NULL,   -- VELOCITY, DEVICE, AMOUNT, COUNTRY, etc
    condition_value TEXT NOT NULL,         -- "tx_count_1m > 5"
    score_delta INT NOT NULL,              -- +30, +20, -10, etc

    -- Версионирование
    version INT NOT NULL,
    ruleset_id INT NOT NULL REFERENCES rulesets(id) ON DELETE CASCADE,

    -- Статус
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE, INACTIVE, DEPRECATED
    priority INT NOT NULL DEFAULT 0,               -- для порядка применения

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Индексы (ВАЖНЫЕ для performance)
CREATE INDEX idx_rules_version ON rules(version);
CREATE INDEX idx_rules_ruleset_id ON rules(ruleset_id);
CREATE INDEX idx_rules_status ON rules(status);
CREATE INDEX idx_rules_condition_type ON rules(condition_type);
CREATE UNIQUE INDEX idx_rules_name_version ON rules(name, vers