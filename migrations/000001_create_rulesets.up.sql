CREATE TABLE rulesets (
    id SERIAL PRIMARY KEY,
    version INT NOT NULL UNIQUE,
    description VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

    approve_max INT NOT NULL,
    review_max INT NOT NULL,
    decline_max INT NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    activeted_at TIMESTAMP,
    created_by VARCHAR(100),
    activated_by VARCHAR(100)
)

CREATE INDEX idx_rulesets_status ON rulesets(status);
CREATE INDEX idx_rulesets_version ON rulesets(version);
CREATE INDEX idx_rulesets_activated_at ON rulesets(activeted_at DECS);