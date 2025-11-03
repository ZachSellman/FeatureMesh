-- Feature metadata registry
CREATE TABLE IF NOT EXISTS feature_definitions (
    feature_name VARCHAR(255) PRIMARY KEY,
    feature_type VARCHAR(50) NOT NULL,
    description TEXT,
    ttl_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Offline feature storage (for training)
CREATE TABLE IF NOT EXISTS offline_features (
    entity_id VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    feature_name VARCHAR(255) NOT NULL,
    feature_value TEXT NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (entity_id, entity_type, feature_name, computed_at)
);

CREATE INDEX idx_offline_features_lookup 
ON offline_features(entity_id, entity_type, feature_name);

CREATE INDEX idx_offline_features_time 
ON offline_features(computed_at);

-- Consistency check results
CREATE TABLE IF NOT EXISTS consistency_checks (
    check_id SERIAL PRIMARY KEY,
    check_time TIMESTAMP NOT NULL,
    entity_id VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    feature_name VARCHAR(255) NOT NULL,
    online_value TEXT,
    offline_value TEXT,
    is_consistent BOOLEAN NOT NULL,
    difference TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_consistency_checks_time ON consistency_checks(check_time);
CREATE INDEX idx_consistency_checks_result ON consistency_checks(is_consistent);