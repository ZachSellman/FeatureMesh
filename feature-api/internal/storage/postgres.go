package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresClient struct {
	db *sql.DB
}

func NewPostgresClient(host string, port int, user, password, dbname string) (*PostgresClient, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresClient{db: db}, nil
}

func (p *PostgresClient) GetOfflineFeature(entityID, entityType, featureName string) (string, error) {
	query := `
        SELECT feature_value 
        FROM offline_features
        WHERE entity_id = $1 
          AND entity_type = $2 
          AND feature_name = $3
        ORDER BY computed_at DESC
        LIMIT 1
    `

	var value string
	err := p.db.QueryRow(query, entityID, entityType, featureName).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return value, nil
}

func (p *PostgresClient) Health() error {
	return p.db.Ping()
}

func (p *PostgresClient) Close() error {
	return p.db.Close()
}
