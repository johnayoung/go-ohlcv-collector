// Package storage provides database migration management for DuckDB.
// This module handles schema evolution, table creation, and data migration
// operations for the OHLCV data collector application.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// Migration represents a single database migration with version and implementation
type Migration struct {
	Version     int
	Description string
	Up          func(ctx context.Context, db *sql.Tx) error
	Down        func(ctx context.Context, db *sql.Tx) error
}

// MigrationManager handles database schema migrations for DuckDB
type MigrationManager struct {
	db      *sql.DB
	logger  *slog.Logger
	migrate []Migration
}

// NewMigrationManager creates a new migration manager instance
func NewMigrationManager(db *sql.DB, logger *slog.Logger) *MigrationManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &MigrationManager{
		db:     db,
		logger: logger,
		migrate: getAllMigrations(),
	}
}

// Initialize creates the migrations table if it doesn't exist
func (m *MigrationManager) Initialize(ctx context.Context) error {
	createMigrationsTable := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			description VARCHAR NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			execution_time BIGINT NOT NULL DEFAULT 0
		)`

	if _, err := m.db.ExecContext(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	m.logger.Info("migration manager initialized successfully")
	return nil
}

// Migrate runs all pending migrations up to the target version
func (m *MigrationManager) Migrate(ctx context.Context, targetVersion int) error {
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migration manager: %w", err)
	}

	currentVersion, err := m.getCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	m.logger.Info("starting migration", 
		"current_version", currentVersion,
		"target_version", targetVersion)

	if currentVersion >= targetVersion {
		m.logger.Info("no migrations to run", "current_version", currentVersion)
		return nil
	}

	// Filter migrations to run
	migrationsToRun := make([]Migration, 0)
	for _, migration := range m.migrate {
		if migration.Version > currentVersion && migration.Version <= targetVersion {
			migrationsToRun = append(migrationsToRun, migration)
		}
	}

	if len(migrationsToRun) == 0 {
		m.logger.Info("no migrations found in range", 
			"current", currentVersion, 
			"target", targetVersion)
		return nil
	}

	// Run migrations in sequence
	for _, migration := range migrationsToRun {
		if err := m.runMigration(ctx, migration); err != nil {
			return fmt.Errorf("failed to run migration %d: %w", migration.Version, err)
		}
	}

	m.logger.Info("all migrations completed successfully", 
		"final_version", targetVersion,
		"migrations_run", len(migrationsToRun))

	return nil
}

// MigrateToLatest runs all available migrations
func (m *MigrationManager) MigrateToLatest(ctx context.Context) error {
	if len(m.migrate) == 0 {
		m.logger.Info("no migrations available")
		return nil
	}

	latestVersion := m.migrate[len(m.migrate)-1].Version
	return m.Migrate(ctx, latestVersion)
}

// Rollback rolls back migrations to the target version
func (m *MigrationManager) Rollback(ctx context.Context, targetVersion int) error {
	currentVersion, err := m.getCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	m.logger.Info("starting rollback", 
		"current_version", currentVersion,
		"target_version", targetVersion)

	if currentVersion <= targetVersion {
		m.logger.Info("no rollback needed", "current_version", currentVersion)
		return nil
	}

	// Filter migrations to rollback (in reverse order)
	migrationsToRollback := make([]Migration, 0)
	for i := len(m.migrate) - 1; i >= 0; i-- {
		migration := m.migrate[i]
		if migration.Version > targetVersion && migration.Version <= currentVersion {
			migrationsToRollback = append(migrationsToRollback, migration)
		}
	}

	// Run rollbacks in reverse order
	for _, migration := range migrationsToRollback {
		if err := m.rollbackMigration(ctx, migration); err != nil {
			return fmt.Errorf("failed to rollback migration %d: %w", migration.Version, err)
		}
	}

	m.logger.Info("rollback completed successfully", 
		"final_version", targetVersion,
		"migrations_rolled_back", len(migrationsToRollback))

	return nil
}

// GetStatus returns the current migration status
func (m *MigrationManager) GetStatus(ctx context.Context) (*MigrationStatus, error) {
	currentVersion, err := m.getCurrentVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}

	appliedMigrations, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}

	latestVersion := 0
	if len(m.migrate) > 0 {
		latestVersion = m.migrate[len(m.migrate)-1].Version
	}

	pendingCount := 0
	for _, migration := range m.migrate {
		if migration.Version > currentVersion {
			pendingCount++
		}
	}

	return &MigrationStatus{
		CurrentVersion:      currentVersion,
		LatestVersion:       latestVersion,
		AppliedMigrations:   appliedMigrations,
		PendingMigrations:   pendingCount,
		TotalMigrations:     len(m.migrate),
		DatabaseInitialized: currentVersion >= 1,
	}, nil
}

// runMigration executes a single migration with timing and error handling
func (m *MigrationManager) runMigration(ctx context.Context, migration Migration) error {
	start := time.Now()
	
	m.logger.Info("applying migration", 
		"version", migration.Version,
		"description", migration.Description)

	// Execute migration in transaction for atomicity
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Run migration function
	if err := migration.Up(ctx, tx); err != nil {
		return fmt.Errorf("migration execution failed: %w", err)
	}

	// Record migration
	executionTime := time.Since(start).Nanoseconds()
	insertQuery := `
		INSERT INTO schema_migrations (version, description, applied_at, execution_time) 
		VALUES ($1, $2, $3, $4)`

	if _, err := tx.ExecContext(ctx, insertQuery, 
		migration.Version, 
		migration.Description, 
		start,
		executionTime); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration: %w", err)
	}

	duration := time.Since(start)
	m.logger.Info("migration applied successfully", 
		"version", migration.Version,
		"duration", duration)

	return nil
}

// rollbackMigration executes a single migration rollback
func (m *MigrationManager) rollbackMigration(ctx context.Context, migration Migration) error {
	start := time.Now()
	
	m.logger.Info("rolling back migration", 
		"version", migration.Version,
		"description", migration.Description)

	if migration.Down == nil {
		return fmt.Errorf("migration %d has no rollback function", migration.Version)
	}

	// Execute rollback in transaction
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start rollback transaction: %w", err)
	}
	defer tx.Rollback()

	// Run rollback function
	if err := migration.Down(ctx, tx); err != nil {
		return fmt.Errorf("rollback execution failed: %w", err)
	}

	// Remove migration record
	deleteQuery := "DELETE FROM schema_migrations WHERE version = $1"
	if _, err := tx.ExecContext(ctx, deleteQuery, migration.Version); err != nil {
		return fmt.Errorf("failed to remove migration record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollback: %w", err)
	}

	m.logger.Info("migration rolled back successfully", 
		"version", migration.Version,
		"duration", time.Since(start))

	return nil
}

// getCurrentVersion returns the highest applied migration version
func (m *MigrationManager) getCurrentVersion(ctx context.Context) (int, error) {
	var version int
	query := "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
	
	err := m.db.QueryRowContext(ctx, query).Scan(&version)
	if err != nil {
		// Table might not exist yet
		if err.Error() == "no such table: schema_migrations" {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, nil
}

// getAppliedMigrations returns list of applied migrations with metadata
func (m *MigrationManager) getAppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	query := `
		SELECT version, description, applied_at, execution_time 
		FROM schema_migrations 
		ORDER BY version`

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var migrations []AppliedMigration
	for rows.Next() {
		var migration AppliedMigration
		var executionTime int64

		err := rows.Scan(
			&migration.Version,
			&migration.Description,
			&migration.AppliedAt,
			&executionTime,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan migration row: %w", err)
		}

		migration.ExecutionTime = time.Duration(executionTime)
		migrations = append(migrations, migration)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return migrations, nil
}

// getAllMigrations returns the complete list of available migrations
func getAllMigrations() []Migration {
	return []Migration{
		{
			Version:     1,
			Description: "Initial schema - candles and gaps tables",
			Up:          migrationV1Up,
			Down:        migrationV1Down,
		},
		{
			Version:     2,
			Description: "Add collection jobs tracking table",
			Up:          migrationV2Up,
			Down:        migrationV2Down,
		},
		{
			Version:     3,
			Description: "Add validation results and anomalies tables",
			Up:          migrationV3Up,
			Down:        migrationV3Down,
		},
		{
			Version:     4,
			Description: "Add trading pairs catalog table",
			Up:          migrationV4Up,
			Down:        migrationV4Down,
		},
		{
			Version:     5,
			Description: "Add performance indexes for analytical queries",
			Up:          migrationV5Up,
			Down:        migrationV5Down,
		},
	}
}

// Migration V1: Initial schema with candles and gaps tables
func migrationV1Up(ctx context.Context, db *sql.Tx) error {
	queries := []string{
		// Candles table with comprehensive constraints
		`CREATE TABLE IF NOT EXISTS candles (
			timestamp TIMESTAMPTZ NOT NULL,
			open DECIMAL(18,8) NOT NULL,
			high DECIMAL(18,8) NOT NULL,
			low DECIMAL(18,8) NOT NULL,
			close DECIMAL(18,8) NOT NULL,
			volume DECIMAL(18,8) NOT NULL,
			pair VARCHAR NOT NULL,
			interval VARCHAR NOT NULL,
			created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT candles_pk PRIMARY KEY (pair, interval, timestamp),
			CONSTRAINT candles_ohlc_valid CHECK (high >= open AND high >= close AND low <= open AND low <= close),
			CONSTRAINT candles_prices_positive CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
			CONSTRAINT candles_volume_non_negative CHECK (volume >= 0)
		)`,

		// Gaps table for tracking missing data periods
		`CREATE TABLE IF NOT EXISTS gaps (
			id VARCHAR PRIMARY KEY,
			pair VARCHAR NOT NULL,
			start_time TIMESTAMPTZ NOT NULL,
			end_time TIMESTAMPTZ NOT NULL,
			interval VARCHAR NOT NULL,
			status VARCHAR NOT NULL CHECK (status IN ('detected', 'filling', 'filled', 'permanent')),
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			filled_at TIMESTAMPTZ,
			priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0 AND priority <= 3),
			attempts INTEGER NOT NULL DEFAULT 0,
			last_attempt_at TIMESTAMPTZ,
			error_message VARCHAR,
			CONSTRAINT gaps_time_order CHECK (end_time > start_time)
		)`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

func migrationV1Down(ctx context.Context, db *sql.Tx) error {
	queries := []string{
		"DROP TABLE IF EXISTS gaps",
		"DROP TABLE IF EXISTS candles",
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute rollback query: %w", err)
		}
	}

	return nil
}

// Migration V2: Collection jobs tracking
func migrationV2Up(ctx context.Context, db *sql.Tx) error {
	query := `
		CREATE TABLE IF NOT EXISTS collection_jobs (
			id VARCHAR PRIMARY KEY,
			type VARCHAR NOT NULL CHECK (type IN ('initial_sync', 'update', 'backfill')),
			pair VARCHAR NOT NULL,
			start_time TIMESTAMPTZ NOT NULL,
			end_time TIMESTAMPTZ NOT NULL,
			interval VARCHAR NOT NULL,
			status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
			progress INTEGER NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
			records_collected INTEGER NOT NULL DEFAULT 0,
			error_message VARCHAR,
			retry_count INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT jobs_time_order CHECK (end_time > start_time)
		)`

	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create collection_jobs table: %w", err)
	}

	return nil
}

func migrationV2Down(ctx context.Context, db *sql.Tx) error {
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS collection_jobs"); err != nil {
		return fmt.Errorf("failed to drop collection_jobs table: %w", err)
	}
	return nil
}

// Migration V3: Validation results and anomalies
func migrationV3Up(ctx context.Context, db *sql.Tx) error {
	queries := []string{
		// Validation results table
		`CREATE TABLE IF NOT EXISTS validation_results (
			id VARCHAR PRIMARY KEY,
			candle_timestamp TIMESTAMPTZ NOT NULL,
			pair VARCHAR NOT NULL,
			validation_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			checks_passed TEXT[] NOT NULL DEFAULT [],
			severity VARCHAR NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
			action_taken VARCHAR,
			FOREIGN KEY (pair, interval, candle_timestamp) REFERENCES candles(pair, interval, timestamp)
		)`,

		// Anomalies table  
		`CREATE TABLE IF NOT EXISTS anomalies (
			id VARCHAR PRIMARY KEY,
			validation_result_id VARCHAR NOT NULL,
			type VARCHAR NOT NULL CHECK (type IN ('price_spike', 'volume_surge', 'logic_error', 'sequence_gap')),
			description VARCHAR NOT NULL,
			value DECIMAL(18,8),
			threshold DECIMAL(18,8),
			confidence DECIMAL(3,2) NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
			FOREIGN KEY (validation_result_id) REFERENCES validation_results(id)
		)`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

func migrationV3Down(ctx context.Context, db *sql.Tx) error {
	queries := []string{
		"DROP TABLE IF EXISTS anomalies",
		"DROP TABLE IF EXISTS validation_results",
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute rollback query: %w", err)
		}
	}

	return nil
}

// Migration V4: Trading pairs catalog
func migrationV4Up(ctx context.Context, db *sql.Tx) error {
	query := `
		CREATE TABLE IF NOT EXISTS trading_pairs (
			symbol VARCHAR PRIMARY KEY,
			base_asset VARCHAR NOT NULL,
			quote_asset VARCHAR NOT NULL,
			exchange VARCHAR NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true,
			min_volume DECIMAL(18,8),
			price_decimals INTEGER NOT NULL DEFAULT 8,
			volume_decimals INTEGER NOT NULL DEFAULT 8,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`

	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create trading_pairs table: %w", err)
	}

	return nil
}

func migrationV4Down(ctx context.Context, db *sql.Tx) error {
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS trading_pairs"); err != nil {
		return fmt.Errorf("failed to drop trading_pairs table: %w", err)
	}
	return nil
}

// Migration V5: Performance indexes
func migrationV5Up(ctx context.Context, db *sql.Tx) error {
	indexes := []string{
		// Candles indexes for optimal query performance
		"CREATE INDEX IF NOT EXISTS idx_candles_pair_interval ON candles (pair, interval)",
		"CREATE INDEX IF NOT EXISTS idx_candles_timestamp ON candles (timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_candles_pair_timestamp ON candles (pair, timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_candles_volume ON candles (volume)",
		
		// Gaps indexes for efficient gap management
		"CREATE INDEX IF NOT EXISTS idx_gaps_pair_interval ON gaps (pair, interval)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_status ON gaps (status)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_priority_status ON gaps (priority DESC, status)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_created_at ON gaps (created_at)",

		// Collection jobs indexes
		"CREATE INDEX IF NOT EXISTS idx_jobs_status ON collection_jobs (status)",
		"CREATE INDEX IF NOT EXISTS idx_jobs_type ON collection_jobs (type)",
		"CREATE INDEX IF NOT EXISTS idx_jobs_pair ON collection_jobs (pair)",
		"CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON collection_jobs (created_at)",

		// Validation results indexes
		"CREATE INDEX IF NOT EXISTS idx_validation_pair ON validation_results (pair)",
		"CREATE INDEX IF NOT EXISTS idx_validation_timestamp ON validation_results (candle_timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_validation_severity ON validation_results (severity)",

		// Trading pairs indexes
		"CREATE INDEX IF NOT EXISTS idx_pairs_exchange ON trading_pairs (exchange)",
		"CREATE INDEX IF NOT EXISTS idx_pairs_active ON trading_pairs (active)",
	}

	for _, indexQuery := range indexes {
		if _, err := db.ExecContext(ctx, indexQuery); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

func migrationV5Down(ctx context.Context, db *sql.Tx) error {
	indexes := []string{
		"DROP INDEX IF EXISTS idx_candles_pair_interval",
		"DROP INDEX IF EXISTS idx_candles_timestamp", 
		"DROP INDEX IF EXISTS idx_candles_pair_timestamp",
		"DROP INDEX IF EXISTS idx_candles_volume",
		"DROP INDEX IF EXISTS idx_gaps_pair_interval",
		"DROP INDEX IF EXISTS idx_gaps_status",
		"DROP INDEX IF EXISTS idx_gaps_priority_status",
		"DROP INDEX IF EXISTS idx_gaps_created_at",
		"DROP INDEX IF EXISTS idx_jobs_status",
		"DROP INDEX IF EXISTS idx_jobs_type",
		"DROP INDEX IF EXISTS idx_jobs_pair",
		"DROP INDEX IF EXISTS idx_jobs_created_at",
		"DROP INDEX IF EXISTS idx_validation_pair",
		"DROP INDEX IF EXISTS idx_validation_timestamp",
		"DROP INDEX IF EXISTS idx_validation_severity",
		"DROP INDEX IF EXISTS idx_pairs_exchange",
		"DROP INDEX IF EXISTS idx_pairs_active",
	}

	for _, indexQuery := range indexes {
		if _, err := db.ExecContext(ctx, indexQuery); err != nil {
			return fmt.Errorf("failed to drop index: %w", err)
		}
	}

	return nil
}

// MigrationStatus represents the current state of database migrations
type MigrationStatus struct {
	CurrentVersion      int                 `json:"current_version"`
	LatestVersion       int                 `json:"latest_version"`
	AppliedMigrations   []AppliedMigration  `json:"applied_migrations"`
	PendingMigrations   int                 `json:"pending_migrations"`
	TotalMigrations     int                 `json:"total_migrations"`
	DatabaseInitialized bool                `json:"database_initialized"`
}

// AppliedMigration represents a migration that has been successfully applied
type AppliedMigration struct {
	Version       int           `json:"version"`
	Description   string        `json:"description"`
	AppliedAt     time.Time     `json:"applied_at"`
	ExecutionTime time.Duration `json:"execution_time"`
}