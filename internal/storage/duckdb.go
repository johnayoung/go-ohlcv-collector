// Package storage provides DuckDB-based storage implementation for OHLCV data.
// This implementation uses the DuckDB Appender API for high-performance bulk inserts
// and leverages DuckDB's analytical query capabilities for fast time-series operations.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/shopspring/decimal"
)

// DuckDBStorage implements the FullStorage interface using DuckDB as the backend.
// It provides high-performance analytical queries and bulk insert capabilities
// specifically optimized for OHLCV time-series data.
type DuckDBStorage struct {
	db      *sql.DB
	dbPath  string
	logger  *slog.Logger
	mu      sync.RWMutex
	stats   *StorageStats
	statsMu sync.RWMutex

	// Performance tracking
	queryTimes map[string][]time.Duration
	queryMu    sync.RWMutex
}

// NewDuckDBStorage creates a new DuckDB storage instance.
// The dbPath can be ":memory:" for in-memory database or a file path for persistent storage.
func NewDuckDBStorage(dbPath string, logger *slog.Logger) (*DuckDBStorage, error) {
	if logger == nil {
		logger = slog.Default()
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, NewStorageError("open", "", "", fmt.Errorf("failed to open DuckDB database: %w", err))
	}

	// Configure connection pool for single writer pattern as recommended for DuckDB
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // connections live forever

	storage := &DuckDBStorage{
		db:         db,
		dbPath:     dbPath,
		logger:     logger,
		queryTimes: make(map[string][]time.Duration),
		stats: &StorageStats{
			QueryPerformance: make(map[string]time.Duration),
		},
	}

	return storage, nil
}

// Initialize implements StorageManager.Initialize
// Creates the required schema including tables, indexes, and optimizations for analytical queries.
func (d *DuckDBStorage) Initialize(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.logger.Info("initializing DuckDB storage", "db_path", d.dbPath)

	// Enable DuckDB extensions for better performance
	if err := d.enableExtensions(ctx); err != nil {
		return NewStorageError("initialize", "", "", fmt.Errorf("failed to enable extensions: %w", err))
	}

	// Create candles table with optimized schema
	if err := d.createCandlesTable(ctx); err != nil {
		return NewStorageError("initialize", "candles", "", fmt.Errorf("failed to create candles table: %w", err))
	}

	// Create gaps table
	if err := d.createGapsTable(ctx); err != nil {
		return NewStorageError("initialize", "gaps", "", fmt.Errorf("failed to create gaps table: %w", err))
	}

	// Create indexes for optimal query performance
	if err := d.createIndexes(ctx); err != nil {
		return NewStorageError("initialize", "", "", fmt.Errorf("failed to create indexes: %w", err))
	}

	// Initialize statistics
	if err := d.updateStats(ctx); err != nil {
		d.logger.Warn("failed to initialize statistics", "error", err)
	}

	d.logger.Info("DuckDB storage initialized successfully")
	return nil
}

// enableExtensions enables DuckDB extensions for better performance
func (d *DuckDBStorage) enableExtensions(ctx context.Context) error {
	// Install and load parquet extension for better compression
	extensions := []string{
		"INSTALL parquet",
		"LOAD parquet",
	}

	for _, ext := range extensions {
		if _, err := d.db.ExecContext(ctx, ext); err != nil {
			d.logger.Warn("failed to enable extension", "extension", ext, "error", err)
			// Continue - extensions are optional
		}
	}

	// Configure DuckDB for optimal performance
	configs := []string{
		"SET memory_limit = '1GB'",
		"SET threads = 4",
		"SET enable_progress_bar = false",
	}

	for _, config := range configs {
		if _, err := d.db.ExecContext(ctx, config); err != nil {
			d.logger.Warn("failed to set configuration", "config", config, "error", err)
		}
	}

	return nil
}

// createCandlesTable creates the optimized candles table with proper data types and partitioning
func (d *DuckDBStorage) createCandlesTable(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS candles (
		timestamp TIMESTAMPTZ NOT NULL,
		open DOUBLE NOT NULL,
		high DOUBLE NOT NULL,
		low DOUBLE NOT NULL,
		close DOUBLE NOT NULL,
		volume DOUBLE NOT NULL,
		pair VARCHAR NOT NULL,
		interval VARCHAR NOT NULL,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT candles_pk PRIMARY KEY (pair, interval, timestamp),
		CONSTRAINT candles_ohlc_valid CHECK (high >= open AND high >= close AND low <= open AND low <= close),
		CONSTRAINT candles_prices_positive CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
		CONSTRAINT candles_volume_non_negative CHECK (volume >= 0)
	)`

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// createGapsTable creates the gaps tracking table
func (d *DuckDBStorage) createGapsTable(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS gaps (
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
	)`

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// createIndexes creates optimized indexes for analytical queries
func (d *DuckDBStorage) createIndexes(ctx context.Context) error {
	indexes := []string{
		// Candles indexes for fast time-series queries
		"CREATE INDEX IF NOT EXISTS idx_candles_pair_interval ON candles (pair, interval)",
		"CREATE INDEX IF NOT EXISTS idx_candles_timestamp ON candles (timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_candles_pair_timestamp ON candles (pair, timestamp)",

		// Gaps indexes for efficient gap management
		"CREATE INDEX IF NOT EXISTS idx_gaps_pair_interval ON gaps (pair, interval)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_status ON gaps (status)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_priority_status ON gaps (priority DESC, status)",
		"CREATE INDEX IF NOT EXISTS idx_gaps_created_at ON gaps (created_at)",
	}

	for _, indexQuery := range indexes {
		if _, err := d.db.ExecContext(ctx, indexQuery); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// Store implements CandleStorer.Store
// For single candle storage, delegates to StoreBatch for consistency
func (d *DuckDBStorage) Store(ctx context.Context, candles []models.Candle) error {
	return d.StoreBatch(ctx, candles)
}

// StoreBatch implements CandleStorer.StoreBatch
// Uses DuckDB Appender API for high-performance bulk inserts (10x faster than INSERT statements)
func (d *DuckDBStorage) StoreBatch(ctx context.Context, candles []models.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		d.recordQueryTime("insert_batch", time.Since(start))
	}()

	// Validate all candles before storing
	for i, candle := range candles {
		if err := candle.Validate(); err != nil {
			return NewInsertError("candles", fmt.Errorf("invalid candle at index %d: %w", i, err))
		}
	}

	// Check if database is available
	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()

	if db == nil {
		return NewInsertError("candles", fmt.Errorf("database connection is closed"))
	}

	// Get connection and create appender
	conn, err := db.Conn(ctx)
	if err != nil {
		return NewInsertError("candles", fmt.Errorf("failed to get connection: %w", err))
	}
	defer conn.Close()

	// Get the underlying driver connection
	var driverConn *duckdb.Conn
	err = conn.Raw(func(dc interface{}) error {
		var ok bool
		driverConn, ok = dc.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("underlying connection is not a DuckDB connection")
		}
		return nil
	})
	if err != nil {
		return NewInsertError("candles", fmt.Errorf("failed to get DuckDB connection: %w", err))
	}

	// Create DuckDB appender for bulk insert
	appender, err := duckdb.NewAppenderFromConn(driverConn, "", "candles")
	if err != nil {
		return NewInsertError("candles", fmt.Errorf("failed to create appender: %w", err))
	}
	defer appender.Close()

	// Append all candles using the high-performance appender API
	for _, candle := range candles {
		if err := d.appendCandle(appender, candle); err != nil {
			return NewInsertError("candles", fmt.Errorf("failed to append candle %s: %w", candle.String(), err))
		}
	}

	// Flush the appender to commit all inserts
	if err := appender.Flush(); err != nil {
		return NewInsertError("candles", fmt.Errorf("failed to flush appender: %w", err))
	}

	d.logger.Debug("stored candles batch",
		"count", len(candles),
		"duration", time.Since(start),
		"rate_per_sec", float64(len(candles))/time.Since(start).Seconds())

	return nil
}

// appendCandle appends a single candle to the DuckDB appender
func (d *DuckDBStorage) appendCandle(appender *duckdb.Appender, candle models.Candle) error {
	// Parse decimal values and convert to float64 for DuckDB Appender API
	open, err := decimal.NewFromString(candle.Open)
	if err != nil {
		return fmt.Errorf("invalid open price: %w", err)
	}
	high, err := decimal.NewFromString(candle.High)
	if err != nil {
		return fmt.Errorf("invalid high price: %w", err)
	}
	low, err := decimal.NewFromString(candle.Low)
	if err != nil {
		return fmt.Errorf("invalid low price: %w", err)
	}
	close, err := decimal.NewFromString(candle.Close)
	if err != nil {
		return fmt.Errorf("invalid close price: %w", err)
	}
	volume, err := decimal.NewFromString(candle.Volume)
	if err != nil {
		return fmt.Errorf("invalid volume: %w", err)
	}

	// Convert decimals to float64 for DuckDB Appender API
	openFloat, _ := open.Float64()
	highFloat, _ := high.Float64()
	lowFloat, _ := low.Float64()
	closeFloat, _ := close.Float64()
	volumeFloat, _ := volume.Float64()

	// Append row to DuckDB
	if err := appender.AppendRow(
		candle.Timestamp,
		openFloat,
		highFloat,
		lowFloat,
		closeFloat,
		volumeFloat,
		candle.Pair,
		candle.Interval,
		time.Now().UTC(),
	); err != nil {
		return fmt.Errorf("failed to append row: %w", err)
	}

	return nil
}

// convertDecimalToString converts DuckDB decimal types to string representation
func (d *DuckDBStorage) convertDecimalToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.8f", v)
	case float32:
		return fmt.Sprintf("%.8f", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	default:
		// For DuckDB decimal types or other unknown types, convert to string
		return fmt.Sprintf("%v", v)
	}
}

// Query implements CandleReader.Query
// Provides high-performance analytical queries optimized for time-series data
func (d *DuckDBStorage) Query(ctx context.Context, req QueryRequest) (*QueryResponse, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("query", time.Since(start))
	}()

	// Build optimized query with proper indexing hints
	query, args := d.buildQuery(req)

	d.logger.Debug("executing candles query",
		"pair", req.Pair,
		"start", req.Start,
		"end", req.End,
		"limit", req.Limit,
		"offset", req.Offset)

	// Execute count query for total results
	total, err := d.getQueryCount(ctx, req)
	if err != nil {
		return nil, NewQueryError("candles", query, fmt.Errorf("failed to get count: %w", err))
	}

	// Execute main query
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, NewQueryError("candles", query, fmt.Errorf("failed to execute query: %w", err))
	}
	defer rows.Close()

	// Scan results efficiently
	candles := make([]models.Candle, 0, req.Limit)
	for rows.Next() {
		var candle models.Candle
		var createdAt time.Time
		var open, high, low, close, volume interface{}

		if err := rows.Scan(
			&candle.Timestamp,
			&open,
			&high,
			&low,
			&close,
			&volume,
			&candle.Pair,
			&candle.Interval,
			&createdAt,
		); err != nil {
			return nil, NewQueryError("candles", query, fmt.Errorf("failed to scan row: %w", err))
		}

		// Convert DuckDB decimal types to string
		candle.Open = d.convertDecimalToString(open)
		candle.High = d.convertDecimalToString(high)
		candle.Low = d.convertDecimalToString(low)
		candle.Close = d.convertDecimalToString(close)
		candle.Volume = d.convertDecimalToString(volume)

		candles = append(candles, candle)
	}

	if err := rows.Err(); err != nil {
		return nil, NewQueryError("candles", query, fmt.Errorf("row iteration error: %w", err))
	}

	// Calculate pagination metadata
	hasMore := req.Offset+len(candles) < total
	nextOffset := req.Offset + len(candles)

	queryTime := time.Since(start)
	d.logger.Debug("query completed",
		"duration", queryTime,
		"results", len(candles),
		"total", total,
		"rate_per_ms", float64(len(candles))/float64(queryTime.Nanoseconds()/1000000))

	return &QueryResponse{
		Candles:    candles,
		Total:      total,
		HasMore:    hasMore,
		NextOffset: nextOffset,
		QueryTime:  queryTime,
	}, nil
}

// buildQuery constructs optimized SQL query based on request parameters
func (d *DuckDBStorage) buildQuery(req QueryRequest) (string, []interface{}) {
	var conditions []string
	var args []interface{}
	argPos := 1

	// Base query with proper column selection
	query := `SELECT timestamp, open, high, low, close, volume, pair, interval, created_at FROM candles`

	// Add WHERE conditions for optimal index usage
	if req.Pair != "" {
		conditions = append(conditions, fmt.Sprintf("pair = $%d", argPos))
		args = append(args, req.Pair)
		argPos++
	}

	if req.Interval != "" {
		conditions = append(conditions, fmt.Sprintf("interval = $%d", argPos))
		args = append(args, req.Interval)
		argPos++
	}

	if !req.Start.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argPos))
		args = append(args, req.Start)
		argPos++
	}

	if !req.End.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp < $%d", argPos))
		args = append(args, req.End)
		argPos++
	}

	// Add WHERE clause if conditions exist
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	// Add ORDER BY for consistent results and optimal index usage
	orderBy := "timestamp ASC"
	if req.OrderBy == "timestamp_desc" {
		orderBy = "timestamp DESC"
	}
	query += " ORDER BY " + orderBy

	// Add LIMIT and OFFSET for pagination
	if req.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, req.Limit)
		argPos++
	}

	if req.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argPos)
		args = append(args, req.Offset)
	}

	return query, args
}

// getQueryCount executes a count query to get total results
func (d *DuckDBStorage) getQueryCount(ctx context.Context, req QueryRequest) (int, error) {
	var conditions []string
	var args []interface{}
	argPos := 1

	query := "SELECT COUNT(*) FROM candles"

	if req.Pair != "" {
		conditions = append(conditions, fmt.Sprintf("pair = $%d", argPos))
		args = append(args, req.Pair)
		argPos++
	}

	if req.Interval != "" {
		conditions = append(conditions, fmt.Sprintf("interval = $%d", argPos))
		args = append(args, req.Interval)
		argPos++
	}

	if !req.Start.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argPos))
		args = append(args, req.Start)
		argPos++
	}

	if !req.End.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp < $%d", argPos))
		args = append(args, req.End)
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int
	err := d.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// GetLatest implements CandleReader.GetLatest
// Optimized query for single latest candle lookup
func (d *DuckDBStorage) GetLatest(ctx context.Context, pair string, interval string) (*models.Candle, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_latest", time.Since(start))
	}()

	query := `
		SELECT timestamp, open, high, low, close, volume, pair, interval, created_at
		FROM candles 
		WHERE pair = $1 AND interval = $2 
		ORDER BY timestamp DESC 
		LIMIT 1`

	var candle models.Candle
	var createdAt time.Time
	var open, high, low, close, volume interface{}

	err := d.db.QueryRowContext(ctx, query, pair, interval).Scan(
		&candle.Timestamp,
		&open,
		&high,
		&low,
		&close,
		&volume,
		&candle.Pair,
		&candle.Interval,
		&createdAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, NewQueryError("candles", query, fmt.Errorf("failed to get latest candle: %w", err))
	}

	// Convert DuckDB decimal types to string
	candle.Open = d.convertDecimalToString(open)
	candle.High = d.convertDecimalToString(high)
	candle.Low = d.convertDecimalToString(low)
	candle.Close = d.convertDecimalToString(close)
	candle.Volume = d.convertDecimalToString(volume)

	return &candle, nil
}

// StoreGap implements GapStorage.StoreGap
func (d *DuckDBStorage) StoreGap(ctx context.Context, gap models.Gap) error {
	start := time.Now()
	defer func() {
		d.recordQueryTime("store_gap", time.Since(start))
	}()

	if err := gap.Validate(); err != nil {
		return NewInsertError("gaps", fmt.Errorf("invalid gap: %w", err))
	}

	query := `
		INSERT INTO gaps (id, pair, start_time, end_time, interval, status, created_at, 
		                  filled_at, priority, attempts, last_attempt_at, error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	_, err := d.db.ExecContext(ctx, query,
		gap.ID,
		gap.Pair,
		gap.StartTime,
		gap.EndTime,
		gap.Interval,
		string(gap.Status),
		gap.CreatedAt,
		gap.FilledAt,
		int(gap.Priority),
		gap.Attempts,
		gap.LastAttemptAt,
		gap.ErrorMessage,
	)

	if err != nil {
		return NewInsertError("gaps", fmt.Errorf("failed to store gap %s: %w", gap.ID, err))
	}

	return nil
}

// GetGaps implements GapStorage.GetGaps
func (d *DuckDBStorage) GetGaps(ctx context.Context, pair string, interval string) ([]models.Gap, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_gaps", time.Since(start))
	}()

	query := `
		SELECT id, pair, start_time, end_time, interval, status, created_at,
		       filled_at, priority, attempts, last_attempt_at, error_message
		FROM gaps 
		WHERE pair = $1 AND interval = $2
		ORDER BY priority DESC, created_at ASC`

	rows, err := d.db.QueryContext(ctx, query, pair, interval)
	if err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("failed to get gaps: %w", err))
	}
	defer rows.Close()

	var gaps []models.Gap
	for rows.Next() {
		var gap models.Gap
		var status string
		var priority int

		err := rows.Scan(
			&gap.ID,
			&gap.Pair,
			&gap.StartTime,
			&gap.EndTime,
			&gap.Interval,
			&status,
			&gap.CreatedAt,
			&gap.FilledAt,
			&priority,
			&gap.Attempts,
			&gap.LastAttemptAt,
			&gap.ErrorMessage,
		)
		if err != nil {
			return nil, NewQueryError("gaps", query, fmt.Errorf("failed to scan gap: %w", err))
		}

		gap.Status = models.GapStatus(status)
		gap.Priority = models.GapPriority(priority)
		gaps = append(gaps, gap)
	}

	if err := rows.Err(); err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("gap rows iteration error: %w", err))
	}

	return gaps, nil
}

// GetGapsByStatus implements GapStorage.GetGapsByStatus
func (d *DuckDBStorage) GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_gaps_by_status", time.Since(start))
	}()

	query := `
		SELECT id, pair, start_time, end_time, interval, status, created_at,
		       filled_at, priority, attempts, last_attempt_at, error_message
		FROM gaps 
		WHERE status = $1
		ORDER BY priority DESC, created_at ASC`

	rows, err := d.db.QueryContext(ctx, query, string(status))
	if err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("failed to get gaps by status: %w", err))
	}
	defer rows.Close()

	var gaps []models.Gap
	for rows.Next() {
		var gap models.Gap
		var statusStr string
		var priority int

		err := rows.Scan(
			&gap.ID,
			&gap.Pair,
			&gap.StartTime,
			&gap.EndTime,
			&gap.Interval,
			&statusStr,
			&gap.CreatedAt,
			&gap.FilledAt,
			&priority,
			&gap.Attempts,
			&gap.LastAttemptAt,
			&gap.ErrorMessage,
		)
		if err != nil {
			return nil, NewQueryError("gaps", query, fmt.Errorf("failed to scan gap: %w", err))
		}

		gap.Status = models.GapStatus(statusStr)
		gap.Priority = models.GapPriority(priority)
		gaps = append(gaps, gap)
	}

	if err := rows.Err(); err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("gap rows iteration error: %w", err))
	}

	return gaps, nil
}

// GetGapByID implements GapStorage.GetGapByID
func (d *DuckDBStorage) GetGapByID(ctx context.Context, gapID string) (*models.Gap, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_gap_by_id", time.Since(start))
	}()

	query := `
		SELECT id, pair, start_time, end_time, interval, status, created_at,
		       filled_at, priority, attempts, last_attempt_at, error_message
		FROM gaps 
		WHERE id = $1`

	var gap models.Gap
	var status string
	var priority int

	err := d.db.QueryRowContext(ctx, query, gapID).Scan(
		&gap.ID,
		&gap.Pair,
		&gap.StartTime,
		&gap.EndTime,
		&gap.Interval,
		&status,
		&gap.CreatedAt,
		&gap.FilledAt,
		&priority,
		&gap.Attempts,
		&gap.LastAttemptAt,
		&gap.ErrorMessage,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("failed to get gap by ID: %w", err))
	}

	gap.Status = models.GapStatus(status)
	gap.Priority = models.GapPriority(priority)

	return &gap, nil
}

// GetGapsByStatus implements GapStorage.GetGapsByStatus
func (d *DuckDBStorage) GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_gaps_by_status", time.Since(start))
	}()

	query := `
		SELECT id, pair, start_time, end_time, interval, status, created_at,
		       filled_at, priority, attempts, last_attempt_at, error_message
		FROM gaps 
		WHERE status = $1
		ORDER BY priority DESC, created_at ASC`

	rows, err := d.db.QueryContext(ctx, query, string(status))
	if err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("failed to get gaps by status: %w", err))
	}
	defer rows.Close()

	var gaps []models.Gap
	for rows.Next() {
		var gap models.Gap
		var statusStr string
		var priority int

		err := rows.Scan(
			&gap.ID,
			&gap.Pair,
			&gap.StartTime,
			&gap.EndTime,
			&gap.Interval,
			&statusStr,
			&gap.CreatedAt,
			&gap.FilledAt,
			&priority,
			&gap.Attempts,
			&gap.LastAttemptAt,
			&gap.ErrorMessage,
		)
		if err != nil {
			return nil, NewQueryError("gaps", query, fmt.Errorf("failed to scan gap: %w", err))
		}

		gap.Status = models.GapStatus(statusStr)
		gap.Priority = models.GapPriority(priority)
		gaps = append(gaps, gap)
	}

	if err := rows.Err(); err != nil {
		return nil, NewQueryError("gaps", query, fmt.Errorf("gap rows iteration error: %w", err))
	}

	return gaps, nil
}

// MarkGapFilled implements GapStorage.MarkGapFilled
func (d *DuckDBStorage) MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error {
	start := time.Now()
	defer func() {
		d.recordQueryTime("mark_gap_filled", time.Since(start))
	}()

	query := `
		UPDATE gaps 
		SET status = 'filled', filled_at = $2, error_message = ''
		WHERE id = $1 AND status = 'filling'`

	result, err := d.db.ExecContext(ctx, query, gapID, filledAt)
	if err != nil {
		return NewUpdateError("gaps", fmt.Errorf("failed to mark gap filled: %w", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return NewUpdateError("gaps", fmt.Errorf("failed to get rows affected: %w", err))
	}

	if rowsAffected == 0 {
		return NewUpdateError("gaps", fmt.Errorf("gap %s not found or not in filling status", gapID))
	}

	return nil
}

// DeleteGap implements GapStorage.DeleteGap
func (d *DuckDBStorage) DeleteGap(ctx context.Context, gapID string) error {
	start := time.Now()
	defer func() {
		d.recordQueryTime("delete_gap", time.Since(start))
	}()

	query := "DELETE FROM gaps WHERE id = $1"

	result, err := d.db.ExecContext(ctx, query, gapID)
	if err != nil {
		return NewDeleteError("gaps", fmt.Errorf("failed to delete gap: %w", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return NewDeleteError("gaps", fmt.Errorf("failed to get rows affected: %w", err))
	}

	if rowsAffected == 0 {
		return NewDeleteError("gaps", fmt.Errorf("gap %s not found", gapID))
	}

	return nil
}

// Close implements StorageManager.Close
// Gracefully shuts down the DuckDB connection
func (d *DuckDBStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		d.logger.Info("closing DuckDB storage")
		if err := d.db.Close(); err != nil {
			return NewStorageError("close", "", "", fmt.Errorf("failed to close database: %w", err))
		}
		d.db = nil
	}

	return nil
}

// Migrate implements StorageManager.Migrate
// Handles schema migrations for version evolution
func (d *DuckDBStorage) Migrate(ctx context.Context, version int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.logger.Info("running migration", "version", version)

	// Create migrations table if it doesn't exist
	createMigrationsTable := `
		CREATE TABLE IF NOT EXISTS migrations (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`

	if _, err := d.db.ExecContext(ctx, createMigrationsTable); err != nil {
		return NewStorageError("migrate", "migrations", createMigrationsTable, err)
	}

	// Check if migration already applied
	var count int
	checkQuery := "SELECT COUNT(*) FROM migrations WHERE version = $1"
	if err := d.db.QueryRowContext(ctx, checkQuery, version).Scan(&count); err != nil {
		return NewStorageError("migrate", "migrations", checkQuery, err)
	}

	if count > 0 {
		d.logger.Info("migration already applied", "version", version)
		return nil
	}

	// Apply migration based on version
	if err := d.applyMigration(ctx, version); err != nil {
		return NewStorageError("migrate", "", "", fmt.Errorf("failed to apply migration %d: %w", version, err))
	}

	// Record successful migration
	insertQuery := "INSERT INTO migrations (version) VALUES ($1)"
	if _, err := d.db.ExecContext(ctx, insertQuery, version); err != nil {
		return NewStorageError("migrate", "migrations", insertQuery, err)
	}

	d.logger.Info("migration applied successfully", "version", version)
	return nil
}

// applyMigration applies specific migration based on version number
func (d *DuckDBStorage) applyMigration(ctx context.Context, version int) error {
	switch version {
	case 1:
		// Initial schema - already handled in Initialize()
		return nil
	case 2:
		// Example: Add index for better performance
		_, err := d.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_candles_volume ON candles (volume)")
		return err
	default:
		return fmt.Errorf("unknown migration version: %d", version)
	}
}

// GetStats implements StorageManager.GetStats
// Provides comprehensive storage statistics for monitoring
func (d *DuckDBStorage) GetStats(ctx context.Context) (*StorageStats, error) {
	start := time.Now()
	defer func() {
		d.recordQueryTime("get_stats", time.Since(start))
	}()

	if err := d.updateStats(ctx); err != nil {
		return nil, NewStorageError("stats", "", "", fmt.Errorf("failed to update stats: %w", err))
	}

	d.statsMu.RLock()
	defer d.statsMu.RUnlock()

	// Create a copy of stats to avoid race conditions
	stats := &StorageStats{
		TotalCandles:     d.stats.TotalCandles,
		TotalPairs:       d.stats.TotalPairs,
		EarliestData:     d.stats.EarliestData,
		LatestData:       d.stats.LatestData,
		StorageSize:      d.stats.StorageSize,
		IndexSize:        d.stats.IndexSize,
		QueryPerformance: make(map[string]time.Duration),
	}

	// Copy query performance metrics
	for k, v := range d.stats.QueryPerformance {
		stats.QueryPerformance[k] = v
	}

	return stats, nil
}

// updateStats refreshes the storage statistics
func (d *DuckDBStorage) updateStats(ctx context.Context) error {
	// Get candle statistics
	var totalCandles int64
	var totalPairs int
	var earliest, latest time.Time

	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM candles").Scan(&totalCandles); err != nil {
		return fmt.Errorf("failed to get total candles: %w", err)
	}

	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT pair) FROM candles").Scan(&totalPairs); err != nil {
		return fmt.Errorf("failed to get total pairs: %w", err)
	}

	// Get time range (handle empty table case)
	if totalCandles > 0 {
		if err := d.db.QueryRowContext(ctx, "SELECT MIN(timestamp), MAX(timestamp) FROM candles").Scan(&earliest, &latest); err != nil {
			return fmt.Errorf("failed to get time range: %w", err)
		}
	}

	// Get storage size information
	var storageSize, indexSize int64
	if d.dbPath != ":memory:" {
		// For file-based databases, we could get file size
		// For now, use approximate calculation based on record count
		avgRecordSize := int64(128) // Approximate bytes per OHLCV record
		storageSize = totalCandles * avgRecordSize
		indexSize = storageSize / 10 // Rough estimate of index overhead
	}

	// Update query performance metrics
	d.queryMu.RLock()
	queryPerformance := make(map[string]time.Duration)
	for operation, times := range d.queryTimes {
		if len(times) > 0 {
			var total time.Duration
			for _, t := range times {
				total += t
			}
			queryPerformance[operation] = total / time.Duration(len(times))
		}
	}
	d.queryMu.RUnlock()

	// Update stats atomically
	d.statsMu.Lock()
	d.stats.TotalCandles = totalCandles
	d.stats.TotalPairs = totalPairs
	d.stats.EarliestData = earliest
	d.stats.LatestData = latest
	d.stats.StorageSize = storageSize
	d.stats.IndexSize = indexSize
	d.stats.QueryPerformance = queryPerformance
	d.statsMu.Unlock()

	return nil
}

// HealthCheck implements HealthChecker.HealthCheck
// Performs a lightweight operation to verify database connectivity
func (d *DuckDBStorage) HealthCheck(ctx context.Context) error {
	start := time.Now()
	defer func() {
		d.recordQueryTime("health_check", time.Since(start))
	}()

	d.mu.RLock()
	db := d.db
	d.mu.RUnlock()

	if db == nil {
		return NewStorageError("health_check", "", "", fmt.Errorf("database health check failed: database connection is closed"))
	}

	// Simple query to verify database is accessible
	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return NewStorageError("health_check", "", "SELECT 1", fmt.Errorf("database health check failed: %w", err))
	}

	if result != 1 {
		return NewStorageError("health_check", "", "SELECT 1", fmt.Errorf("unexpected health check result: %d", result))
	}

	return nil
}

// recordQueryTime tracks query performance for monitoring
func (d *DuckDBStorage) recordQueryTime(operation string, duration time.Duration) {
	d.queryMu.Lock()
	defer d.queryMu.Unlock()

	if _, exists := d.queryTimes[operation]; !exists {
		d.queryTimes[operation] = make([]time.Duration, 0, 100)
	}

	// Keep only last 100 measurements to prevent memory growth
	times := d.queryTimes[operation]
	if len(times) >= 100 {
		times = times[1:]
	}

	d.queryTimes[operation] = append(times, duration)
}

// Compile-time interface compliance check
var (
	_ FullStorage    = (*DuckDBStorage)(nil)
	_ CandleStorer   = (*DuckDBStorage)(nil)
	_ CandleReader   = (*DuckDBStorage)(nil)
	_ GapStorage     = (*DuckDBStorage)(nil)
	_ StorageManager = (*DuckDBStorage)(nil)
	_ HealthChecker  = (*DuckDBStorage)(nil)
)
