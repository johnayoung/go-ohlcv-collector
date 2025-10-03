// Package storage defines the storage layer interfaces for OHLCV data persistence.
// These interfaces provide abstractions over different storage backends (SQL, NoSQL, etc.)
// while maintaining contract compatibility and enabling dependency injection.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
)

// CandleStorer handles OHLCV candle storage operations.
// This interface supports both single candle storage and batch operations
// for efficient bulk inserts during historical data collection.
type CandleStorer interface {
	// Store persists a slice of candles to storage.
	// All candles in the slice should be validated before storage.
	// Returns an error if any candle fails to store or validation fails.
	Store(ctx context.Context, candles []models.Candle) error

	// StoreBatch performs optimized bulk storage of candles.
	// This method should be preferred for large datasets as it can
	// use database-specific bulk insert optimizations.
	// Returns an error if the batch operation fails.
	StoreBatch(ctx context.Context, candles []models.Candle) error
}

// CandleReader handles OHLCV candle retrieval operations.
// This interface provides flexible querying capabilities with pagination,
// filtering, and sorting support.
type CandleReader interface {
	// Query retrieves candles based on the provided request parameters.
	// Supports filtering by pair, time range, interval with pagination and ordering.
	// Returns a response containing the candles and metadata about the query.
	Query(ctx context.Context, req QueryRequest) (*QueryResponse, error)

	// GetLatest retrieves the most recent candle for a specific pair and interval.
	// This is optimized for single candle lookups when only the latest data is needed.
	// Returns nil if no candles exist for the given pair/interval combination.
	GetLatest(ctx context.Context, pair string, interval string) (*models.Candle, error)
}

// GapStorage manages data gap tracking and resolution.
// Gaps represent missing periods in historical data that need to be filled.
// This interface supports the complete gap lifecycle from detection to resolution.
type GapStorage interface {
	// StoreGap persists a new data gap to storage.
	// The gap should be in "detected" status when first stored.
	// Returns an error if the gap already exists or validation fails.
	StoreGap(ctx context.Context, gap models.Gap) error

	// GetGaps retrieves all gaps for a specific pair and interval.
	// Results are typically ordered by priority and creation time.
	// Returns empty slice if no gaps exist for the given criteria.
	GetGaps(ctx context.Context, pair string, interval string) ([]models.Gap, error)

	// GetGapsByStatus retrieves all gaps with a specific status.
	// This is useful for filtering gaps by their lifecycle state (detected, filling, filled, permanent).
	// Results are typically ordered by priority and creation time.
	// Returns empty slice if no gaps exist with the given status.
	GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error)

	// GetGapByID retrieves a specific gap by its unique identifier.
	// Returns nil if the gap doesn't exist.
	// This method is used for gap status updates and resolution tracking.
	GetGapByID(ctx context.Context, gapID string) (*models.Gap, error)

	// GetGapsByStatus retrieves all gaps with a specific status.
	// This method is used for filtering gaps by their lifecycle state.
	// Returns empty slice if no gaps exist with the given status.
	GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error)

	// MarkGapFilled updates a gap's status to "filled" with a timestamp.
	// This should be called after successfully collecting the missing data.
	// The gap status must be "filling" to be marked as filled.
	MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error

	// DeleteGap removes a gap from storage.
	// This should only be used for gaps that are no longer relevant
	// or were created in error. Prefer MarkGapFilled for successful resolution.
	DeleteGap(ctx context.Context, gapID string) error

	// GetGapsByStatus retrieves gaps with a specific status.
	// This is useful for filtering gaps by their lifecycle state (detected, filling, filled, permanent).
	// Returns empty slice if no gaps exist with the given status.
	GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error)
}

// StorageManager handles storage lifecycle and operational concerns.
// This interface provides initialization, cleanup, migration, and monitoring capabilities.
type StorageManager interface {
	// Initialize prepares the storage backend for operation.
	// This includes creating tables, indexes, and any required schema setup.
	// Should be idempotent and safe to call multiple times.
	Initialize(ctx context.Context) error

	// Close gracefully shuts down the storage backend.
	// This should release connections, flush pending writes, and cleanup resources.
	// After Close() is called, the storage instance should not be used.
	Close() error

	// Migrate applies schema changes for the specified version.
	// This enables database schema evolution and backward compatibility.
	// Version should be monotonically increasing integers.
	Migrate(ctx context.Context, version int) error

	// GetStats returns operational statistics about the storage backend.
	// This includes data volume, performance metrics, and storage utilization.
	// Used for monitoring and capacity planning.
	GetStats(ctx context.Context) (*StorageStats, error)

	// HealthChecker embedded interface for health monitoring
	HealthChecker
}

// HealthChecker provides health monitoring capabilities for storage backends.
// This interface enables health checks and monitoring integration.
type HealthChecker interface {
	// HealthCheck verifies that the storage backend is operational.
	// This should perform a lightweight operation to verify connectivity
	// and basic functionality without impacting performance.
	// Returns an error if the storage backend is unhealthy.
	HealthCheck(ctx context.Context) error
}

// CandleStorage combines all candle-related operations.
// This composite interface provides a unified view of candle storage and retrieval.
type CandleStorage interface {
	CandleStorer
	CandleReader
}

// FullStorage combines all storage capabilities into a single interface.
// This is the primary interface that storage implementations should implement
// to provide complete OHLCV data persistence functionality.
type FullStorage interface {
	CandleStorage
	GapStorage
	StorageManager
}

// Request/Response types for storage operations

// QueryRequest defines parameters for querying stored candles.
// Provides flexible filtering, pagination, and sorting capabilities.
type QueryRequest struct {
	// Pair is the trading pair symbol (e.g., "BTC-USD")
	Pair string

	// Start is the earliest timestamp to include in results (inclusive)
	Start time.Time

	// End is the latest timestamp to include in results (exclusive)
	End time.Time

	// Interval is the candle interval (e.g., "1h", "1d")
	Interval string

	// Limit is the maximum number of results to return (0 = no limit)
	Limit int

	// Offset is the number of results to skip for pagination
	Offset int

	// OrderBy specifies result ordering ("timestamp_asc" or "timestamp_desc")
	OrderBy string
}

// QueryResponse contains the results of a candle query operation.
// Includes both data and metadata for pagination and performance monitoring.
type QueryResponse struct {
	// Candles contains the query results
	Candles []models.Candle

	// Total is the total number of matches (before limit/offset)
	Total int

	// HasMore indicates if more results are available beyond the current page
	HasMore bool

	// NextOffset is the offset value for retrieving the next page
	NextOffset int

	// QueryTime is the duration taken to execute the query
	QueryTime time.Duration
}

// StorageStats provides operational metrics and statistics about storage.
// Used for monitoring, capacity planning, and performance optimization.
type StorageStats struct {
	// TotalCandles is the total number of candles stored
	TotalCandles int64

	// TotalPairs is the number of unique trading pairs with data
	TotalPairs int

	// EarliestData is the timestamp of the oldest candle
	EarliestData time.Time

	// LatestData is the timestamp of the newest candle
	LatestData time.Time

	// StorageSize is the total storage space used in bytes
	StorageSize int64

	// IndexSize is the storage space used by indexes in bytes
	IndexSize int64

	// QueryPerformance contains average query times by operation type
	QueryPerformance map[string]time.Duration
}

// Error types for storage operations

// StorageError represents errors that occur during storage operations.
// Provides structured error information for better error handling and debugging.
type StorageError struct {
	// Operation is the storage operation that failed (e.g., "insert", "query")
	Operation string

	// Table is the database table involved in the operation
	Table string

	// Query is the SQL query or operation details (may be empty)
	Query string

	// Err is the underlying error that caused the failure
	Err error
}

// Error implements the error interface for StorageError.
// Returns a formatted error message with operation context.
func (e *StorageError) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("storage operation %s on table %s failed: %v", e.Operation, e.Table, e.Err)
	}
	return fmt.Sprintf("storage operation %s failed: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error for error chain support.
// This enables errors.Is() and errors.As() functionality.
func (e *StorageError) Unwrap() error {
	return e.Err
}

// Common error constructors for storage operations

// NewStorageError creates a new StorageError with the provided details.
func NewStorageError(operation, table, query string, err error) *StorageError {
	return &StorageError{
		Operation: operation,
		Table:     table,
		Query:     query,
		Err:       err,
	}
}

// NewQueryError creates a StorageError specifically for query operations.
func NewQueryError(table, query string, err error) *StorageError {
	return &StorageError{
		Operation: "query",
		Table:     table,
		Query:     query,
		Err:       err,
	}
}

// NewInsertError creates a StorageError specifically for insert operations.
func NewInsertError(table string, err error) *StorageError {
	return &StorageError{
		Operation: "insert",
		Table:     table,
		Err:       err,
	}
}

// NewUpdateError creates a StorageError specifically for update operations.
func NewUpdateError(table string, err error) *StorageError {
	return &StorageError{
		Operation: "update",
		Table:     table,
		Err:       err,
	}
}

// NewDeleteError creates a StorageError specifically for delete operations.
func NewDeleteError(table string, err error) *StorageError {
	return &StorageError{
		Operation: "delete",
		Table:     table,
		Err:       err,
	}
}
