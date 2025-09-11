// Storage adapter interface contracts for OHLCV data persistence

package contracts

import (
    "context"
    "fmt"
    "time"
)

// CandleStorer handles OHLCV candle storage operations
type CandleStorer interface {
    Store(ctx context.Context, candles []Candle) error
    StoreBatch(ctx context.Context, candles []Candle) error
}

// CandleReader handles OHLCV candle retrieval operations
type CandleReader interface {
    Query(ctx context.Context, req QueryRequest) (*QueryResponse, error)
    GetLatest(ctx context.Context, pair string, interval string) (*Candle, error)
}

// GapStorage manages data gap tracking
type GapStorage interface {
    StoreGap(ctx context.Context, gap Gap) error
    GetGaps(ctx context.Context, pair string, interval string) ([]Gap, error)
    MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error
    DeleteGap(ctx context.Context, gapID string) error
}

// StorageManager handles storage lifecycle
type StorageManager interface {
    Initialize(ctx context.Context) error
    Close() error
    Migrate(ctx context.Context, version int) error
    GetStats(ctx context.Context) (*StorageStats, error)
    HealthChecker
}

// CandleStorage combines all candle operations
type CandleStorage interface {
    CandleStorer
    CandleReader
}

// FullStorage combines all storage capabilities
type FullStorage interface {
    CandleStorage
    GapStorage
    StorageManager
}

// Request/Response types
type QueryRequest struct {
    Pair      string
    Start     time.Time
    End       time.Time
    Interval  string
    Limit     int
    Offset    int
    OrderBy   string // "timestamp_asc", "timestamp_desc"
}

type QueryResponse struct {
    Candles      []Candle
    Total        int
    HasMore      bool
    NextOffset   int
    QueryTime    time.Duration
}

type Gap struct {
    ID        string
    Pair      string  
    Interval  string
    StartTime time.Time
    EndTime   time.Time
    Status    string // "detected", "filling", "filled", "permanent"
    CreatedAt time.Time
    FilledAt  *time.Time
}

type StorageStats struct {
    TotalCandles     int64
    TotalPairs       int
    EarliestData     time.Time
    LatestData       time.Time
    StorageSize      int64 // bytes
    IndexSize        int64 // bytes
    QueryPerformance map[string]time.Duration
}

// Error types
type StorageError struct {
    Operation string
    Table     string
    Query     string
    Err       error
}

func (e *StorageError) Error() string {
    return fmt.Sprintf("storage operation %s failed: %v", e.Operation, e.Err)
}

func (e *StorageError) Unwrap() error {
    return e.Err
}