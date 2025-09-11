# Implementation Guide: OHLCV Data Collector

## File-by-File Implementation Guidance

This document maps research findings to specific implementation files and provides detailed guidance for each component.

### Core Data Structures

#### `internal/models/candle.go`
**Research Reference**: [Data Model](./data-model.md#ohlcv-candle), [Interface Design](./research.md#interface-design)

```go
package models

import (
    "time"
    "github.com/shopspring/decimal"
)

// Candle represents OHLCV data for a trading pair at a specific time interval
type Candle struct {
    Timestamp time.Time `json:"timestamp" db:"timestamp"`
    Open      string    `json:"open" db:"open"`           // Decimal as string for precision
    High      string    `json:"high" db:"high"`           // See research.md#decimal-math
    Low       string    `json:"low" db:"low"`
    Close     string    `json:"close" db:"close"`
    Volume    string    `json:"volume" db:"volume"`
    Pair      string    `json:"pair" db:"pair"`           // e.g., "BTC-USD"
    Exchange  string    `json:"exchange" db:"exchange"`   // e.g., "coinbase"
    Interval  string    `json:"interval" db:"interval"`   // e.g., "1d", "1h"
}

// ValidateOHLC checks logical consistency per spec FR-004
// Research Reference: research.md#ohlcv-data-validation
func (c *Candle) ValidateOHLC() []ValidationError {
    // Implementation uses validation thresholds from research:
    // - High ≥ max(Open, Close)
    // - Low ≤ min(Open, Close) 
    // - Price spike detection: 500% threshold
    // - Volume surge detection: 10x threshold
}
```

**Implementation Steps**:
1. Define struct with proper JSON/DB tags
2. Implement validation methods using decimal package
3. Add conversion methods (ToDecimal(), FromAPI(), etc.)
4. **Research Applied**: Use string storage for precision, validation rules from research.md

#### `internal/models/gap.go`
**Research Reference**: [Data Model](./data-model.md#data-gap), [Contract](./contracts/storage.go#L71-L80)

```go
// Gap represents missing periods in historical data
// Implementation matches contracts/storage.go Gap struct exactly
type Gap struct {
    ID        string     `json:"id" db:"id"`
    Pair      string     `json:"pair" db:"pair"`
    Interval  string     `json:"interval" db:"interval"`
    StartTime time.Time  `json:"start_time" db:"start_time"`
    EndTime   time.Time  `json:"end_time" db:"end_time"`
    Status    GapStatus  `json:"status" db:"status"`
    CreatedAt time.Time  `json:"created_at" db:"created_at"`
    FilledAt  *time.Time `json:"filled_at,omitempty" db:"filled_at"`
}
```

### Exchange Adapters

#### `internal/exchange/exchange.go` (Interfaces)
**Research Reference**: [Contracts](./contracts/exchange.go), [Coinbase API](./research.md#coinbase-api-integration)

```go
package exchange

// Interfaces MUST match contracts/exchange.go exactly
// Implementation applies research findings:

type CandleFetcher interface {
    // Research Reference: research.md#coinbase-api-integration
    // - Uses /api/v3/brokerage/products/{product_id}/candles
    // - Rate limit: 10 req/sec
    // - Max 300 candles per request
    FetchCandles(ctx context.Context, req FetchRequest) (*FetchResponse, error)
}
```

#### `internal/exchange/coinbase.go`
**Research Reference**: [Coinbase API Research](./research.md#coinbase-api-integration), [Exponential Backoff](./research.md#exponential-backoff), [Go Concurrency](./research.md#go-concurrency-patterns)

```go
package exchange

import (
    "context"
    "net/http"
    "time"
    "golang.org/x/time/rate"
    "github.com/cenkalti/backoff/v4"
)

type CoinbaseAdapter struct {
    client      *http.Client
    rateLimiter *rate.Limiter
    baseURL     string
}

func NewCoinbaseAdapter(config CoinbaseConfig) *CoinbaseAdapter {
    // Research Applied:
    // - Rate limiter: rate.NewLimiter(10, 1) from research.md#coinbase-api-integration
    // - HTTP client with timeout
    return &CoinbaseAdapter{
        client:      &http.Client{Timeout: 30 * time.Second},
        rateLimiter: rate.NewLimiter(10, 1), // 10 req/sec, burst 1
        baseURL:     "https://api.coinbase.com",
    }
}

func (c *CoinbaseAdapter) FetchCandles(ctx context.Context, req FetchRequest) (*FetchResponse, error) {
    // Implementation applies research findings:
    // 1. Rate limiting with rateLimiter.Wait(ctx)
    // 2. Exponential backoff with cenkalti/backoff
    // 3. Chunking for >300 candles
    // 4. Proper error handling for 429 responses
    
    // Research Reference: research.md#exponential-backoff
    b := backoff.NewExponentialBackOff()
    b.InitialInterval = 500 * time.Millisecond
    b.MaxInterval = 30 * time.Second
    b.RandomizationFactor = 0.5 // 50% jitter
    
    // Research Reference: research.md#coinbase-api-integration
    // URL: /api/v3/brokerage/products/{product_id}/candles
    // Pagination: 300 candles max per request
}
```

**Implementation Steps**:
1. Create rate limiter using research findings (10 req/sec)
2. Implement exponential backoff with jitter
3. Handle chunking for historical data >300 candles
4. **Research Applied**: All Coinbase API, backoff, and rate limiting research

### Storage Adapters

#### `internal/storage/storage.go` (Interfaces)
**Research Reference**: [Contracts](./contracts/storage.go), [DuckDB Research](./research.md#duckdb-go-driver)

```go
package storage

// Interfaces MUST match contracts/storage.go exactly
// No modifications - just import and implement

type CandleStorer interface {
    Store(ctx context.Context, candles []Candle) error
    StoreBatch(ctx context.Context, candles []Candle) error
}
```

#### `internal/storage/duckdb.go`
**Research Reference**: [DuckDB Go Driver](./research.md#duckdb-go-driver), [Memory Management](./research.md#memory-management)

```go
package storage

import (
    "database/sql"
    "github.com/marcboeker/go-duckdb/v2"
    _ "github.com/marcboeker/go-duckdb/v2"
)

type DuckDBStorage struct {
    db *sql.DB
}

func NewDuckDBStorage(config DuckDBConfig) (*DuckDBStorage, error) {
    // Research Applied from research.md#duckdb-go-driver:
    // - Single connection: db.SetMaxOpenConns(1)
    // - Connection string with memory limits
    // - Use Appender API for bulk inserts
    
    db, err := sql.Open("duckdb", config.DatabasePath+"?memory_limit=8GB")
    if err != nil {
        return nil, err
    }
    
    // Research Reference: research.md#duckdb-go-driver
    db.SetMaxOpenConns(1)    // DuckDB works best with single writer
    db.SetMaxIdleConns(1)
    db.SetConnMaxLifetime(0) // Connections never expire
    
    return &DuckDBStorage{db: db}, nil
}

func (d *DuckDBStorage) StoreBatch(ctx context.Context, candles []Candle) error {
    // Research Applied: Use Appender API for 10x performance improvement
    // Reference: research.md#duckdb-go-driver
    
    conn, err := d.db.Conn(ctx)
    if err != nil {
        return err
    }
    defer conn.Close()
    
    appender, err := duckdb.NewAppenderFromConn(conn, "", "ohlcv")
    if err != nil {
        return err
    }
    defer appender.Close()
    
    // Bulk insert using Appender - research shows 10x faster than INSERT
    for _, candle := range candles {
        err = appender.AppendRow(
            candle.Timestamp,
            candle.Pair,
            candle.Interval,
            candle.Open,
            candle.High,
            candle.Low,
            candle.Close,
            candle.Volume,
        )
        if err != nil {
            return err
        }
    }
    
    return appender.Flush()
}
```

**Implementation Steps**:
1. Configure single connection pool per research
2. Use Appender API for bulk inserts
3. Apply memory optimization settings
4. **Research Applied**: All DuckDB performance optimizations

### Main Collector

#### `internal/collector/collector.go`
**Research Reference**: [Go Concurrency](./research.md#go-concurrency-patterns), [Memory Management](./research.md#memory-management)

```go
package collector

import (
    "context"
    "sync"
    "golang.org/x/time/rate"
    "github.com/johnayoung/go-ohlcv-collector/internal/exchange"
    "github.com/johnayoung/go-ohlcv-collector/internal/storage"
    "github.com/johnayoung/go-ohlcv-collector/internal/models"
)

type Collector struct {
    exchange exchange.ExchangeAdapter
    storage  storage.FullStorage
    pools    *MemoryPools // Research Reference: memory-management
}

func (c *Collector) CollectHistorical(ctx context.Context, req HistoricalRequest) error {
    // Research Applied: Worker pool pattern with rate limiting
    // Reference: research.md#go-concurrency-patterns
    
    pipeline := make(chan []models.Candle, 10) // Buffered channel
    
    // Producer: respect rate limits and chunk requests
    go func() {
        defer close(pipeline)
        c.fetchWithRateLimit(ctx, req, pipeline)
    }()
    
    // Consumer: batch storage operations
    const numWorkers = 4 // Based on research findings
    var wg sync.WaitGroup
    
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for batch := range pipeline {
                // Research Applied: Memory pooling for performance
                c.processBatchWithPooling(ctx, batch)
            }
        }()
    }
    
    wg.Wait()
    return nil
}
```

**Implementation Steps**:
1. Implement worker pool pattern from research
2. Add memory pooling for high-frequency operations
3. Apply concurrent collection patterns
4. **Research Applied**: All concurrency and memory management research

#### `internal/collector/scheduler.go`  
**Research Reference**: [Time.Ticker Scheduling](./research.md#timeticker-scheduling)

```go
package collector

import (
    "context"
    "time"
)

type Scheduler struct {
    collector *Collector
}

func (s *Scheduler) StartHourlyCollection(ctx context.Context) error {
    // Research Applied: Hour boundary alignment and proper cleanup
    // Reference: research.md#timeticker-scheduling
    
    // Align to hour boundary per research findings
    now := time.Now()
    nextHour := now.Truncate(time.Hour).Add(time.Hour)
    initialDelay := nextHour.Sub(now)
    
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(initialDelay):
    }
    
    // Research Applied: Proper ticker cleanup
    ticker := time.NewTicker(time.Hour)
    defer ticker.Stop() // Essential per research findings
    
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case tickTime := <-ticker.C:
            // Apply timeout and error recovery from research
            if err := s.collectWithTimeout(ctx, tickTime); err != nil {
                // Log but continue - don't break scheduler
                continue
            }
        }
    }
}
```

**Implementation Steps**:
1. Implement hour boundary alignment
2. Add proper ticker cleanup
3. Apply timeout and error recovery patterns
4. **Research Applied**: All time.Ticker best practices

### Testing Structure

#### `test/contract/exchange_contract_test.go`
**Research Reference**: [Interface Design Testing](./research.md#interface-design), [Contract Tests](./contracts/exchange.go)

```go
package contract

import (
    "testing"
    "github.com/johnayoung/go-ohlcv-collector/internal/exchange"
)

// Contract test suite that all exchange implementations must pass
type ExchangeContractTests struct {
    NewExchange func() exchange.ExchangeAdapter
    Cleanup     func()
}

func (e *ExchangeContractTests) TestExchangeContract(t *testing.T) {
    // Test all interface methods match contracts/exchange.go
    t.Run("FetchCandles", e.testFetchCandles)
    t.Run("GetTradingPairs", e.testGetTradingPairs)
    t.Run("RateLimit", e.testRateLimit)
    t.Run("HealthCheck", e.testHealthCheck)
}
```

## Implementation Dependencies

### Build Order
Based on dependencies, implement in this order:

1. **Models** (`internal/models/`) - No dependencies
2. **Interfaces** (`internal/exchange/exchange.go`, `internal/storage/storage.go`) - Depend on models
3. **Storage Implementations** (`internal/storage/*.go`) - Depend on interfaces
4. **Exchange Implementations** (`internal/exchange/*.go`) - Depend on interfaces  
5. **Validator** (`internal/validator/`) - Depends on models
6. **Gap Detector** (`internal/gaps/`) - Depends on models, storage
7. **Collector** (`internal/collector/`) - Depends on all above
8. **CLI** (`cmd/ohlcv/`) - Depends on collector

### Research Cross-References

| Implementation File | Research Sections Applied |
|-------------------|---------------------------|
| `internal/exchange/coinbase.go` | Coinbase API, Exponential Backoff, Go Concurrency |
| `internal/storage/duckdb.go` | DuckDB Go Driver, Memory Management |
| `internal/collector/collector.go` | Go Concurrency, Memory Management |
| `internal/collector/scheduler.go` | Time.Ticker Scheduling |
| `internal/validator/validator.go` | OHLCV Data Validation |
| All interfaces | Interface Design patterns |

### Missing Implementation Details

These components need additional specification:

1. **Validator implementation** - needs validation rule specifics
2. **Gap detector algorithm** - needs gap detection logic
3. **Error handling strategy** - needs structured error types
4. **Configuration management** - needs config file structure
5. **Metrics/observability** - needs monitoring patterns

Each file now has clear guidance on which research findings to apply and specific implementation steps.