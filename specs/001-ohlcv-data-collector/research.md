# Research Findings: OHLCV Data Collector MVP

## Coinbase API Integration

**Decision**: Use Coinbase Advanced Trade API `/api/v3/brokerage/products/{product_id}/candles`
**Rationale**: Public endpoint (no auth required), 10 req/sec limit, 300 candles per request
**Alternatives considered**: Pro API (deprecated), WebSocket (complex for MVP)

**Key Implementation Details**:
- Rate limit: 10 requests/second for public endpoints
- Max 300-350 candles per request requiring chunking for historical data
- Use `golang.org/x/time/rate.NewLimiter(10, 1)` for rate limiting
- Exponential backoff with `cenkalti/backoff` library for retries

## DuckDB Go Driver

**Decision**: Use `github.com/marcboeker/go-duckdb/v2` with single connection pattern
**Rationale**: 12-35x faster than SQLite for analytics, excellent compression, handles time-series well
**Alternatives considered**: SQLite (slower analytics), PostgreSQL (overkill for MVP)

**Key Implementation Details**:
- Use Appender API for bulk inserts (10x faster than INSERT statements)
- Single writer connection: `db.SetMaxOpenConns(1)`
- Automatic compression reduces storage by ~3x
- Optimal for analytical queries on large OHLCV datasets

## Go Concurrency Patterns

**Decision**: Worker pool pattern with `golang.org/x/time/rate` for API rate limiting
**Rationale**: Balances throughput with API compliance, prevents goroutine leaks
**Alternatives considered**: Semaphore approach (less intuitive), simple rate limiting (lower throughput)

**Key Implementation Details**:
- Channel-based worker pool with buffered channels
- Context propagation for graceful cancellation
- Error aggregation with retry classification

## Exponential Backoff

**Decision**: Use `cenkalti/backoff` library with jitter
**Rationale**: Mature, well-tested, handles context cancellation properly
**Alternatives considered**: Manual implementation (error-prone), `hashicorp/go-retryablehttp` (HTTP-specific)

**Key Implementation Details**:
- Initial interval: 500ms, max interval: 30s
- 50% jitter to prevent thundering herd
- Respect Coinbase Retry-After headers

## Time.Ticker Scheduling

**Decision**: Standard `time.Ticker` with hour boundary alignment
**Rationale**: Simple, reliable, adequate for hourly collection requirement
**Alternatives considered**: Cron library (overkill), custom scheduler (unnecessary complexity)

**Key Implementation Details**:
- Align to hour boundaries: `time.Truncate(time.Hour).Add(time.Hour)`
- Proper cleanup with `defer ticker.Stop()`
- Context-based cancellation support

## OHLCV Data Validation

**Decision**: Multi-layer validation with configurable thresholds
**Rationale**: Catches data quality issues early, prevents bad data propagation
**Alternatives considered**: Basic validation only (insufficient), External validation service (complex)

**Key Implementation Details**:
- Logical consistency: high ≥ max(open,close), low ≤ min(open,close)
- Anomaly detection: 500% price spike, 10x volume surge thresholds
- Timestamp sequence validation with drift tolerance

## Memory Management

**Decision**: Streaming processing with `sync.Pool` for object reuse
**Rationale**: Constant memory usage, handles millions of records efficiently
**Alternatives considered**: Batch processing only (memory spikes), No pooling (GC pressure)

**Key Implementation Details**:
- Process in 10K record batches to maintain <100MB memory usage
- Use `sync.Pool` for frequent allocations (candles, slices, buffers)
- GOGC=300 tuning for reduced GC frequency

## Interface Design

**Decision**: Segregated interfaces following ISP (Interface Segregation Principle)
**Rationale**: Clean adapter patterns, easier testing, better composability
**Alternatives considered**: Monolithic interfaces (violates ISP), Struct embedding (less flexible)

**Key Implementation Details**:
- Separate `CandleFetcher`, `CandleStorer`, `GapDetector` interfaces
- Context-first method signatures
- Structured error types with retry classification

## Technology Stack Summary

- **Language**: Go 1.21+
- **Module**: Standard Go modules with `internal/` packages for encapsulation
- **HTTP Client**: Standard `net/http` with rate limiting
- **Database**: DuckDB with `github.com/marcboeker/go-duckdb/v2`
- **Concurrency**: Standard library with worker pools
- **Retry Logic**: `github.com/cenkalti/backoff/v4`
- **Rate Limiting**: `golang.org/x/time/rate`
- **Decimal Math**: `github.com/shopspring/decimal` for financial precision
- **Testing**: Standard `testing` package with contract tests
- **Logging**: Structured logging with `log/slog`

## Go Module Structure

**Decision**: Use `internal/` packages for implementation, `pkg/` for optional public API
**Rationale**: Follows Go project layout standards, prevents external imports of internal code
**Alternatives considered**: Flat structure (doesn't scale), `pkg/` only (exposes internals)

**Key Implementation Details**:
- Module name: `github.com/johnayoung/go-ohlcv-collector`
- Internal packages: `internal/{collector,exchange,storage,gaps,validator,models}`
- CLI in `cmd/ohlcv/main.go` following Go conventions
- Tests co-located with implementation files (`*_test.go`)

All decisions prioritize simplicity, performance, and maintainability while meeting the 99.9% data completeness and <100ms query performance requirements.