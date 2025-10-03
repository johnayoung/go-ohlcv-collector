# Go OHLCV Collector Development Guidelines

Auto-generated from project structure and conventions. Last updated: 2025-10-03

## Active Technologies

- **Language**: Go 1.24+
- **Database**: DuckDB (via marcboeker/go-duckdb/v2)
- **Testing**: testify (stretchr/testify)
- **Decimal Arithmetic**: shopspring/decimal
- **Logging**: structured logging with slog
- **Build Tool**: Go modules

## Project Structure

```
cmd/ohlcv/              # CLI application entry point
internal/
  ├── collector/        # Data collection orchestration and scheduling
  ├── config/           # Configuration management
  ├── contracts/        # Interface definitions and contracts
  ├── errors/           # Custom error types and handling
  ├── exchange/         # Exchange adapter implementations (Coinbase, Mock)
  ├── gaps/             # Gap detection and backfill algorithms
  ├── logger/           # Logging utilities and structured logging
  ├── metrics/          # Performance metrics and monitoring
  ├── models/           # Core data models (Candle, Gap, etc.)
  ├── storage/          # Storage adapter implementations (DuckDB, Memory)
  └── validator/        # Data validation and anomaly detection
test/
  ├── contract/         # Interface compliance tests
  ├── integration/      # Integration tests with real APIs/storage
  └── benchmark/        # Performance benchmarks
specs/                  # Feature specifications and documentation
scripts/                # Development and automation scripts
templates/              # Project templates
```

## Commands

```bash
# Testing
go test ./...                           # Run all tests
go test ./test/contract/...             # Run contract tests only
go test ./test/integration/...          # Run integration tests
go test -bench=. ./test/benchmark/...   # Run benchmarks
go test -race ./...                     # Run tests with race detector
go test -cover ./...                    # Run tests with coverage

# Code Quality
go fmt ./...                            # Format code
go vet ./...                            # Run go vet
go mod tidy                             # Clean up dependencies
go mod verify                           # Verify dependencies

# Building
go build -o bin/ohlcv cmd/ohlcv/main.go           # Build CLI
go build -ldflags="-s -w" -o bin/ohlcv cmd/ohlcv/main.go  # Build optimized
go install github.com/johnayoung/go-ohlcv-collector/cmd/ohlcv@latest  # Install CLI

# Development
go run cmd/ohlcv/main.go --help         # Run CLI directly
go mod download                          # Download dependencies
```

## Code Style

### Go Conventions
- Follow [Effective Go](https://golang.org/doc/effective_go.html) principles
- Use `gofmt` for consistent formatting
- Prefer table-driven tests for comprehensive coverage
- Use descriptive variable names (avoid single-letter except in short loops)
- Document all exported functions, types, and packages with godoc comments
- Use interfaces for testability and dependency injection
- Handle errors explicitly - never ignore errors
- Use context.Context for cancellation and timeouts

### Error Handling
- Return errors as the last return value
- Wrap errors with context using `fmt.Errorf("context: %w", err)`
- Use custom error types in `internal/errors` package
- Log errors at appropriate levels (ERROR for critical, WARN for recoverable)
- Implement retry logic for transient failures

### Testing Guidelines
- Write tests before implementation (TDD approach)
- Contract tests verify interface implementations
- Integration tests use real dependencies (can be slow)
- Benchmark tests track performance regressions
- Use testify assertions for readable test code
- Mock external dependencies in unit tests
- Aim for 95%+ code coverage

### Project-Specific Conventions
- Use `decimal.Decimal` for all financial calculations (never float64)
- Store OHLCV data with timestamp precision to seconds
- Use ISO 8601 format for dates in APIs and logs
- Implement adapters as separate packages (exchange, storage)
- All timestamps in UTC
- Trading pairs format: "BTC-USD" (uppercase, hyphen-separated)
- Intervals: "1m", "5m", "15m", "1h", "6h", "1d"

### Package Organization
- `internal/` for implementation details (not importable by external packages)
- `internal/contracts/` for interface definitions shared across packages
- `internal/models/` for core data structures
- `cmd/` for executable commands
- `test/` for test code organized by test type

### Naming Conventions
- Interfaces: use descriptive names without "Interface" suffix (e.g., `Storage`, `ExchangeAdapter`)
- Implementations: descriptive names (e.g., `DuckDBStorage`, `CoinbaseExchange`)
- Test files: `*_test.go` in same package for unit tests, separate packages for integration
- Mock implementations: prefix with "Mock" (e.g., `MockExchange`)
- Configuration structs: suffix with "Config" (e.g., `CollectorConfig`)

### Performance Considerations
- Use batching for database operations (default 10,000 candles)
- Implement rate limiting for API calls
- Use connection pooling for database access
- Profile with pprof when optimizing
- Benchmark critical paths
- Use buffered channels for concurrent operations
- Avoid allocations in hot paths

## Recent Changes

- 001-ohlcv-data-collector: Added Go + DuckDB for OHLCV data collection with gap detection and validation

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
