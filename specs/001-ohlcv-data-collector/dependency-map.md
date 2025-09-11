# Dependency Map: OHLCV Data Collector

## Build Dependency Chain

This document shows the exact order components must be built and their dependencies.

```
Level 1: Foundation (No dependencies)
├── go.mod                              # Module definition
├── internal/models/candle.go           # Core data structures
├── internal/models/gap.go              # Gap tracking models  
├── internal/models/job.go              # Collection job models
└── internal/models/validation.go       # Validation result models

Level 2: Interface Definitions (Depend on models)
├── internal/exchange/exchange.go       # Exchange interfaces
├── internal/storage/storage.go         # Storage interfaces  
├── internal/validator/validator.go     # Validation interfaces
└── internal/gaps/gaps.go              # Gap detection interfaces

Level 3: Core Implementations (Depend on interfaces + models)
├── internal/storage/memory.go          # In-memory storage
├── internal/storage/duckdb.go          # DuckDB storage
├── internal/exchange/coinbase.go       # Coinbase adapter
├── internal/validator/ohlcv.go         # OHLCV validation logic
└── internal/gaps/detector.go          # Gap detection logic

Level 4: Orchestration (Depend on all implementations)
├── internal/collector/collector.go     # Main collection logic
└── internal/collector/scheduler.go     # Scheduling logic

Level 5: Applications (Depend on orchestration)
└── cmd/ohlcv/main.go                  # CLI application
```

## File Dependencies Detail

### Level 1: Models (Foundation)

#### `internal/models/candle.go`
- **Dependencies**: None (pure Go stdlib)
- **Exports**: `Candle`, `ValidationError`, validation methods
- **Used by**: All other components

#### `internal/models/gap.go`  
- **Dependencies**: None
- **Exports**: `Gap`, `GapStatus` enum
- **Used by**: Storage, gap detector, collector

#### `internal/models/job.go`
- **Dependencies**: None  
- **Exports**: `CollectionJob`, `JobStatus` enum
- **Used by**: Collector, scheduler

### Level 2: Interface Definitions

#### `internal/exchange/exchange.go`
- **Dependencies**: `internal/models`
- **Exports**: `CandleFetcher`, `ExchangeAdapter`, request/response types
- **Implemented by**: `coinbase.go`

#### `internal/storage/storage.go`
- **Dependencies**: `internal/models`
- **Exports**: `CandleStorage`, `GapStorage`, `StorageManager`
- **Implemented by**: `memory.go`, `duckdb.go`

### Level 3: Implementations

#### `internal/exchange/coinbase.go`
- **Dependencies**: 
  - `internal/exchange` (interfaces)
  - `internal/models`
  - `golang.org/x/time/rate`
  - `github.com/cenkalti/backoff/v4`
- **Exports**: `CoinbaseAdapter`
- **Research Applied**: Coinbase API, exponential backoff, rate limiting

#### `internal/storage/duckdb.go`
- **Dependencies**:
  - `internal/storage` (interfaces)
  - `internal/models` 
  - `github.com/marcboeker/go-duckdb/v2`
- **Exports**: `DuckDBStorage`
- **Research Applied**: DuckDB driver, bulk inserts, connection pooling

#### `internal/storage/memory.go`
- **Dependencies**:
  - `internal/storage` (interfaces)
  - `internal/models`
- **Exports**: `MemoryStorage`
- **Research Applied**: Memory management, concurrent access

### Level 4: Orchestration

#### `internal/collector/collector.go`
- **Dependencies**:
  - `internal/exchange` (all)
  - `internal/storage` (all)
  - `internal/validator`
  - `internal/gaps`
  - `internal/models`
- **Exports**: `Collector`
- **Research Applied**: Concurrency patterns, memory pooling, worker pools

#### `internal/collector/scheduler.go`
- **Dependencies**:
  - `internal/collector` (collector.go)
  - All transitive dependencies
- **Exports**: `Scheduler`
- **Research Applied**: time.Ticker best practices, hour alignment

### Level 5: Applications

#### `cmd/ohlcv/main.go`
- **Dependencies**: All internal packages
- **Exports**: CLI binary
- **Research Applied**: Standard Go CLI patterns

## Testing Dependencies

### Contract Tests (Independent)
```
test/contract/exchange_contract_test.go  # Tests exchange interfaces
├── Depends on: internal/exchange (interfaces only)
└── Tests: All exchange implementations

test/contract/storage_contract_test.go   # Tests storage interfaces  
├── Depends on: internal/storage (interfaces only)
└── Tests: All storage implementations
```

### Integration Tests (Full Dependencies)
```
test/integration/collector_integration_test.go
├── Depends on: All components
└── Tests: End-to-end collection flows

test/integration/gaps_integration_test.go
├── Depends on: Gap detector + storage + models
└── Tests: Gap detection and backfill
```

## Build Script (`scripts/build.sh`)

```bash
#!/bin/bash
set -e

echo "Building OHLCV Data Collector..."

# Level 1: Test models (no dependencies)
echo "Testing models..."
go test ./internal/models/...

# Level 2: Test interfaces
echo "Testing interfaces..."  
go test ./internal/exchange/exchange_test.go
go test ./internal/storage/storage_test.go

# Level 3: Test implementations
echo "Testing implementations..."
go test ./internal/storage/...
go test ./internal/exchange/...
go test ./internal/validator/...
go test ./internal/gaps/...

# Level 4: Test orchestration
echo "Testing orchestration..."
go test ./internal/collector/...

# Contract tests (verify all implementations)
echo "Running contract tests..."
go test ./test/contract/...

# Integration tests (full system)
echo "Running integration tests..."
go test ./test/integration/...

# Build CLI
echo "Building CLI..."
go build -o bin/ohlcv cmd/ohlcv/main.go

echo "Build complete!"
```

## Parallel Build Opportunities

These components can be built in parallel within each level:

### Level 3 Parallel Groups:
- **Group A**: `memory.go`, `duckdb.go` (storage implementations)
- **Group B**: `coinbase.go` (exchange implementation)  
- **Group C**: `validator.go`, `detector.go` (utility components)

### Test Parallel Groups:
- **Contract Tests**: All can run in parallel
- **Unit Tests**: All within same level can run in parallel
- **Integration Tests**: Must run sequentially (shared resources)

## Implementation Strategy

1. **Start with Level 1** (models) - provides foundation for all other work
2. **Level 2 in parallel** - interfaces can be defined simultaneously
3. **Level 3 in parallel groups** - implementations can be built by different developers
4. **Level 4 sequentially** - orchestration requires all implementations complete
5. **Level 5** - CLI built last with full system available

This dependency map ensures:
- No circular dependencies
- Clear build order
- Parallel development opportunities
- Testable at each level
- Research findings applied at the right layer