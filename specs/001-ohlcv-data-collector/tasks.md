# Tasks: OHLCV Data Collector MVP

**Input**: Design documents from `/specs/001-ohlcv-data-collector/`
**Prerequisites**: plan.md ✓, research.md ✓, data-model.md ✓, contracts/ ✓

## Execution Flow (main)
```
1. Load plan.md from feature directory ✓
   → Tech stack: Go 1.21+, DuckDB, standard library
   → Structure: Go module with internal/ packages
2. Load design documents ✓:
   → data-model.md: 6 entities → model tasks
   → contracts/: 2 files → contract test tasks
   → dependency-map.md: 5-level build hierarchy
3. Generate tasks by category: ✓
4. Apply task rules: ✓
   → Different files = mark [P] for parallel
   → Tests before implementation (TDD)
5. Number tasks sequentially (T001, T002...) ✓
6. Generate dependency graph ✓
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
**Go Module Structure**: `internal/` packages, `cmd/` for CLI, `test/` for contract/integration tests

## Phase 3.1: Setup

- [ ] **T001** Initialize Go module with `go mod init github.com/johnayoung/go-ohlcv-collector`
- [ ] **T002** Create standard Go project directory structure: `cmd/`, `internal/`, `test/`, `scripts/`, `build/`
- [ ] **T003** [P] Setup dependencies: `go.mod` with DuckDB, decimal, backoff, rate limiting packages
- [ ] **T004** [P] Configure Go development tools: `.golangci.yml` linter config and `scripts/test.sh`

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**

### Contract Tests (Level 2 Dependencies)
- [ ] **T005** [P] Contract test for ExchangeAdapter interface in `test/contract/exchange_contract_test.go`
- [ ] **T006** [P] Contract test for CandleStorage interface in `test/contract/storage_contract_test.go`
- [ ] **T007** [P] Contract test for GapStorage interface in `test/contract/gap_storage_contract_test.go`

### Integration Tests (End-to-End Scenarios)
- [ ] **T008** [P] Integration test historical data collection in `test/integration/collector_integration_test.go`
- [ ] **T009** [P] Integration test scheduled collection in `test/integration/scheduler_integration_test.go`
- [ ] **T010** [P] Integration test gap detection and backfill in `test/integration/gaps_integration_test.go`
- [ ] **T011** [P] Integration test data validation and anomaly detection in `test/integration/validation_integration_test.go`

## Phase 3.3: Core Implementation (ONLY after tests are failing)

### Level 1: Foundation Models (No Dependencies)
- [ ] **T012** [P] OHLCV Candle model with validation in `internal/models/candle.go`
- [ ] **T013** [P] Data Gap model with state transitions in `internal/models/gap.go`
- [ ] **T014** [P] Collection Job model with status tracking in `internal/models/job.go`
- [ ] **T015** [P] Validation Result and Anomaly models in `internal/models/validation.go`

### Level 2: Interface Definitions (Depend on Models)
- [ ] **T016** [P] Exchange interfaces (CandleFetcher, PairProvider, RateLimitInfo) in `internal/exchange/exchange.go`
- [ ] **T017** [P] Storage interfaces (CandleStorer, CandleReader, GapStorage) in `internal/storage/storage.go`
- [ ] **T018** [P] Validation interfaces (Validator, AnomalyDetector) in `internal/validator/validator.go`
- [ ] **T019** [P] Gap detection interfaces (GapDetector, Backfiller) in `internal/gaps/gaps.go`

### Level 3: Core Implementations (Depend on Interfaces + Models)
- [ ] **T020** [P] In-memory storage implementation in `internal/storage/memory.go`
- [ ] **T021** [P] DuckDB storage with Appender API in `internal/storage/duckdb.go`
- [ ] **T022** [P] Coinbase exchange adapter with rate limiting in `internal/exchange/coinbase.go`
- [ ] **T023** [P] OHLCV data validator with anomaly detection in `internal/validator/ohlcv.go`
- [ ] **T024** [P] Gap detector with automatic backfill in `internal/gaps/detector.go`

### Level 4: Orchestration (Depend on All Implementations)
- [ ] **T025** Main collector with worker pools and memory management in `internal/collector/collector.go`
- [ ] **T026** Scheduler with time.Ticker and hour alignment in `internal/collector/scheduler.go`

### Level 5: Applications (Depend on Orchestration)
- [ ] **T027** CLI application with commands: collect, schedule, gaps, query in `cmd/ohlcv/main.go`

## Phase 3.4: Integration & Configuration

- [ ] **T028** Database migrations and table creation for DuckDB in `internal/storage/migrations.go`
- [ ] **T029** Configuration management for all adapters in `internal/config/config.go`
- [ ] **T030** Structured logging with context propagation in `internal/logger/logger.go`
- [ ] **T031** Error handling with retry classification in `internal/errors/errors.go`
- [ ] **T032** Metrics collection and health monitoring in `internal/metrics/metrics.go`

## Phase 3.5: Polish

### Unit Tests (Test Individual Components)
- [ ] **T033** [P] Unit tests for Candle validation logic in `internal/models/candle_test.go`
- [ ] **T034** [P] Unit tests for Coinbase API parsing in `internal/exchange/coinbase_test.go`
- [ ] **T035** [P] Unit tests for DuckDB queries in `internal/storage/duckdb_test.go`
- [ ] **T036** [P] Unit tests for gap detection algorithm in `internal/gaps/detector_test.go`

### Performance & Documentation
- [ ] **T037** Performance benchmarks for data collection throughput in `test/benchmark/collector_bench_test.go`
- [ ] **T038** [P] Update README.md with installation and usage instructions
- [ ] **T039** [P] Generate API documentation with godoc comments
- [ ] **T040** Memory profiling and optimization validation

### Final Validation
- [ ] **T041** Run quickstart.md scenarios to verify end-to-end functionality
- [ ] **T042** Performance validation: <100ms query response, <50MB memory usage
- [ ] **T043** Code quality: run linting, format code, remove duplication

## Dependencies

### Critical Path
- **Setup** (T001-T004) before everything
- **Contract Tests** (T005-T007) before any implementation
- **Integration Tests** (T008-T011) before implementation
- **Models** (T012-T015) before interfaces
- **Interfaces** (T016-T019) before implementations
- **Implementations** (T020-T024) before orchestration
- **Orchestration** (T025-T026) before applications

### Parallel Groups
- **Setup**: T003-T004 in parallel
- **Contract Tests**: T005-T007 in parallel
- **Integration Tests**: T008-T011 in parallel  
- **Models**: T012-T015 in parallel
- **Interfaces**: T016-T019 in parallel
- **Implementations**: T020-T024 in parallel
- **Unit Tests**: T033-T036 in parallel
- **Documentation**: T038-T039 in parallel

### Blocking Dependencies
- T025 (collector.go) depends on T020-T024 (all implementations)
- T026 (scheduler.go) depends on T025 (collector.go)
- T027 (CLI) depends on T025-T026 (orchestration)
- T028-T032 (integration) depends on core implementations
- T037-T040 (polish) depends on complete system

## Parallel Execution Examples

### Phase 3.2: Launch All Contract Tests Together
```
Task: "Contract test for ExchangeAdapter interface in test/contract/exchange_contract_test.go"
Task: "Contract test for CandleStorage interface in test/contract/storage_contract_test.go" 
Task: "Contract test for GapStorage interface in test/contract/gap_storage_contract_test.go"
```

### Phase 3.3 Level 1: Launch All Model Creation Together  
```
Task: "OHLCV Candle model with validation in internal/models/candle.go"
Task: "Data Gap model with state transitions in internal/models/gap.go"
Task: "Collection Job model with status tracking in internal/models/job.go"
Task: "Validation Result and Anomaly models in internal/models/validation.go"
```

### Phase 3.3 Level 3: Launch Core Implementations Together
```
Task: "In-memory storage implementation in internal/storage/memory.go"
Task: "DuckDB storage with Appender API in internal/storage/duckdb.go"
Task: "Coinbase exchange adapter with rate limiting in internal/exchange/coinbase.go"
Task: "OHLCV data validator with anomaly detection in internal/validator/ohlcv.go"
Task: "Gap detector with automatic backfill in internal/gaps/detector.go"
```

## Implementation References

Each task should reference the appropriate design documents:

- **Models** (T012-T015): See `data-model.md` for entity specifications
- **Storage** (T020-T021): See `research.md#duckdb-go-driver` and `implementation-guide.md#storage-adapters`
- **Exchange** (T022): See `research.md#coinbase-api-integration` and `implementation-guide.md#exchange-adapters`  
- **Collector** (T025): See `research.md#go-concurrency-patterns` and `implementation-guide.md#main-collector`
- **Scheduler** (T026): See `research.md#timeticker-scheduling` and `implementation-guide.md#scheduler`
- **CLI** (T027): See `quickstart.md` for usage patterns

## Notes

- **[P] tasks** = different files, no dependencies, can run in parallel
- **Verify tests fail** before implementing (TDD requirement)
- **Commit after each task** for clean git history
- **Follow dependency-map.md** for proper build order
- **Apply research findings** from implementation-guide.md
- Use **Go 1.21+ features** and standard library where possible
- **Memory management**: Apply sync.Pool patterns for high-frequency operations
- **Error handling**: Use structured errors with retry classification

## Task Generation Rules Applied

1. **From Contracts**: 2 contract files → 3 contract test tasks (T005-T007)
2. **From Data Model**: 4+ entities → 4 model tasks (T012-T015)  
3. **From Quickstart**: 4 user scenarios → 4 integration tests (T008-T011)
4. **From Dependency Map**: 5 levels → proper ordering and parallelization
5. **From Implementation Guide**: Specific file paths and research references

## Validation Checklist ✓

- [x] All contracts have corresponding tests (T005-T007)
- [x] All entities have model tasks (T012-T015)
- [x] All tests come before implementation (Phase 3.2 before 3.3)
- [x] Parallel tasks truly independent (different files, no shared dependencies)
- [x] Each task specifies exact file path
- [x] No task modifies same file as another [P] task
- [x] Dependencies follow dependency-map.md levels
- [x] Research findings mapped to implementation tasks