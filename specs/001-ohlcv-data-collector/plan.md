# Implementation Plan: OHLCV Data Collector MVP

**Branch**: `001-ohlcv-data-collector` | **Date**: 2025-09-11 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-ohlcv-data-collector/spec.md`

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
4. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
5. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, or `GEMINI.md` for Gemini CLI).
6. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
7. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
8. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 7. Phases 2-4 are executed by other commands:
- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary
A Go library for collecting and maintaining complete historical OHLCV (Open, High, Low, Close, Volume) cryptocurrency data with self-healing capabilities, automatic gap detection, and pluggable storage adapters. Starting with Coinbase REST API and DuckDB storage, expandable to other exchanges and databases.

## Technical Context
**Language/Version**: Go 1.21+  
**Primary Dependencies**: Go standard library (net/http, time), DuckDB Go driver  
**Storage**: In-memory (default), DuckDB (file-based), pluggable interfaces for PostgreSQL/TimescaleDB/InfluxDB  
**Testing**: Go testing package with table-driven tests  
**Target Platform**: Linux/macOS/Windows (cross-platform Go binary)
**Project Type**: single - Go library with CLI example  
**Performance Goals**: <100ms query response for 1 year of daily data, concurrent collection of 50+ trading pairs  
**Constraints**: <50MB memory for in-memory storage of 1 year daily data per pair, rate limit compliance (Coinbase: 10 req/sec)  
**Scale/Scope**: Support 100+ trading pairs, 5+ years historical data, extensible to minute-level granularity

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Simplicity**:
- Projects: 2 (library, cli example)
- Using framework directly? Yes (standard library only, no wrapper classes)
- Single data model? Yes (OHLCV candle struct shared across storage adapters)
- Avoiding patterns? Yes (interfaces for extensibility, no unnecessary abstractions)

**Architecture**:
- EVERY feature as library? Yes (core collector library, separate CLI)
- Libraries listed: 
  - ohlcv-collector: Core data collection, validation, storage interfaces
  - cli-example: Demonstration CLI using the library
- CLI per library: collector --fetch, --validate, --gaps, --backfill, --schedule
- Library docs: README.md with usage examples, godoc comments

**Testing (NON-NEGOTIABLE)**:
- RED-GREEN-Refactor cycle enforced? Yes
- Git commits show tests before implementation? Yes
- Order: Contract→Integration→E2E→Unit strictly followed? Yes
- Real dependencies used? Yes (real Coinbase API, actual DuckDB)
- Integration tests for: exchange adapters, storage adapters, gap detection
- FORBIDDEN: Implementation before test, skipping RED phase

**Observability**:
- Structured logging included? Yes (JSON logs with context)
- Frontend logs → backend? N/A (library only)
- Error context sufficient? Yes (exchange errors, validation failures, storage issues)

**Versioning**:
- Version number assigned? 0.1.0
- BUILD increments on every change? Yes
- Breaking changes handled? Semantic versioning, interface stability

## Project Structure

### Documentation (this feature)
```
specs/001-ohlcv-data-collector/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root - Go module layout)
```
# Standard Go project layout
go.mod                   # Go module definition
go.sum                   # Dependency checksums
README.md               # Project documentation
LICENSE                 # License file

# Main application
cmd/
└── ohlcv/              # CLI application
    └── main.go

# Library packages (internal to this module)
internal/
├── collector/          # Main collection orchestration
│   ├── collector.go
│   ├── collector_test.go
│   ├── scheduler.go
│   └── scheduler_test.go
├── exchange/           # Exchange adapters
│   ├── coinbase.go
│   ├── coinbase_test.go
│   ├── exchange.go     # Interface definitions
│   └── exchange_test.go
├── storage/            # Storage adapters  
│   ├── duckdb.go
│   ├── duckdb_test.go
│   ├── memory.go
│   ├── memory_test.go
│   ├── storage.go      # Interface definitions
│   └── storage_test.go
├── gaps/               # Gap detection & healing
│   ├── detector.go
│   └── detector_test.go
├── validator/          # Data validation logic
│   ├── validator.go
│   └── validator_test.go
└── models/             # Core data structures
    ├── candle.go
    ├── gap.go
    ├── job.go
    └── models_test.go

# Public API (if exposing as library for others)
pkg/                    # Only if providing public API
└── ohlcv/              # Public interfaces (optional)
    ├── client.go
    └── types.go

# Integration and contract tests
test/
├── integration/        # Integration tests
│   ├── collector_integration_test.go
│   └── storage_integration_test.go
└── contract/          # Contract tests for interfaces
    ├── exchange_contract_test.go
    └── storage_contract_test.go

# Build and deployment
build/                  # Build scripts and Dockerfiles
├── Dockerfile
└── docker-compose.yml

# Development tools
scripts/                # Build and development scripts
├── build.sh
├── test.sh
└── update-agent-context.sh
```

**Structure Decision**: Option 1 (Single project) - Go library with example CLI

## Phase 0: Outline & Research
1. **Extract unknowns from Technical Context** above:
   - Coinbase REST API rate limits and pagination strategies
   - DuckDB Go driver capabilities and performance characteristics
   - Optimal goroutine pool size for concurrent collection
   - Exponential backoff best practices for API retry logic

2. **Generate and dispatch research agents**:
   ```
   For each unknown in Technical Context:
     Task: "Research Coinbase API v3 REST endpoints for OHLCV data"
     Task: "Find DuckDB Go driver best practices for time-series data"
     Task: "Research Go concurrency patterns for rate-limited APIs"
     Task: "Find exponential backoff implementations in Go"
   ```

3. **Consolidate findings** in `research.md` using format:
   - Decision: [what was chosen]
   - Rationale: [why chosen]
   - Alternatives considered: [what else evaluated]

**Output**: research.md with all NEEDS CLARIFICATION resolved

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Extract entities from feature spec** → `data-model.md`:
   - OHLCV Candle: timestamp, open, high, low, close, volume, pair, exchange
   - Trading Pair: base, quote, symbol, exchange, active status
   - Data Gap: pair, start_time, end_time, filled status
   - Collection Job: pair, time_range, status, error, retry_count
   - Validation Result: candle_id, checks_passed, anomalies, severity

2. **Generate API contracts** from functional requirements:
   - Exchange interface: FetchCandles, GetPairs, GetLimits
   - Storage interface: Store, Query, GetGaps, MarkGapFilled
   - Collector interface: Collect, Validate, BackfillGaps, Schedule
   - Output interface definitions to `/contracts/`

3. **Generate contract tests** from contracts:
   - One test file per interface
   - Assert method signatures and return types
   - Tests must fail (no implementation yet)

4. **Extract test scenarios** from user stories:
   - Initial sync retrieves all historical data
   - Continuous collection within 30 minutes
   - Gap detection and automatic backfill
   - Data validation catches anomalies
   - Concurrent collection handles rate limits

5. **Update agent file incrementally** (O(1) operation):
   - Run `/scripts/update-agent-context.sh claude` 
   - Add Go 1.21+, DuckDB, standard library focus
   - Update recent changes (keep last 3)
   - Keep under 150 lines for token efficiency
   - Output to repository root

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md, implementation-guide.md, dependency-map.md, CLAUDE.md

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
- Load `/templates/tasks-template.md` as base
- Generate tasks from Phase 1 design docs (contracts, data model, implementation-guide, dependency-map)
- Each interface → contract test task [P]
- Each entity → model creation task [P] 
- Each user story → integration test task
- Implementation tasks following dependency-map.md build order
- Each implementation file references specific research sections from implementation-guide.md

**Ordering Strategy**:
- TDD order: Tests before implementation 
- Dependency order from dependency-map.md: Level 1 (Models) → Level 2 (Interfaces) → Level 3 (Implementations) → Level 4 (Orchestration) → Level 5 (CLI)
- Mark [P] for parallel execution within same dependency level
- Each task references specific implementation-guide.md section

**Estimated Output**: 25-30 numbered, ordered tasks in tasks.md

**IMPORTANT**: This phase is executed by the /tasks command, NOT by /plan

## Phase 3+: Future Implementation
*These phases are beyond the scope of the /plan command*

**Phase 3**: Task execution (/tasks command creates tasks.md)  
**Phase 4**: Implementation (execute tasks.md following constitutional principles)  
**Phase 5**: Validation (run tests, execute quickstart.md, performance validation)

## Complexity Tracking
*Fill ONLY if Constitution Check has violations that must be justified*

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | - | - |

## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [x] Phase 0: Research complete (/plan command)
- [x] Phase 1: Design complete (/plan command)
- [ ] Phase 2: Task planning complete (/plan command - describe approach only)
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [x] Initial Constitution Check: PASS
- [x] Post-Design Constitution Check: PASS
- [x] All NEEDS CLARIFICATION resolved
- [ ] Complexity deviations documented

---
*Based on Constitution v2.1.1 - See `/memory/constitution.md`*