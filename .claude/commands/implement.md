Execute tasks from a feature's tasks.md file with state tracking and resumability.

This is the fourth step in the Spec-Driven Development lifecycle.

Given the feature directory path as an argument, do this:

1. **Load and validate task list**:
   - Read `$FEATURE_DIR/tasks.md` and parse all numbered tasks (T001, T002, etc.)
   - Extract task dependencies, parallel markers [P], and file paths
   - Validate task list completeness and dependency graph
   - If tasks.md not found or malformed: ERROR "Invalid or missing tasks.md"

2. **Initialize state tracking**:
   - Create or load `$FEATURE_DIR/.implementation-state.json` with task status:
     ```json
     {
       "current_phase": "3.1",
       "last_completed_task": "T000", 
       "task_status": {
         "T001": "pending|in_progress|completed|failed",
         "T002": "pending"
       },
       "parallel_groups": [],
       "failed_tasks": [],
       "started_at": "2025-01-15T10:30:00Z",
       "updated_at": "2025-01-15T11:45:00Z"
     }
     ```

3. **Determine execution strategy**:
   - Identify next executable tasks based on:
     * Current completion state
     * Dependency requirements (setup → tests → implementation → polish)
     * TDD enforcement (tests MUST complete before implementation)
     * Parallel execution opportunities ([P] marked tasks)
   - If resuming: Skip completed tasks, validate partial progress
   - If TDD violations detected: ERROR "Tests not completed before implementation"

4. **Execute tasks with state updates**:
   - For each executable task or parallel group:
     * Update state to "in_progress" before starting
     * Execute task using appropriate implementation strategy:
       - **Setup tasks**: Run shell commands, create directories, initialize files
       - **Test tasks**: Generate failing tests, validate they fail properly  
       - **Implementation tasks**: Generate code to make tests pass
       - **Integration tasks**: Configure connections, middleware, logging
       - **Polish tasks**: Documentation, performance validation, cleanup
     * Validate task completion using specified criteria
     * Update state to "completed" on success or "failed" on error
     * Commit changes with message "feat: Complete $TASK_ID - $DESCRIPTION"

5. **Handle parallel execution**:
   - When encountering [P] marked tasks in same dependency level:
     * Group all available parallel tasks
     * Launch multiple Task agents simultaneously using single message with multiple tool calls
     * Example for parallel test generation:
       ```
       Task: "Contract test for ExchangeAdapter interface in test/contract/exchange_contract_test.go"
       Task: "Contract test for CandleStorage interface in test/contract/storage_contract_test.go"
       Task: "Contract test for GapStorage interface in test/contract/gap_storage_contract_test.go"
       ```
   - Track all parallel task progress and wait for completion before proceeding

6. **Enforce TDD workflow**:
   - **CRITICAL**: For test tasks (T005-T011):
     * Generate test code that MUST fail initially
     * Run tests to verify they fail with expected errors
     * Mark test as complete only when failing properly
     * Block all implementation tasks until ALL tests in phase are failing
   - **For implementation tasks**: 
     * Run tests before starting to confirm they fail
     * Implement code to make tests pass
     * Run tests again to confirm they now pass
     * Only mark complete when tests pass

7. **Progress reporting and resumability**:
   - After each task completion:
     * Update `.implementation-state.json` with new status
     * Display progress summary: "Completed T005 (7/43 tasks) - Phase 3.2: Tests First"
     * Show next available tasks or parallel opportunities
   - On errors:
     * Mark task as "failed" with error details
     * Suggest remediation steps
     * Allow manual retry or skip (with justification)
   - Support interruption/resumption:
     * Save state after every task
     * On restart: "Resuming from T012 - OHLCV Candle model (Phase 3.3: Core Implementation)"

8. **Task-specific execution strategies**:

   **Setup Tasks (T001-T004)**:
   - T001: `go mod init`, verify go.mod created
   - T002: `mkdir -p cmd/ internal/ test/ scripts/ build/`
   - T003-T004: Install dependencies, create config files

   **Test Tasks (T005-T011)**:
   - Generate test files that exercise interfaces/integration scenarios
   - Run `go test` to verify tests fail with meaningful errors
   - Document expected failure reasons in state file

   **Model Tasks (T012-T015)**:
   - Generate Go structs with proper tags, validation methods
   - Run existing tests to verify they now pass (if applicable)

   **Interface Tasks (T016-T019)**:
   - Generate interface definitions matching contracts
   - Verify contract tests can compile against interfaces

   **Implementation Tasks (T020-T027)**:
   - Generate implementations that satisfy interfaces
   - Run tests to verify functionality
   - Verify no regressions in earlier tests

9. **Validation and quality gates**:
   - Before marking any task complete:
     * Verify all specified file paths exist and contain expected code
     * Run relevant tests and confirm they pass (or fail appropriately for test tasks)
     * Check code compiles without errors
     * Validate no breaking changes to earlier work
   - Before moving between phases:
     * Run full test suite for completed components
     * Verify dependency requirements met
     * Check implementation-guide.md references applied correctly

10. **Handle dependencies and blocking**:
    - Parse dependency relationships: "T025 depends on T020-T024"
    - Block dependent tasks until prerequisites complete
    - For parallel groups: All tasks in group must complete before dependents can start
    - Special handling for TDD: Implementation phases blocked until test phases complete

11. **Error handling and recovery**:
    - On task failure:
      * Capture error details and context
      * Suggest specific remediation based on failure type
      * Allow retry with fixes or manual skip with justification
      * Update state to track failed/skipped tasks
    - On compilation errors: Focus on syntax/import issues
    - On test failures: Analyze whether failure is expected (TDD) or actual problem
    - On dependency issues: Check if prerequisites actually completed properly

12. **Final validation**:
    - When all tasks completed:
      * Run full test suite: `go test ./...`
      * Verify performance requirements from tasks.md
      * Execute quickstart.md scenarios end-to-end
      * Generate final completion report with metrics
      * Archive `.implementation-state.json` as `.implementation-completed.json`

## Usage Examples

```bash
# Start fresh implementation
/implement specs/001-ohlcv-data-collector

# Resume interrupted implementation  
/implement specs/001-ohlcv-data-collector

# Force restart from specific task
/implement specs/001-ohlcv-data-collector --restart-from T010
```

## Error Recovery Patterns

**Test Generation Failure**: Analyze interface/contract requirements, regenerate with corrections
**Compilation Error**: Fix imports, syntax, type issues iteratively
**Test Failure (Unexpected)**: Determine if implementation bug or test issue, fix accordingly
**Dependency Error**: Verify prerequisites completed, check state consistency
**Performance Failure**: Profile code, apply optimizations from research.md findings

## State Persistence Format

```json
{
  "feature_dir": "/path/to/specs/001-ohlcv-data-collector",
  "current_phase": "3.3",
  "phase_descriptions": {
    "3.1": "Setup",
    "3.2": "Tests First (TDD)", 
    "3.3": "Core Implementation",
    "3.4": "Integration",
    "3.5": "Polish"
  },
  "last_completed_task": "T015",
  "task_status": {
    "T001": {"status": "completed", "completed_at": "2025-01-15T10:35:00Z"},
    "T002": {"status": "completed", "completed_at": "2025-01-15T10:37:00Z"},
    "T016": {"status": "in_progress", "started_at": "2025-01-15T11:42:00Z"}
  },
  "parallel_groups": [
    {"tasks": ["T005", "T006", "T007"], "status": "completed"},
    {"tasks": ["T012", "T013", "T014", "T015"], "status": "completed"}
  ],
  "tdd_gates": {
    "tests_phase_complete": true,
    "failing_tests_validated": ["T005", "T006", "T007", "T008", "T009", "T010", "T011"]
  },
  "metrics": {
    "total_tasks": 43,
    "completed_tasks": 15,
    "failed_tasks": 0,
    "estimated_remaining_time": "2.5 hours"
  }
}
```

## Success Criteria

The implementation is complete when:
- All 43 tasks marked as "completed"
- Full test suite passes: `go test ./...`
- Performance requirements verified
- Quickstart scenarios execute successfully
- All files exist at specified paths
- No compilation errors or warnings
- State file shows 100% completion

## Integration with Development Workflow

This command integrates with the Spec-Driven Development lifecycle:
1. `/specify` - Creates feature specification  
2. `/plan` - Generates implementation plan
3. `/tasks` - Breaks down into executable tasks
4. **`/implement`** - Executes tasks with state tracking ← THIS COMMAND
5. Manual testing and refinement
6. Feature completion and merge

The command ensures efficient, resumable, and reliable task execution while maintaining code quality and TDD discipline.