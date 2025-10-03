// Package gaps provides interfaces for gap detection and backfilling operations.
// These interfaces enable detection of missing data periods in OHLCV time series
// and orchestrate the backfill process to maintain data continuity.
package gaps

import (
	"context"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
)

// GapDetector identifies missing periods in historical OHLCV data sequences.
// It analyzes stored candle data to find gaps where expected candles are missing
// based on the trading pair's interval and expected trading hours.
type GapDetector interface {
	// DetectGaps scans the specified time range for missing candles and returns
	// detected gaps. It compares expected candle timestamps against stored data
	// to identify periods where data is missing.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair symbol (e.g., "BTC/USD", "ETH/BTC")
	//   - interval: Time interval for candles (e.g., "1m", "5m", "1h", "1d")
	//   - startTime: Beginning of the time range to scan for gaps
	//   - endTime: End of the time range to scan for gaps
	//
	// Returns:
	//   - []models.Gap: Slice of detected gaps with initial status "detected"
	//   - error: Error if gap detection fails
	//
	// The method will:
	// 1. Query existing candles in the time range
	// 2. Generate expected candle timestamps based on interval
	// 3. Compare expected vs actual timestamps to identify gaps
	// 4. Create Gap models for each missing period
	// 5. Store detected gaps with appropriate priority calculation
	DetectGaps(ctx context.Context, pair, interval string, startTime, endTime time.Time) ([]models.Gap, error)

	// DetectGapsInSequence analyzes a continuous sequence of candles to find
	// gaps within the sequence. This is useful for real-time gap detection
	// as new candles arrive.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - candles: Ordered slice of candles to analyze for gaps
	//   - expectedInterval: Expected time interval between candles
	//
	// Returns:
	//   - []models.Gap: Detected gaps between candles in the sequence
	//   - error: Error if analysis fails
	DetectGapsInSequence(ctx context.Context, candles []contracts.Candle, expectedInterval string) ([]models.Gap, error)

	// DetectRecentGaps scans for gaps in the most recent data, typically used
	// for ongoing monitoring to catch newly missing data.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair to check for recent gaps
	//   - interval: Candle interval
	//   - lookbackPeriod: How far back to check (e.g., 24h, 7d)
	//
	// Returns:
	//   - []models.Gap: Recently detected gaps
	//   - error: Error if detection fails
	DetectRecentGaps(ctx context.Context, pair, interval string, lookbackPeriod time.Duration) ([]models.Gap, error)

	// ValidateGapPeriod checks if a time period represents a valid gap
	// by considering exchange trading hours, maintenance windows, and
	// other factors that might explain missing data.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair symbol
	//   - startTime: Start of the potential gap period
	//   - endTime: End of the potential gap period
	//   - interval: Expected candle interval
	//
	// Returns:
	//   - bool: True if the period represents a legitimate gap to fill
	//   - string: Reason if the period should not be considered a gap
	//   - error: Error if validation fails
	ValidateGapPeriod(ctx context.Context, pair string, startTime, endTime time.Time, interval string) (bool, string, error)
}

// Backfiller manages the process of filling detected gaps by fetching missing
// data from exchange APIs and storing it appropriately.
type Backfiller interface {
	// StartBackfill begins automatic backfilling of detected gaps for a
	// specific trading pair and interval. It processes gaps in priority order
	// and manages concurrent filling operations.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair to backfill gaps for
	//   - interval: Candle interval to process
	//
	// Returns:
	//   - error: Error if backfill process fails to start
	//
	// The method will:
	// 1. Query all detected gaps for the pair/interval
	// 2. Sort gaps by priority (critical, high, medium, low)
	// 3. Start concurrent workers to process gaps
	// 4. Monitor progress and handle failures with retry logic
	StartBackfill(ctx context.Context, pair, interval string) error

	// StartPriorityBackfill processes gaps in strict priority order, ensuring
	// higher priority gaps are completed before lower priority ones.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair to process
	//   - interval: Candle interval
	//
	// Returns:
	//   - error: Error if priority backfill fails to start
	StartPriorityBackfill(ctx context.Context, pair, interval string) error

	// StartGapFilling fills a specific gap by its ID. This allows targeted
	// filling of individual gaps and precise control over the process.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - gapID: Unique identifier of the gap to fill
	//
	// Returns:
	//   - error: Error if gap filling fails
	//
	// The method will:
	// 1. Retrieve gap details from storage
	// 2. Transition gap status from "detected" to "filling"
	// 3. Fetch missing candles from exchange API
	// 4. Store retrieved candles
	// 5. Mark gap as "filled" or "permanent" based on result
	StartGapFilling(ctx context.Context, gapID string) error

	// FillGapWithData fills a gap using provided candle data instead of
	// fetching from an exchange. Useful for bulk imports or data recovery.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - gapID: ID of gap to fill
	//   - candles: Candle data to use for filling the gap
	//
	// Returns:
	//   - error: Error if filling with provided data fails
	FillGapWithData(ctx context.Context, gapID string, candles []contracts.Candle) error

	// RetryFailedGaps attempts to re-process gaps that previously failed,
	// applying backoff strategies and retry limits.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - maxRetries: Maximum number of retry attempts per gap
	//   - retryDelay: Minimum delay between retry attempts
	//
	// Returns:
	//   - int: Number of gaps attempted for retry
	//   - error: Error if retry process fails
	RetryFailedGaps(ctx context.Context, maxRetries int, retryDelay time.Duration) (int, error)

	// GetBackfillProgress returns current status of backfill operations
	// for monitoring and reporting purposes.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//
	// Returns:
	//   - *BackfillStatus: Current backfill operation status
	//   - error: Error if status retrieval fails
	GetBackfillProgress(ctx context.Context) (*BackfillStatus, error)

	// StopBackfill gracefully stops all running backfill operations,
	// allowing current gap fills to complete before stopping.
	//
	// Parameters:
	//   - ctx: Context with timeout for graceful shutdown
	//
	// Returns:
	//   - error: Error if stop operation fails
	StopBackfill(ctx context.Context) error
}

// GapManager provides high-level gap management functionality combining
// detection and backfilling capabilities with workflow orchestration.
type GapManager interface {
	// StartContinuousGapManagement begins ongoing gap detection and backfilling
	// for specified pairs. This runs continuously to maintain data completeness.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pairs: Trading pairs to monitor and manage gaps for
	//   - intervals: Candle intervals to process
	//   - config: Configuration for detection frequency, priorities, etc.
	//
	// Returns:
	//   - error: Error if continuous management fails to start
	StartContinuousGapManagement(ctx context.Context, pairs []string, intervals []string, config GapManagerConfig) error

	// RunGapDetectionCycle executes a single cycle of gap detection across
	// all monitored pairs and intervals.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//
	// Returns:
	//   - *DetectionResult: Results of the detection cycle
	//   - error: Error if detection cycle fails
	RunGapDetectionCycle(ctx context.Context) (*DetectionResult, error)

	// RunBackfillCycle executes a single cycle of gap backfilling, processing
	// the highest priority gaps first.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//
	// Returns:
	//   - *BackfillResult: Results of the backfill cycle
	//   - error: Error if backfill cycle fails
	RunBackfillCycle(ctx context.Context) (*BackfillResult, error)

	// GetGapStatistics returns comprehensive statistics about gap detection
	// and backfilling across all monitored pairs.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//
	// Returns:
	//   - *GapStatistics: Detailed gap management statistics
	//   - error: Error if statistics retrieval fails
	GetGapStatistics(ctx context.Context) (*GapStatistics, error)

	// PrioritizeGaps recalculates priorities for all detected gaps based on
	// current criteria like age, duration, and business importance.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//
	// Returns:
	//   - int: Number of gaps that had priority changes
	//   - error: Error if prioritization fails
	PrioritizeGaps(ctx context.Context) (int, error)

	// CleanupCompletedGaps removes old completed gaps from storage to
	// prevent unbounded growth of gap history.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - retentionPeriod: How long to keep completed gap records
	//
	// Returns:
	//   - int: Number of gaps cleaned up
	//   - error: Error if cleanup fails
	CleanupCompletedGaps(ctx context.Context, retentionPeriod time.Duration) (int, error)

	// HandleGapOverlaps resolves situations where multiple gaps overlap
	// in time periods, merging or adjusting them as appropriate.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - pair: Trading pair to check for overlapping gaps
	//   - interval: Candle interval
	//
	// Returns:
	//   - int: Number of overlaps resolved
	//   - error: Error if overlap resolution fails
	HandleGapOverlaps(ctx context.Context, pair, interval string) (int, error)
}

// GapManagerConfig provides configuration for gap management operations
type GapManagerConfig struct {
	// DetectionInterval defines how often to run gap detection
	DetectionInterval time.Duration

	// BackfillInterval defines how often to run backfill cycles
	BackfillInterval time.Duration

	// MaxConcurrentBackfills limits concurrent gap filling operations
	MaxConcurrentBackfills int

	// RetryAttempts sets maximum retry attempts for failed gaps
	RetryAttempts int

	// RetryDelay sets minimum delay between retry attempts
	RetryDelay time.Duration

	// PriorityThresholds define cutoffs for gap priority levels
	PriorityThresholds PriorityThresholds

	// CleanupInterval defines how often to clean up old gaps
	CleanupInterval time.Duration

	// CompletedGapRetention defines how long to keep completed gaps
	CompletedGapRetention time.Duration
}

// PriorityThresholds defines criteria for gap priority calculation
type PriorityThresholds struct {
	// Critical priority thresholds
	CriticalAgeDuration time.Duration // Gaps older than this are critical
	CriticalGapDuration time.Duration // Gaps longer than this are critical

	// High priority thresholds
	HighAgeDuration time.Duration // Gaps older than this are high priority
	HighGapDuration time.Duration // Gaps longer than this are high priority

	// Low priority threshold
	RecentGapThreshold time.Duration // Very recent gaps get low priority
}

// BackfillStatus represents the current state of backfill operations
type BackfillStatus struct {
	// Active indicates if backfilling is currently running
	Active bool

	// ActiveGaps is the number of gaps currently being filled
	ActiveGaps int

	// QueuedGaps is the number of gaps waiting to be processed
	QueuedGaps int

	// CompletedGaps is the number of gaps filled in the current session
	CompletedGaps int

	// FailedGaps is the number of gaps that failed to fill
	FailedGaps int

	// StartTime indicates when the current backfill session started
	StartTime time.Time

	// LastActivity indicates the last time a gap was processed
	LastActivity time.Time

	// ErrorCount tracks the number of errors encountered
	ErrorCount int

	// SuccessRate is the percentage of successfully filled gaps
	SuccessRate float64
}

// DetectionResult contains results from a gap detection cycle
type DetectionResult struct {
	// PairsScanned is the number of trading pairs that were scanned
	PairsScanned int

	// IntervalsScanned is the number of intervals checked per pair
	IntervalsScanned int

	// GapsDetected is the total number of new gaps found
	GapsDetected int

	// GapsByPair breaks down gap counts by trading pair
	GapsByPair map[string]int

	// GapsByPriority breaks down gaps by priority level
	GapsByPriority map[models.GapPriority]int

	// ScanDuration is how long the detection cycle took
	ScanDuration time.Duration

	// Errors contains any errors encountered during detection
	Errors []error
}

// BackfillResult contains results from a backfill cycle
type BackfillResult struct {
	// GapsProcessed is the number of gaps that were attempted
	GapsProcessed int

	// GapsFilled is the number of gaps successfully filled
	GapsFilled int

	// GapsFailed is the number of gaps that failed to fill
	GapsFailed int

	// GapsMarkedPermanent is the number of gaps marked as permanent
	GapsMarkedPermanent int

	// CandlesRetrieved is the total number of candles fetched
	CandlesRetrieved int

	// ProcessingDuration is how long the backfill cycle took
	ProcessingDuration time.Duration

	// Errors contains any errors encountered during backfilling
	Errors []error
}

// GapStatistics provides comprehensive gap management metrics
type GapStatistics struct {
	// TotalGaps is the total number of gaps ever detected
	TotalGaps int

	// ActiveGaps is the number of gaps currently waiting to be filled
	ActiveGaps int

	// FilledGaps is the number of successfully filled gaps
	FilledGaps int

	// PermanentGaps is the number of gaps marked as permanent
	PermanentGaps int

	// GapsByStatus breaks down gaps by current status
	GapsByStatus map[models.GapStatus]int

	// GapsByPriority breaks down gaps by priority level
	GapsByPriority map[models.GapPriority]int

	// GapsByPair breaks down gaps by trading pair
	GapsByPair map[string]int

	// AverageGapDuration is the mean duration of detected gaps
	AverageGapDuration time.Duration

	// AverageFillTime is the mean time taken to fill gaps
	AverageFillTime time.Duration

	// SuccessRate is the percentage of gaps successfully filled
	SuccessRate float64

	// OldestActiveGap indicates the oldest gap still waiting to be filled
	OldestActiveGap *time.Time

	// LastDetectionRun indicates when gaps were last detected
	LastDetectionRun *time.Time

	// LastBackfillRun indicates when backfilling last ran
	LastBackfillRun *time.Time
}

// GapValidator provides validation logic for gap detection
type GapValidator interface {
	// IsValidGapPeriod determines if a time period should be considered a gap
	// based on exchange trading hours, maintenance schedules, and other factors.
	IsValidGapPeriod(ctx context.Context, pair string, start, end time.Time, interval string) (bool, string, error)

	// ShouldIgnoreGap determines if a detected gap should be ignored based
	// on business rules, data quality issues, or other criteria.
	ShouldIgnoreGap(ctx context.Context, gap *models.Gap) (bool, string, error)

	// ValidateGapData ensures that candles intended to fill a gap are
	// appropriate and don't conflict with existing data.
	ValidateGapData(ctx context.Context, gap *models.Gap, candles []contracts.Candle) error
}

// GapPersistence provides storage operations specific to gap management
type GapPersistence interface {
	// StoreDetectedGaps persists newly detected gaps to storage
	StoreDetectedGaps(ctx context.Context, gaps []models.Gap) error

	// UpdateGapStatus changes the status of a gap and updates related fields
	UpdateGapStatus(ctx context.Context, gapID string, status models.GapStatus) error

	// GetGapsByStatus retrieves gaps with a specific status
	GetGapsByStatus(ctx context.Context, status models.GapStatus) ([]models.Gap, error)

	// GetPriorityGaps retrieves gaps ordered by priority for backfilling
	GetPriorityGaps(ctx context.Context, limit int) ([]models.Gap, error)

	// MarkGapFilled updates a gap as successfully filled
	MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error

	// RecordGapFailure records a failed attempt to fill a gap
	RecordGapFailure(ctx context.Context, gapID string, errorMsg string) error
}

// GapEventNotifier provides notifications about gap management events
type GapEventNotifier interface {
	// NotifyGapDetected sends notification when new gaps are found
	NotifyGapDetected(ctx context.Context, gaps []models.Gap) error

	// NotifyGapFilled sends notification when a gap is successfully filled
	NotifyGapFilled(ctx context.Context, gap models.Gap) error

	// NotifyGapFailed sends notification when gap filling fails
	NotifyGapFailed(ctx context.Context, gap models.Gap, err error) error

	// NotifyBackfillComplete sends notification when a backfill session completes
	NotifyBackfillComplete(ctx context.Context, result BackfillResult) error
}
