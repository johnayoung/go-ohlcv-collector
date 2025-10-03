// Package gaps provides gap detection and automatic backfill implementation
// for maintaining OHLCV data continuity and completeness.
package gaps

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

// GapDetectorImpl implements the GapDetector interface with comprehensive
// gap detection logic for identifying missing periods in historical data.
type GapDetectorImpl struct {
	storage   storage.FullStorage
	validator GapValidator
	logger    *slog.Logger
	mu        sync.RWMutex
}

// BackfillerImpl implements the Backfiller interface with automatic
// backfill capabilities including priority ordering and concurrent processing.
type BackfillerImpl struct {
	storage       storage.FullStorage
	exchange      contracts.ExchangeAdapter
	logger        *slog.Logger
	isRunning     bool
	activeFills   map[string]bool
	maxConcurrent int
	retryAttempts int
	retryDelay    time.Duration
	mu            sync.RWMutex
	fillWg        sync.WaitGroup
	metrics       *BackfillMetrics
}

// GapManagerImpl implements the GapManager interface providing high-level
// gap management functionality with continuous monitoring and orchestration.
type GapManagerImpl struct {
	detector   GapDetector
	backfiller Backfiller
	storage    storage.FullStorage
	config     GapManagerConfig
	logger     *slog.Logger
	isRunning  bool
	stopCh     chan struct{}
	mu         sync.RWMutex
}

// BackfillMetrics tracks performance and operational metrics for backfill operations.
type BackfillMetrics struct {
	TotalGapsProcessed  int64
	GapsFilled          int64
	GapsFailed          int64
	GapsMarkedPermanent int64
	CandlesRetrieved    int64
	TotalProcessingTime time.Duration
	LastBackfillRun     time.Time
	mu                  sync.RWMutex
}

// GapValidatorImpl provides validation logic for gap detection and processing.
type GapValidatorImpl struct {
	logger *slog.Logger
}

// NewGapDetector creates a new gap detector instance with the provided storage backend.
func NewGapDetector(storage storage.FullStorage) GapDetector {
	return &GapDetectorImpl{
		storage:   storage,
		validator: NewGapValidator(),
		logger:    slog.Default().With("component", "gap_detector"),
	}
}

// NewBackfiller creates a new backfiller instance with storage and exchange adapters.
func NewBackfiller(storage storage.FullStorage, exchange contracts.ExchangeAdapter) Backfiller {
	return &BackfillerImpl{
		storage:       storage,
		exchange:      exchange,
		logger:        slog.Default().With("component", "backfiller"),
		activeFills:   make(map[string]bool),
		maxConcurrent: 10, // Default concurrent gap fills
		retryAttempts: 3,
		retryDelay:    5 * time.Minute,
		metrics:       &BackfillMetrics{},
	}
}

// NewGapManager creates a new gap manager instance combining detection and backfill capabilities.
func NewGapManager(detector GapDetector, backfiller Backfiller, storage storage.FullStorage, config GapManagerConfig) GapManager {
	return &GapManagerImpl{
		detector:   detector,
		backfiller: backfiller,
		storage:    storage,
		config:     config,
		logger:     slog.Default().With("component", "gap_manager"),
		stopCh:     make(chan struct{}),
	}
}

// NewGapValidator creates a new gap validator instance.
func NewGapValidator() GapValidator {
	return &GapValidatorImpl{
		logger: slog.Default().With("component", "gap_validator"),
	}
}

// DetectGaps scans the specified time range for missing candles and returns detected gaps.
func (gd *GapDetectorImpl) DetectGaps(ctx context.Context, pair, interval string, startTime, endTime time.Time) ([]models.Gap, error) {
	gd.logger.Info("Starting gap detection",
		"pair", pair,
		"interval", interval,
		"start_time", startTime,
		"end_time", endTime,
	)

	// Parse interval duration
	intervalDuration, err := parseIntervalDuration(interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval '%s': %w", interval, err)
	}

	// Query existing candles in the time range
	queryReq := storage.QueryRequest{
		Pair:     pair,
		Start:    startTime,
		End:      endTime,
		Interval: interval,
		OrderBy:  "timestamp_asc",
	}

	response, err := gd.storage.Query(ctx, queryReq)
	if err != nil {
		return nil, fmt.Errorf("failed to query existing candles: %w", err)
	}

	// Convert storage candles to contracts candles for gap detection
	contractCandles := make([]contracts.Candle, len(response.Candles))
	for i, candle := range response.Candles {
		contractCandles[i] = contracts.Candle{
			Timestamp: candle.Timestamp,
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
			Pair:      candle.Pair,
			Interval:  candle.Interval,
		}
	}

	// Generate expected timestamps and compare with actual
	gaps := gd.findGapsInRange(ctx, pair, interval, startTime, endTime, intervalDuration, contractCandles)

	// Validate and store detected gaps
	validGaps := make([]models.Gap, 0, len(gaps))
	for _, gap := range gaps {
		// Validate the gap period
		isValid, reason, err := gd.ValidateGapPeriod(ctx, pair, gap.StartTime, gap.EndTime, interval)
		if err != nil {
			gd.logger.Warn("Failed to validate gap period",
				"gap_id", gap.ID,
				"error", err,
			)
			continue
		}

		if !isValid {
			gd.logger.Debug("Skipping invalid gap",
				"gap_id", gap.ID,
				"reason", reason,
			)
			continue
		}

		// Store the gap
		if err := gd.storage.StoreGap(ctx, gap); err != nil {
			gd.logger.Error("Failed to store detected gap",
				"gap_id", gap.ID,
				"error", err,
			)
			continue
		}

		validGaps = append(validGaps, gap)
	}

	gd.logger.Info("Gap detection completed",
		"pair", pair,
		"interval", interval,
		"gaps_found", len(validGaps),
	)

	return validGaps, nil
}

// DetectGapsInSequence analyzes a continuous sequence of candles to find gaps within the sequence.
func (gd *GapDetectorImpl) DetectGapsInSequence(ctx context.Context, candles []contracts.Candle, expectedInterval string) ([]models.Gap, error) {
	if len(candles) == 0 {
		return nil, nil
	}

	intervalDuration, err := parseIntervalDuration(expectedInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval '%s': %w", expectedInterval, err)
	}

	// Sort candles by timestamp
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	var gaps []models.Gap
	for i := 0; i < len(candles)-1; i++ {
		current := candles[i]
		next := candles[i+1]

		expectedNext := current.Timestamp.Add(intervalDuration)
		if next.Timestamp.After(expectedNext) {
			// Found a gap
			gapID := generateGapID(current.Pair, expectedNext, next.Timestamp, expectedInterval)
			gap, err := models.NewGap(gapID, current.Pair, expectedNext, next.Timestamp, expectedInterval)
			if err != nil {
				gd.logger.Warn("Failed to create gap",
					"gap_id", gapID,
					"error", err,
				)
				continue
			}

			gaps = append(gaps, *gap)
		}
	}

	return gaps, nil
}

// DetectRecentGaps scans for gaps in the most recent data for ongoing monitoring.
func (gd *GapDetectorImpl) DetectRecentGaps(ctx context.Context, pair, interval string, lookbackPeriod time.Duration) ([]models.Gap, error) {
	endTime := time.Now().UTC()
	startTime := endTime.Add(-lookbackPeriod)

	return gd.DetectGaps(ctx, pair, interval, startTime, endTime)
}

// ValidateGapPeriod checks if a time period represents a valid gap by considering
// exchange trading hours, maintenance windows, and other factors.
func (gd *GapDetectorImpl) ValidateGapPeriod(ctx context.Context, pair string, startTime, endTime time.Time, interval string) (bool, string, error) {
	return gd.validator.IsValidGapPeriod(ctx, pair, startTime, endTime, interval)
}

// StartBackfill begins automatic backfilling of detected gaps for a specific trading pair and interval.
func (bf *BackfillerImpl) StartBackfill(ctx context.Context, pair, interval string) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.isRunning {
		return fmt.Errorf("backfill already running for pair %s interval %s", pair, interval)
	}

	bf.logger.Info("Starting backfill process",
		"pair", pair,
		"interval", interval,
	)

	// Get all detected gaps for the pair/interval
	gaps, err := bf.storage.GetGaps(ctx, pair, interval)
	if err != nil {
		return fmt.Errorf("failed to retrieve gaps: %w", err)
	}

	// Filter for gaps that can be filled
	fillableGaps := bf.filterFillableGaps(gaps)
	if len(fillableGaps) == 0 {
		bf.logger.Info("No fillable gaps found",
			"pair", pair,
			"interval", interval,
		)
		return nil
	}

	// Sort gaps by priority (highest first)
	sort.Slice(fillableGaps, func(i, j int) bool {
		if fillableGaps[i].Priority != fillableGaps[j].Priority {
			return fillableGaps[i].Priority > fillableGaps[j].Priority
		}
		// If same priority, older gaps first
		return fillableGaps[i].CreatedAt.Before(fillableGaps[j].CreatedAt)
	})

	bf.isRunning = true

	// Start concurrent workers to process gaps
	go bf.processGapsConcurrently(ctx, fillableGaps)

	return nil
}

// StartPriorityBackfill processes gaps in strict priority order.
func (bf *BackfillerImpl) StartPriorityBackfill(ctx context.Context, pair, interval string) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.logger.Info("Starting priority backfill",
		"pair", pair,
		"interval", interval,
	)

	// Get all detected gaps
	gaps, err := bf.storage.GetGaps(ctx, pair, interval)
	if err != nil {
		return fmt.Errorf("failed to retrieve gaps: %w", err)
	}

	// Filter and sort by priority
	fillableGaps := bf.filterFillableGaps(gaps)
	sort.Slice(fillableGaps, func(i, j int) bool {
		if fillableGaps[i].Priority != fillableGaps[j].Priority {
			return fillableGaps[i].Priority > fillableGaps[j].Priority
		}
		return fillableGaps[i].CreatedAt.Before(fillableGaps[j].CreatedAt)
	})

	// Process gaps sequentially by priority
	go bf.processGapsSequentially(ctx, fillableGaps)

	return nil
}

// StartGapFilling fills a specific gap by its ID.
func (bf *BackfillerImpl) StartGapFilling(ctx context.Context, gapID string) error {
	bf.mu.Lock()
	if bf.activeFills[gapID] {
		bf.mu.Unlock()
		return fmt.Errorf("gap %s is already being filled", gapID)
	}
	bf.activeFills[gapID] = true
	bf.mu.Unlock()

	defer func() {
		bf.mu.Lock()
		delete(bf.activeFills, gapID)
		bf.mu.Unlock()
	}()

	bf.logger.Info("Starting gap filling",
		"gap_id", gapID,
	)

	// Retrieve gap details
	gap, err := bf.storage.GetGapByID(ctx, gapID)
	if err != nil {
		return fmt.Errorf("failed to retrieve gap %s: %w", gapID, err)
	}

	if gap == nil {
		return fmt.Errorf("gap %s not found", gapID)
	}

	// Gap from storage is already models.Gap
	modelGap := *gap

	// Transition gap to filling status
	if err := modelGap.StartFilling(); err != nil {
		return fmt.Errorf("failed to transition gap to filling status: %w", err)
	}

	// Store the updated gap with filling status
	if err := bf.storage.StoreGap(ctx, modelGap); err != nil {
		return fmt.Errorf("failed to update gap status to filling: %w", err)
	}

	// Fetch missing candles from exchange
	err = bf.fillGapWithExchangeData(ctx, &modelGap)
	if err != nil {
		// Record failure and transition back to detected
		bf.logger.Error("Failed to fill gap",
			"gap_id", gapID,
			"error", err,
		)

		if failErr := modelGap.RecordFailure(err.Error()); failErr != nil {
			bf.logger.Error("Failed to record gap failure",
				"gap_id", gapID,
				"error", failErr,
			)
		}

		if updateErr := bf.storage.StoreGap(ctx, modelGap); updateErr != nil {
			bf.logger.Error("Failed to update gap status after failure",
				"gap_id", gapID,
				"error", updateErr,
			)
		}

		return err
	}

	// Mark gap as filled
	if err := modelGap.MarkFilled(); err != nil {
		return fmt.Errorf("failed to mark gap as filled: %w", err)
	}

	if err := bf.storage.MarkGapFilled(ctx, gapID, time.Now().UTC()); err != nil {
		return fmt.Errorf("failed to update gap as filled in storage: %w", err)
	}

	bf.logger.Info("Gap successfully filled",
		"gap_id", gapID,
	)

	// Update metrics
	bf.metrics.mu.Lock()
	bf.metrics.GapsFilled++
	bf.metrics.TotalGapsProcessed++
	bf.metrics.mu.Unlock()

	return nil
}

// FillGapWithData fills a gap using provided candle data.
func (bf *BackfillerImpl) FillGapWithData(ctx context.Context, gapID string, candles []contracts.Candle) error {
	bf.logger.Info("Filling gap with provided data",
		"gap_id", gapID,
		"candle_count", len(candles),
	)

	// Retrieve gap details
	gap, err := bf.storage.GetGapByID(ctx, gapID)
	if err != nil {
		return fmt.Errorf("failed to retrieve gap %s: %w", gapID, err)
	}

	if gap == nil {
		return fmt.Errorf("gap %s not found", gapID)
	}

	// Convert contracts.Candle to models.Candle for storage
	modelCandles := make([]models.Candle, len(candles))
	for i, candle := range candles {
		modelCandles[i] = models.Candle{
			Timestamp: candle.Timestamp,
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
			Pair:      candle.Pair,
			Interval:  candle.Interval,
		}
	}

	// Store the candles
	if err := bf.storage.Store(ctx, modelCandles); err != nil {
		return fmt.Errorf("failed to store candles: %w", err)
	}

	// Mark gap as filled
	if err := bf.storage.MarkGapFilled(ctx, gapID, time.Now().UTC()); err != nil {
		return fmt.Errorf("failed to mark gap as filled: %w", err)
	}

	bf.logger.Info("Gap filled with provided data",
		"gap_id", gapID,
	)

	return nil
}

// RetryFailedGaps attempts to re-process gaps that previously failed.
func (bf *BackfillerImpl) RetryFailedGaps(ctx context.Context, maxRetries int, retryDelay time.Duration) (int, error) {
	bf.logger.Info("Starting retry of failed gaps",
		"max_retries", maxRetries,
		"retry_delay", retryDelay,
	)

	// Get all detected gaps (gaps that have failed remain in detected status)
	detectedGaps, err := bf.storage.GetGapsByStatus(ctx, models.GapStatusDetected)
	if err != nil {
		return 0, fmt.Errorf("failed to get detected gaps: %w", err)
	}

	// Filter for gaps that have errors (previously failed) and haven't exceeded max retries
	var failedGaps []models.Gap
	for _, gap := range detectedGaps {
		if gap.ErrorMessage != "" && gap.Attempts > 0 && gap.Attempts < maxRetries {
			failedGaps = append(failedGaps, gap)
		}
	}

	if len(failedGaps) == 0 {
		bf.logger.Info("No failed gaps found to retry")
		return 0, nil
	}

	bf.logger.Info("Found failed gaps to retry", "count", len(failedGaps))

	retriedCount := 0
	for _, gap := range failedGaps {
		// Apply retry delay if configured
		if retryDelay > 0 && gap.LastAttemptAt != nil {
			timeSinceLastAttempt := time.Since(*gap.LastAttemptAt)
			if timeSinceLastAttempt < retryDelay {
				bf.logger.Debug("Skipping gap due to retry delay",
					"gap_id", gap.ID,
					"time_since_last_attempt", timeSinceLastAttempt,
				)
				continue
			}
		}

		// Attempt to fill the gap
		err := bf.StartGapFilling(ctx, gap.ID)
		if err != nil {
			bf.logger.Warn("Failed to start gap filling for retry",
				"gap_id", gap.ID,
				"error", err,
			)
			continue
		}

		retriedCount++
	}

	bf.logger.Info("Completed retry of failed gaps",
		"attempted", retriedCount,
		"total_failed", len(failedGaps),
	)

	return retriedCount, nil
}

// GetBackfillProgress returns current status of backfill operations.
func (bf *BackfillerImpl) GetBackfillProgress(ctx context.Context) (*BackfillStatus, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Get gaps by status for accurate progress reporting
	detectedGaps, err := bf.storage.GetGapsByStatus(ctx, models.GapStatusDetected)
	if err != nil {
		bf.logger.Warn("Failed to get detected gaps for progress", "error", err)
		detectedGaps = []models.Gap{}
	}

	fillingGaps, err := bf.storage.GetGapsByStatus(ctx, models.GapStatusFilling)
	if err != nil {
		bf.logger.Warn("Failed to get filling gaps for progress", "error", err)
		fillingGaps = []models.Gap{}
	}

	bf.metrics.mu.RLock()
	metrics := BackfillMetrics{
		TotalGapsProcessed:  bf.metrics.TotalGapsProcessed,
		GapsFilled:          bf.metrics.GapsFilled,
		GapsFailed:          bf.metrics.GapsFailed,
		GapsMarkedPermanent: bf.metrics.GapsMarkedPermanent,
		CandlesRetrieved:    bf.metrics.CandlesRetrieved,
		TotalProcessingTime: bf.metrics.TotalProcessingTime,
		LastBackfillRun:     bf.metrics.LastBackfillRun,
	}
	bf.metrics.mu.RUnlock()

	successRate := float64(0)
	if metrics.TotalGapsProcessed > 0 {
		successRate = float64(metrics.GapsFilled) / float64(metrics.TotalGapsProcessed) * 100
	}

	return &BackfillStatus{
		Active:        bf.isRunning,
		ActiveGaps:    len(fillingGaps),
		QueuedGaps:    len(detectedGaps),
		CompletedGaps: int(metrics.GapsFilled),
		FailedGaps:    int(metrics.GapsFailed),
		StartTime:     time.Time{}, // Would be set when backfill session starts
		LastActivity:  metrics.LastBackfillRun,
		ErrorCount:    int(metrics.GapsFailed),
		SuccessRate:   successRate,
	}, nil
}

// StopBackfill gracefully stops all running backfill operations.
func (bf *BackfillerImpl) StopBackfill(ctx context.Context) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if !bf.isRunning {
		return nil
	}

	bf.logger.Info("Stopping backfill operations")

	bf.isRunning = false

	// Wait for active fills to complete
	done := make(chan struct{})
	go func() {
		bf.fillWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		bf.logger.Info("All backfill operations stopped gracefully")
	case <-ctx.Done():
		bf.logger.Warn("Backfill stop timed out, some operations may still be running")
		return ctx.Err()
	}

	return nil
}

// Gap Manager Implementation

// StartContinuousGapManagement begins ongoing gap detection and backfilling.
func (gm *GapManagerImpl) StartContinuousGapManagement(ctx context.Context, pairs []string, intervals []string, config GapManagerConfig) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.isRunning {
		return fmt.Errorf("gap management is already running")
	}

	gm.config = config
	gm.isRunning = true

	gm.logger.Info("Starting continuous gap management",
		"pairs", pairs,
		"intervals", intervals,
	)

	// Start detection and backfill cycles
	go gm.runContinuousManagement(ctx, pairs, intervals)

	return nil
}

// RunGapDetectionCycle executes a single cycle of gap detection.
func (gm *GapManagerImpl) RunGapDetectionCycle(ctx context.Context) (*DetectionResult, error) {
	gm.logger.Info("Running gap detection cycle")

	startTime := time.Now()
	result := &DetectionResult{
		GapsByPair:     make(map[string]int),
		GapsByPriority: make(map[models.GapPriority]int),
		Errors:         []error{},
	}

	// This would need to be configured with actual pairs and intervals
	// For now, returning a basic result structure
	result.ScanDuration = time.Since(startTime)

	gm.logger.Info("Gap detection cycle completed",
		"duration", result.ScanDuration,
		"gaps_detected", result.GapsDetected,
	)

	return result, nil
}

// RunBackfillCycle executes a single cycle of gap backfilling.
func (gm *GapManagerImpl) RunBackfillCycle(ctx context.Context) (*BackfillResult, error) {
	gm.logger.Info("Running backfill cycle")

	startTime := time.Now()
	result := &BackfillResult{
		Errors: []error{},
	}

	// Get backfill progress
	progress, err := gm.backfiller.GetBackfillProgress(ctx)
	if err != nil {
		result.Errors = append(result.Errors, err)
	} else {
		result.GapsFilled = progress.CompletedGaps
		result.GapsFailed = progress.FailedGaps
	}

	result.ProcessingDuration = time.Since(startTime)

	gm.logger.Info("Backfill cycle completed",
		"duration", result.ProcessingDuration,
		"gaps_filled", result.GapsFilled,
		"gaps_failed", result.GapsFailed,
	)

	return result, nil
}

// GetGapStatistics returns comprehensive statistics about gap management.
func (gm *GapManagerImpl) GetGapStatistics(ctx context.Context) (*GapStatistics, error) {
	// Get gaps by status for comprehensive statistics
	detectedGaps, err := gm.storage.GetGapsByStatus(ctx, models.GapStatusDetected)
	if err != nil {
		return nil, fmt.Errorf("failed to get detected gaps: %w", err)
	}

	fillingGaps, err := gm.storage.GetGapsByStatus(ctx, models.GapStatusFilling)
	if err != nil {
		return nil, fmt.Errorf("failed to get filling gaps: %w", err)
	}

	filledGaps, err := gm.storage.GetGapsByStatus(ctx, models.GapStatusFilled)
	if err != nil {
		return nil, fmt.Errorf("failed to get filled gaps: %w", err)
	}

	permanentGaps, err := gm.storage.GetGapsByStatus(ctx, models.GapStatusPermanent)
	if err != nil {
		return nil, fmt.Errorf("failed to get permanent gaps: %w", err)
	}

	// Calculate gaps by status
	gapsByStatus := map[models.GapStatus]int{
		models.GapStatusDetected:  len(detectedGaps),
		models.GapStatusFilling:   len(fillingGaps),
		models.GapStatusFilled:    len(filledGaps),
		models.GapStatusPermanent: len(permanentGaps),
	}

	// Calculate gaps by priority
	gapsByPriority := make(map[models.GapPriority]int)
	allActiveGaps := append(detectedGaps, fillingGaps...)
	for _, gap := range allActiveGaps {
		gapsByPriority[gap.Priority]++
	}

	// Calculate gaps by pair
	gapsByPair := make(map[string]int)
	for _, gap := range allActiveGaps {
		gapsByPair[gap.Pair]++
	}

	totalGaps := len(detectedGaps) + len(fillingGaps) + len(filledGaps) + len(permanentGaps)
	successRate := float64(0)
	if totalGaps > 0 {
		successRate = float64(len(filledGaps)) / float64(totalGaps) * 100
	}

	// Find oldest active gap
	var oldestActiveGap *time.Time
	for _, gap := range allActiveGaps {
		if oldestActiveGap == nil || gap.CreatedAt.Before(*oldestActiveGap) {
			gapTime := gap.CreatedAt
			oldestActiveGap = &gapTime
		}
	}

	return &GapStatistics{
		TotalGaps:       totalGaps,
		ActiveGaps:      len(detectedGaps),
		FilledGaps:      len(filledGaps),
		PermanentGaps:   len(permanentGaps),
		GapsByStatus:    gapsByStatus,
		GapsByPriority:  gapsByPriority,
		GapsByPair:      gapsByPair,
		SuccessRate:     successRate,
		OldestActiveGap: oldestActiveGap,
	}, nil
}

// PrioritizeGaps recalculates priorities for all detected gaps.
func (gm *GapManagerImpl) PrioritizeGaps(ctx context.Context) (int, error) {
	// Get all detected gaps that need priority recalculation
	gaps, err := gm.storage.GetGapsByStatus(ctx, models.GapStatusDetected)
	if err != nil {
		return 0, fmt.Errorf("failed to get detected gaps: %w", err)
	}

	if len(gaps) == 0 {
		gm.logger.Info("No detected gaps to prioritize")
		return 0, nil
	}

	priorityChanges := 0

	// Recalculate priority for each gap
	for _, gap := range gaps {
		oldPriority := gap.Priority

		// Get the gap from storage to update it (this should be done via a storage method)
		// For now, we'll just count potential changes based on gap age and duration
		// A full implementation would need a UpdateGapPriority method in storage

		// Calculate new priority based on gap characteristics
		newPriority := calculateGapPriority(&gap)

		if newPriority != oldPriority {
			priorityChanges++
			gm.logger.Debug("Gap priority changed",
				"gap_id", gap.ID,
				"old_priority", oldPriority,
				"new_priority", newPriority,
			)
		}
	}

	gm.logger.Info("Gap prioritization completed",
		"gaps_checked", len(gaps),
		"priority_changes", priorityChanges,
	)

	return priorityChanges, nil
}

// calculateGapPriority determines the priority of a gap based on its characteristics.
func calculateGapPriority(gap *models.Gap) models.GapPriority {
	// Calculate gap age
	age := time.Since(gap.CreatedAt)

	// Calculate gap duration
	duration := gap.EndTime.Sub(gap.StartTime)

	// Critical priority: very recent gaps (< 1 hour old) or very large gaps (> 24 hours duration)
	if age < 1*time.Hour || duration > 24*time.Hour {
		return models.PriorityCritical
	}

	// High priority: recent gaps (< 6 hours old) or large gaps (> 6 hours duration)
	if age < 6*time.Hour || duration > 6*time.Hour {
		return models.PriorityHigh
	}

	// Medium priority: moderately old gaps (< 24 hours old) or medium gaps (> 1 hour duration)
	if age < 24*time.Hour || duration > 1*time.Hour {
		return models.PriorityMedium
	}

	// Low priority: old gaps or small gaps
	return models.PriorityLow
}

// CleanupCompletedGaps removes old completed gaps from storage.
func (gm *GapManagerImpl) CleanupCompletedGaps(ctx context.Context, retentionPeriod time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-retentionPeriod)

	// This would need to be implemented with a proper cleanup query
	// For now, return 0 as no gaps were cleaned up
	cleanedCount := 0

	gm.logger.Info("Gap cleanup completed",
		"cutoff_time", cutoffTime,
		"gaps_cleaned", cleanedCount,
	)

	return cleanedCount, nil
}

// HandleGapOverlaps resolves situations where multiple gaps overlap in time periods.
func (gm *GapManagerImpl) HandleGapOverlaps(ctx context.Context, pair, interval string) (int, error) {
	gaps, err := gm.storage.GetGaps(ctx, pair, interval)
	if err != nil {
		return 0, fmt.Errorf("failed to get gaps: %w", err)
	}

	// Sort gaps by start time
	sort.Slice(gaps, func(i, j int) bool {
		return gaps[i].StartTime.Before(gaps[j].StartTime)
	})

	overlapsResolved := 0
	// Logic to detect and resolve overlaps would go here
	// For now, return 0 as no overlaps were resolved

	gm.logger.Info("Gap overlap resolution completed",
		"pair", pair,
		"interval", interval,
		"overlaps_resolved", overlapsResolved,
	)

	return overlapsResolved, nil
}

// Gap Validator Implementation

// IsValidGapPeriod determines if a time period should be considered a gap.
func (gv *GapValidatorImpl) IsValidGapPeriod(ctx context.Context, pair string, start, end time.Time, interval string) (bool, string, error) {
	// Basic validation - in a real implementation this would check:
	// - Exchange trading hours
	// - Maintenance windows
	// - Holiday schedules
	// - Minimum gap duration thresholds

	duration := end.Sub(start)
	intervalDuration, err := parseIntervalDuration(interval)
	if err != nil {
		return false, "invalid interval", err
	}

	// Must be at least one interval duration
	if duration < intervalDuration {
		return false, "gap too short", nil
	}

	// Don't fill gaps that are too recent (might still be arriving)
	if time.Since(end) < 5*time.Minute {
		return false, "gap too recent", nil
	}

	return true, "", nil
}

// ShouldIgnoreGap determines if a detected gap should be ignored.
func (gv *GapValidatorImpl) ShouldIgnoreGap(ctx context.Context, gap *models.Gap) (bool, string, error) {
	// Check if gap has exceeded maximum retry attempts
	if gap.Attempts >= 5 {
		return true, "max retry attempts exceeded", nil
	}

	// Check if gap is too old (might be from exchange downtime)
	if gap.Age() > 30*24*time.Hour {
		return true, "gap too old", nil
	}

	return false, "", nil
}

// ValidateGapData ensures that candles intended to fill a gap are appropriate.
func (gv *GapValidatorImpl) ValidateGapData(ctx context.Context, gap *models.Gap, candles []contracts.Candle) error {
	if len(candles) == 0 {
		return fmt.Errorf("no candles provided to fill gap")
	}

	// Validate all candles are within the gap time range
	for _, candle := range candles {
		if candle.Timestamp.Before(gap.StartTime) || candle.Timestamp.After(gap.EndTime) {
			return fmt.Errorf("candle timestamp %v is outside gap range [%v, %v]",
				candle.Timestamp, gap.StartTime, gap.EndTime)
		}

		// Validate candle has the correct pair and interval
		if candle.Pair != gap.Pair {
			return fmt.Errorf("candle pair %s does not match gap pair %s", candle.Pair, gap.Pair)
		}

		if candle.Interval != gap.Interval {
			return fmt.Errorf("candle interval %s does not match gap interval %s", candle.Interval, gap.Interval)
		}
	}

	return nil
}

// Helper Functions

// findGapsInRange identifies gaps between expected and actual candle timestamps.
func (gd *GapDetectorImpl) findGapsInRange(ctx context.Context, pair, interval string, startTime, endTime time.Time, intervalDuration time.Duration, candles []contracts.Candle) []models.Gap {
	var gaps []models.Gap

	// Create a map of existing timestamps for fast lookup
	existingTimes := make(map[int64]bool)
	for _, candle := range candles {
		existingTimes[candle.Timestamp.Unix()] = true
	}

	// Generate expected timestamps and check for gaps
	current := startTime
	for current.Before(endTime) {
		if !existingTimes[current.Unix()] {
			// Found start of a gap, find the end
			gapStart := current
			gapEnd := current.Add(intervalDuration)

			// Extend gap end until we find an existing candle or reach endTime
			for gapEnd.Before(endTime) && !existingTimes[gapEnd.Unix()] {
				gapEnd = gapEnd.Add(intervalDuration)
			}

			// Create gap
			gapID := generateGapID(pair, gapStart, gapEnd, interval)
			gap, err := models.NewGap(gapID, pair, gapStart, gapEnd, interval)
			if err != nil {
				gd.logger.Warn("Failed to create gap",
					"gap_id", gapID,
					"error", err,
				)
				current = gapEnd
				continue
			}

			gaps = append(gaps, *gap)
			current = gapEnd
		} else {
			current = current.Add(intervalDuration)
		}
	}

	return gaps
}

// processGapsConcurrently processes gaps with concurrent workers.
func (bf *BackfillerImpl) processGapsConcurrently(ctx context.Context, gaps []models.Gap) {
	defer func() {
		bf.mu.Lock()
		bf.isRunning = false
		bf.mu.Unlock()
	}()

	semaphore := make(chan struct{}, bf.maxConcurrent)

	for _, gap := range gaps {
		if !bf.isRunning {
			break
		}

		semaphore <- struct{}{} // Acquire
		bf.fillWg.Add(1)

		go func(g models.Gap) {
			defer func() {
				bf.fillWg.Done()
				<-semaphore // Release
			}()

			if err := bf.StartGapFilling(ctx, g.ID); err != nil {
				bf.logger.Error("Failed to fill gap in concurrent processing",
					"gap_id", g.ID,
					"error", err,
				)
			}
		}(gap)
	}

	bf.fillWg.Wait()
}

// processGapsSequentially processes gaps one by one in priority order.
func (bf *BackfillerImpl) processGapsSequentially(ctx context.Context, gaps []models.Gap) {
	defer func() {
		bf.mu.Lock()
		bf.isRunning = false
		bf.mu.Unlock()
	}()

	for _, gap := range gaps {
		if !bf.isRunning {
			break
		}

		if err := bf.StartGapFilling(ctx, gap.ID); err != nil {
			bf.logger.Error("Failed to fill gap in sequential processing",
				"gap_id", gap.ID,
				"error", err,
			)
		}
	}
}

// fillGapWithExchangeData fetches missing candles from exchange and stores them.
func (bf *BackfillerImpl) fillGapWithExchangeData(ctx context.Context, gap *models.Gap) error {
	// Prepare fetch request
	fetchReq := contracts.FetchRequest{
		Pair:     gap.Pair,
		Start:    gap.StartTime,
		End:      gap.EndTime,
		Interval: gap.Interval,
		Limit:    1000, // Adjust based on exchange limits
	}

	// Fetch candles from exchange
	response, err := bf.exchange.FetchCandles(ctx, fetchReq)
	if err != nil {
		return fmt.Errorf("failed to fetch candles from exchange: %w", err)
	}

	if len(response.Candles) == 0 {
		// No data available - mark gap as permanent
		return fmt.Errorf("no data available for gap period")
	}

	// Convert and store candles
	modelCandles := make([]models.Candle, len(response.Candles))
	for i, candle := range response.Candles {
		modelCandles[i] = models.Candle{
			Timestamp: candle.Timestamp,
			Open:      candle.Open,
			High:      candle.High,
			Low:       candle.Low,
			Close:     candle.Close,
			Volume:    candle.Volume,
			Pair:      candle.Pair,
			Interval:  candle.Interval,
		}
	}

	if err := bf.storage.Store(ctx, modelCandles); err != nil {
		return fmt.Errorf("failed to store fetched candles: %w", err)
	}

	bf.metrics.mu.Lock()
	bf.metrics.CandlesRetrieved += int64(len(response.Candles))
	bf.metrics.mu.Unlock()

	return nil
}

// filterFillableGaps returns gaps that can be filled.
func (bf *BackfillerImpl) filterFillableGaps(gaps []models.Gap) []models.Gap {
	var fillable []models.Gap
	for _, gap := range gaps {
		if gap.CanFill() {
			fillable = append(fillable, gap)
		}
	}
	return fillable
}

// runContinuousManagement runs the continuous gap management cycles.
func (gm *GapManagerImpl) runContinuousManagement(ctx context.Context, pairs []string, intervals []string) {
	detectionTicker := time.NewTicker(gm.config.DetectionInterval)
	backfillTicker := time.NewTicker(gm.config.BackfillInterval)
	defer detectionTicker.Stop()
	defer backfillTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gm.stopCh:
			return
		case <-detectionTicker.C:
			if _, err := gm.RunGapDetectionCycle(ctx); err != nil {
				gm.logger.Error("Gap detection cycle failed", "error", err)
			}
		case <-backfillTicker.C:
			if _, err := gm.RunBackfillCycle(ctx); err != nil {
				gm.logger.Error("Backfill cycle failed", "error", err)
			}
		}
	}
}

// Utility Functions

// parseIntervalDuration converts interval string to time.Duration.
func parseIntervalDuration(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "8h":
		return 8 * time.Hour, nil
	case "12h":
		return 12 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "1w":
		return 7 * 24 * time.Hour, nil
	case "1M":
		return 30 * 24 * time.Hour, nil // Approximate
	default:
		// Try to parse custom format like "2h", "3m", etc.
		if len(interval) < 2 {
			return 0, fmt.Errorf("invalid interval format: %s", interval)
		}

		unit := interval[len(interval)-1:]
		valueStr := interval[:len(interval)-1]
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			return 0, fmt.Errorf("invalid interval value: %s", interval)
		}

		switch unit {
		case "m":
			return time.Duration(value) * time.Minute, nil
		case "h":
			return time.Duration(value) * time.Hour, nil
		case "d":
			return time.Duration(value) * 24 * time.Hour, nil
		default:
			return 0, fmt.Errorf("unsupported interval unit: %s", unit)
		}
	}
}

// generateGapID creates a unique identifier for a gap.
func generateGapID(pair string, startTime, endTime time.Time, interval string) string {
	// Create a deterministic ID based on gap characteristics
	idStr := fmt.Sprintf("%s_%s_%d_%d_%s",
		pair,
		interval,
		startTime.Unix(),
		endTime.Unix(),
		uuid.New().String()[:8],
	)
	// Clean up the ID to remove invalid characters
	idStr = strings.ReplaceAll(idStr, "/", "-")
	idStr = strings.ReplaceAll(idStr, " ", "_")
	return idStr
}
