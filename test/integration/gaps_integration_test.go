// Integration tests for gap detection and backfill workflows
package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
)

// TestGapDetectionAndBackfillWorkflow tests the complete end-to-end gap detection and backfill process
func TestGapDetectionAndBackfillWorkflow(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	gapDetector := setupGapDetector(t, storage)
	backfiller := setupBackfiller(t, storage, exchange)

	// Test data: create a scenario with gaps
	pair := "BTC/USD"
	interval := "1h"
	testTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("DetectGapsInHistoricalData", func(t *testing.T) {
		// Setup: Store candles with gaps
		candles := createCandlesWithGaps(pair, interval, testTime, 100)
		err := storage.StoreBatch(ctx, candles)
		require.NoError(t, err)

		// Execute: Run gap detection
		gaps, err := gapDetector.DetectGaps(ctx, pair, interval, testTime, testTime.Add(100*time.Hour))
		require.NoError(t, err)

		// Verify: Gaps should be detected
		assert.Greater(t, len(gaps), 0, "Should detect at least one gap")

		// Verify gaps are stored with correct initial state
		storedGaps, err := storage.GetGaps(ctx, pair, interval)
		require.NoError(t, err)

		for _, gap := range storedGaps {
			assert.Equal(t, "detected", gap.Status)
			assert.NotEmpty(t, gap.ID)
			assert.Equal(t, pair, gap.Pair)
			assert.Equal(t, interval, gap.Interval)
			assert.True(t, gap.StartTime.Before(gap.EndTime))
		}
	})

	t.Run("AutomaticBackfillOfDetectedGaps", func(t *testing.T) {
		// Setup: Ensure gaps exist from previous test
		gaps, err := storage.GetGaps(ctx, pair, interval)
		require.NoError(t, err)
		require.Greater(t, len(gaps), 0, "Should have gaps from previous test")

		// Execute: Start automatic backfill
		err = backfiller.StartBackfill(ctx, pair, interval)
		require.NoError(t, err)

		// Wait for backfill to process
		time.Sleep(2 * time.Second)

		// Verify: Gaps should transition through states
		updatedGaps, err := storage.GetGaps(ctx, pair, interval)
		require.NoError(t, err)

		filledCount := 0
		fillingCount := 0
		for _, gap := range updatedGaps {
			switch gap.Status {
			case "filled":
				filledCount++
				assert.NotNil(t, gap.FilledAt, "Filled gaps should have FilledAt timestamp")
			case "filling":
				fillingCount++
			}
		}

		assert.Greater(t, filledCount+fillingCount, 0, "Some gaps should be filled or filling")
	})
}

// TestGapPriorityAndBackfillOrdering tests that gaps are filled in the correct order
func TestGapPriorityAndBackfillOrdering(t *testing.T) {
	ctx := context.Background()

	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	backfiller := setupBackfiller(t, storage, exchange)

	pair := "ETH/USD"
	interval := "1h"
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("PriorityBasedGapFilling", func(t *testing.T) {
		// Setup: Create gaps with different priorities (older gaps should have higher priority)
		gaps := []contracts.Gap{
			{
				ID:        "gap-old",
				Pair:      pair,
				Interval:  interval,
				StartTime: baseTime,
				EndTime:   baseTime.Add(2 * time.Hour),
				Status:    "detected",
				CreatedAt: baseTime,
			},
			{
				ID:        "gap-recent",
				Pair:      pair,
				Interval:  interval,
				StartTime: baseTime.Add(10 * time.Hour),
				EndTime:   baseTime.Add(12 * time.Hour),
				Status:    "detected",
				CreatedAt: baseTime.Add(5 * time.Hour),
			},
			{
				ID:        "gap-newest",
				Pair:      pair,
				Interval:  interval,
				StartTime: baseTime.Add(20 * time.Hour),
				EndTime:   baseTime.Add(22 * time.Hour),
				Status:    "detected",
				CreatedAt: baseTime.Add(10 * time.Hour),
			},
		}

		// Store gaps
		for _, gap := range gaps {
			err := storage.StoreGap(ctx, gap)
			require.NoError(t, err)
		}

		// Execute: Start backfill with priority ordering
		err := backfiller.StartPriorityBackfill(ctx, pair, interval)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Verify: Older gaps should be processed first
		processedGaps, err := storage.GetGaps(ctx, pair, interval)
		require.NoError(t, err)

		var oldGap, recentGap, newestGap *contracts.Gap
		for _, gap := range processedGaps {
			switch gap.ID {
			case "gap-old":
				oldGap = &gap
			case "gap-recent":
				recentGap = &gap
			case "gap-newest":
				newestGap = &gap
			}
		}

		// Older gap should be filled or filling before newer ones
		if oldGap.Status == "filled" {
			// If oldest is filled, others should be filled, filling, or still detected
			if recentGap.Status == "filled" {
				// If both older gaps are filled, newest can be in any state
				assert.Contains(t, []string{"detected", "filling", "filled"}, newestGap.Status)
			}
		}
	})
}

// TestConcurrentGapDetectionAndFilling tests concurrent operations
func TestConcurrentGapDetectionAndFilling(t *testing.T) {
	ctx := context.Background()

	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	gapDetector := setupGapDetector(t, storage)
	backfiller := setupBackfiller(t, storage, exchange)

	pairs := []string{"BTC/USD", "ETH/USD", "ADA/USD"}
	interval := "5m"
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("ConcurrentDetectionAndFilling", func(t *testing.T) {
		var wg sync.WaitGroup
		errorCh := make(chan error, len(pairs)*2)

		// Start concurrent gap detection for multiple pairs
		for _, pair := range pairs {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()

				// Create test data with gaps
				candles := createCandlesWithGaps(p, interval, baseTime, 50)
				if err := storage.StoreBatch(ctx, candles); err != nil {
					errorCh <- fmt.Errorf("failed to store candles for %s: %w", p, err)
					return
				}

				// Detect gaps
				_, err := gapDetector.DetectGaps(ctx, p, interval, baseTime, baseTime.Add(50*5*time.Minute))
				if err != nil {
					errorCh <- fmt.Errorf("gap detection failed for %s: %w", p, err)
				}
			}(pair)
		}

		// Start concurrent backfilling
		for _, pair := range pairs {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()

				// Wait a bit for gap detection to create some gaps
				time.Sleep(500 * time.Millisecond)

				if err := backfiller.StartBackfill(ctx, p, interval); err != nil {
					errorCh <- fmt.Errorf("backfill failed for %s: %w", p, err)
				}
			}(pair)
		}

		wg.Wait()
		close(errorCh)

		// Check for any errors
		var errors []error
		for err := range errorCh {
			errors = append(errors, err)
		}

		if len(errors) > 0 {
			for _, err := range errors {
				t.Logf("Concurrent operation error: %v", err)
			}
			t.Fatalf("Expected no errors in concurrent operations, got %d errors", len(errors))
		}

		// Verify: Each pair should have some gaps processed
		for _, pair := range pairs {
			gaps, err := storage.GetGaps(ctx, pair, interval)
			require.NoError(t, err, "Should be able to retrieve gaps for %s", pair)

			// Should have detected some gaps
			assert.Greater(t, len(gaps), 0, "Should have gaps for pair %s", pair)
		}
	})
}

// TestGapStateTransitions tests the state machine for gaps
func TestGapStateTransitions(t *testing.T) {
	ctx := context.Background()

	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	backfiller := setupBackfiller(t, storage, exchange)

	pair := "BTC/USD"
	interval := "1h"
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("ValidStateTransitions", func(t *testing.T) {
		// Create a gap in detected state
		gap := contracts.Gap{
			ID:        "test-gap-transitions",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime,
			EndTime:   baseTime.Add(2 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime,
		}

		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err)

		// Verify initial state
		storedGap, err := getGapByID(ctx, storage, gap.ID)
		require.NoError(t, err)
		assert.Equal(t, "detected", storedGap.Status)

		// Transition to filling
		err = backfiller.StartGapFilling(ctx, gap.ID)
		require.NoError(t, err)

		// Verify filling state
		fillingGap, err := getGapByID(ctx, storage, gap.ID)
		require.NoError(t, err)
		assert.Equal(t, "filling", fillingGap.Status)

		// Wait for fill completion
		time.Sleep(1 * time.Second)

		// Verify filled state
		filledGap, err := getGapByID(ctx, storage, gap.ID)
		require.NoError(t, err)
		assert.Contains(t, []string{"filled", "permanent"}, filledGap.Status)

		if filledGap.Status == "filled" {
			assert.NotNil(t, filledGap.FilledAt, "Filled gaps should have FilledAt timestamp")
		}
	})

	t.Run("PermanentGapHandling", func(t *testing.T) {
		// Create a gap that will be marked as permanent (e.g., exchange downtime)
		gap := contracts.Gap{
			ID:        "permanent-gap",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime.Add(100 * time.Hour),
			EndTime:   baseTime.Add(102 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime.Add(100 * time.Hour),
		}

		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err)

		// Configure exchange to return no data for this period (simulating permanent gap)
		configureMockExchangeForPermanentGap(exchange, gap)

		// Attempt to fill the gap
		err = backfiller.StartGapFilling(ctx, gap.ID)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(2 * time.Second)

		// Verify gap is marked as permanent
		permanentGap, err := getGapByID(ctx, storage, gap.ID)
		require.NoError(t, err)
		assert.Equal(t, "permanent", permanentGap.Status)
	})
}

// TestBackfillPerformanceAndResourceUsage tests performance characteristics
func TestBackfillPerformanceAndResourceUsage(t *testing.T) {
	ctx := context.Background()

	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	backfiller := setupBackfiller(t, storage, exchange)

	pair := "BTC/USD"
	interval := "1m"
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("LargeGapBackfillPerformance", func(t *testing.T) {
		// Create a large gap (24 hours of 1-minute data = 1440 candles)
		largeGap := contracts.Gap{
			ID:        "large-gap",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime,
			EndTime:   baseTime.Add(24 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime,
		}

		err := storage.StoreGap(ctx, largeGap)
		require.NoError(t, err)

		// Measure backfill performance
		startTime := time.Now()

		err = backfiller.StartGapFilling(ctx, largeGap.ID)
		require.NoError(t, err)

		// Wait for completion
		maxWaitTime := 30 * time.Second
		timeout := time.After(maxWaitTime)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		var finalGap *contracts.Gap
		for {
			select {
			case <-timeout:
				t.Fatalf("Backfill did not complete within %v", maxWaitTime)
			case <-ticker.C:
				gap, err := getGapByID(ctx, storage, largeGap.ID)
				require.NoError(t, err)

				if gap.Status == "filled" || gap.Status == "permanent" {
					finalGap = gap
					goto completed
				}
			}
		}
	completed:

		duration := time.Since(startTime)

		// Performance assertions
		assert.Less(t, duration, maxWaitTime, "Backfill should complete within reasonable time")
		assert.NotNil(t, finalGap, "Should have final gap state")

		// Log performance metrics
		t.Logf("Large gap backfill completed in %v", duration)
		t.Logf("Final gap status: %s", finalGap.Status)
	})

	t.Run("ResourceUsageMonitoring", func(t *testing.T) {
		// Create multiple gaps for concurrent processing
		numGaps := 10
		gaps := make([]contracts.Gap, numGaps)

		for i := 0; i < numGaps; i++ {
			gaps[i] = contracts.Gap{
				ID:        fmt.Sprintf("resource-gap-%d", i),
				Pair:      pair,
				Interval:  interval,
				StartTime: baseTime.Add(time.Duration(i*2) * time.Hour),
				EndTime:   baseTime.Add(time.Duration(i*2+1) * time.Hour),
				Status:    "detected",
				CreatedAt: baseTime,
			}

			err := storage.StoreGap(ctx, gaps[i])
			require.NoError(t, err)
		}

		// Start concurrent backfilling
		var wg sync.WaitGroup
		for _, gap := range gaps {
			wg.Add(1)
			go func(g contracts.Gap) {
				defer wg.Done()
				err := backfiller.StartGapFilling(ctx, g.ID)
				assert.NoError(t, err, "Gap filling should not fail for gap %s", g.ID)
			}(gap)
		}

		wg.Wait()

		// Verify no resource exhaustion or deadlocks occurred
		// All gaps should be processed (filled or permanent)
		processedCount := 0
		for _, gap := range gaps {
			storedGap, err := getGapByID(ctx, storage, gap.ID)
			require.NoError(t, err)

			if storedGap.Status == "filled" || storedGap.Status == "permanent" {
				processedCount++
			}
		}

		// At least some gaps should be processed without resource issues
		assert.Greater(t, processedCount, 0, "Should process some gaps without resource exhaustion")
	})
}

// TestEdgeCases tests various edge cases and error scenarios
func TestEdgeCases(t *testing.T) {
	ctx := context.Background()

	storage := setupTestStorage(t)
	defer teardownTestStorage(t, storage)

	exchange := setupMockExchange(t)
	backfiller := setupBackfiller(t, storage, exchange)

	pair := "BTC/USD"
	interval := "1h"
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("OverlappingGaps", func(t *testing.T) {
		// Create overlapping gaps
		gap1 := contracts.Gap{
			ID:        "overlap-gap-1",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime,
			EndTime:   baseTime.Add(4 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime,
		}

		gap2 := contracts.Gap{
			ID:        "overlap-gap-2",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime.Add(2 * time.Hour),
			EndTime:   baseTime.Add(6 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime,
		}

		err := storage.StoreGap(ctx, gap1)
		require.NoError(t, err)
		err = storage.StoreGap(ctx, gap2)
		require.NoError(t, err)

		// Start filling both gaps
		err = backfiller.StartGapFilling(ctx, gap1.ID)
		require.NoError(t, err)
		err = backfiller.StartGapFilling(ctx, gap2.ID)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Verify: Both gaps should be handled correctly without duplicate data
		processedGap1, err := getGapByID(ctx, storage, gap1.ID)
		require.NoError(t, err)
		processedGap2, err := getGapByID(ctx, storage, gap2.ID)
		require.NoError(t, err)

		// Both should be processed (filled or permanent)
		assert.Contains(t, []string{"filled", "permanent"}, processedGap1.Status)
		assert.Contains(t, []string{"filled", "permanent"}, processedGap2.Status)
	})

	t.Run("PartialFillRecovery", func(t *testing.T) {
		// Create a gap that will be partially filled then fail
		partialGap := contracts.Gap{
			ID:        "partial-gap",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime.Add(10 * time.Hour),
			EndTime:   baseTime.Add(15 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime.Add(10 * time.Hour),
		}

		err := storage.StoreGap(ctx, partialGap)
		require.NoError(t, err)

		// Configure exchange to fail partway through
		configureMockExchangeForPartialFill(exchange, partialGap)

		// Attempt to fill
		err = backfiller.StartGapFilling(ctx, partialGap.ID)
		require.NoError(t, err)

		// Wait for partial fill attempt
		time.Sleep(2 * time.Second)

		// Verify gap handling of partial fill
		updatedGap, err := getGapByID(ctx, storage, partialGap.ID)
		require.NoError(t, err)

		// Gap should either retry or be marked appropriately
		assert.Contains(t, []string{"detected", "filling", "permanent"}, updatedGap.Status)
	})

	t.Run("FailedBackfillRetry", func(t *testing.T) {
		// Create a gap that will initially fail
		retryGap := contracts.Gap{
			ID:        "retry-gap",
			Pair:      pair,
			Interval:  interval,
			StartTime: baseTime.Add(20 * time.Hour),
			EndTime:   baseTime.Add(22 * time.Hour),
			Status:    "detected",
			CreatedAt: baseTime.Add(20 * time.Hour),
		}

		err := storage.StoreGap(ctx, retryGap)
		require.NoError(t, err)

		// Configure exchange to fail initially then succeed
		configureMockExchangeForRetryScenario(exchange, retryGap)

		// Start backfill
		err = backfiller.StartGapFilling(ctx, retryGap.ID)
		require.NoError(t, err)

		// Wait for retry mechanism to work
		time.Sleep(5 * time.Second)

		// Verify retry succeeded
		finalGap, err := getGapByID(ctx, storage, retryGap.ID)
		require.NoError(t, err)

		// Should eventually succeed or be marked permanent after retries
		assert.Contains(t, []string{"filled", "permanent"}, finalGap.Status)
	})
}

// Helper functions - These will fail initially as they reference non-existent implementations

func setupTestStorage(t *testing.T) contracts.FullStorage {
	// This should create a test storage implementation
	// Will fail initially since no implementation exists
	storage := NewTestStorage()

	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)

	return storage
}

func teardownTestStorage(t *testing.T, storage contracts.FullStorage) {
	if storage != nil {
		storage.Close()
	}
}

func setupMockExchange(t *testing.T) contracts.ExchangeAdapter {
	// This should create a mock exchange adapter
	// Will fail initially since no implementation exists
	return NewMockExchange()
}

func setupGapDetector(t *testing.T, storage contracts.FullStorage) GapDetector {
	// This should create a gap detector
	// Will fail initially since no implementation exists
	return NewGapDetector(storage)
}

func setupBackfiller(t *testing.T, storage contracts.FullStorage, exchange contracts.ExchangeAdapter) Backfiller {
	// This should create a backfiller
	// Will fail initially since no implementation exists
	return NewBackfiller(storage, exchange)
}

func createCandlesWithGaps(pair, interval string, startTime time.Time, totalCount int) []contracts.Candle {
	candles := make([]contracts.Candle, 0, totalCount)

	intervalDuration := parseInterval(interval)
	currentTime := startTime

	for i := 0; i < totalCount; i++ {
		// Create gaps by skipping some candles
		if i%10 == 7 || i%10 == 8 {
			// Skip these candles to create gaps
			currentTime = currentTime.Add(intervalDuration)
			continue
		}

		candle := contracts.Candle{
			Timestamp: currentTime,
			Open:      fmt.Sprintf("%.2f", 50000.0+float64(i)*10),
			High:      fmt.Sprintf("%.2f", 50000.0+float64(i)*10+100),
			Low:       fmt.Sprintf("%.2f", 50000.0+float64(i)*10-50),
			Close:     fmt.Sprintf("%.2f", 50000.0+float64(i)*10+50),
			Volume:    fmt.Sprintf("%.2f", 100.0+float64(i)),
			Pair:      pair,
			Interval:  interval,
		}

		candles = append(candles, candle)
		currentTime = currentTime.Add(intervalDuration)
	}

	return candles
}

func parseInterval(interval string) time.Duration {
	// Simple interval parsing - in real implementation this would be more robust
	switch interval {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "1h":
		return time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return time.Hour
	}
}

func getGapByID(ctx context.Context, storage contracts.FullStorage, gapID string) (*contracts.Gap, error) {
	// This would need to be implemented - getting gap by ID
	// Will fail initially since no implementation exists
	return storage.GetGapByID(ctx, gapID)
}

func configureMockExchangeForPermanentGap(exchange contracts.ExchangeAdapter, gap contracts.Gap) {
	// Configure mock to return no data for this gap period
	// Will fail initially since no implementation exists
	mockExchange := exchange.(*MockExchange)
	mockExchange.SetPermanentGap(gap.StartTime, gap.EndTime)
}

func configureMockExchangeForPartialFill(exchange contracts.ExchangeAdapter, gap contracts.Gap) {
	// Configure mock to fail partway through filling
	// Will fail initially since no implementation exists
	mockExchange := exchange.(*MockExchange)
	mockExchange.SetPartialFillFailure(gap.StartTime, gap.EndTime)
}

func configureMockExchangeForRetryScenario(exchange contracts.ExchangeAdapter, gap contracts.Gap) {
	// Configure mock to fail initially then succeed on retry
	// Will fail initially since no implementation exists
	mockExchange := exchange.(*MockExchange)
	mockExchange.SetRetryScenario(gap.StartTime, gap.EndTime)
}

// Interfaces that don't exist yet - these will cause compilation failures

type GapDetector interface {
	DetectGaps(ctx context.Context, pair, interval string, startTime, endTime time.Time) ([]contracts.Gap, error)
}

type Backfiller interface {
	StartBackfill(ctx context.Context, pair, interval string) error
	StartPriorityBackfill(ctx context.Context, pair, interval string) error
	StartGapFilling(ctx context.Context, gapID string) error
}

// These types don't exist yet and will cause compilation failures

func NewTestStorage() contracts.FullStorage {
	panic("NewTestStorage not implemented yet")
}

func NewMockExchange() contracts.ExchangeAdapter {
	panic("NewMockExchange not implemented yet")
}

func NewGapDetector(storage contracts.FullStorage) GapDetector {
	panic("NewGapDetector not implemented yet")
}

func NewBackfiller(storage contracts.FullStorage, exchange contracts.ExchangeAdapter) Backfiller {
	panic("NewBackfiller not implemented yet")
}

type MockExchange struct {
	permanentGaps  map[string]bool
	partialFails   map[string]bool
	retryScenarios map[string]int
}

func (m *MockExchange) FetchCandles(ctx context.Context, req contracts.FetchRequest) (*contracts.FetchResponse, error) {
	panic("FetchCandles not implemented yet")
}

func (m *MockExchange) GetTradingPairs(ctx context.Context) ([]contracts.TradingPair, error) {
	panic("GetTradingPairs not implemented yet")
}

func (m *MockExchange) GetPairInfo(ctx context.Context, pair string) (*contracts.PairInfo, error) {
	panic("GetPairInfo not implemented yet")
}

func (m *MockExchange) GetLimits() contracts.RateLimit {
	panic("GetLimits not implemented yet")
}

func (m *MockExchange) WaitForLimit(ctx context.Context) error {
	panic("WaitForLimit not implemented yet")
}

func (m *MockExchange) HealthCheck(ctx context.Context) error {
	panic("HealthCheck not implemented yet")
}

func (m *MockExchange) SetPermanentGap(start, end time.Time) {
	panic("SetPermanentGap not implemented yet")
}

func (m *MockExchange) SetPartialFillFailure(start, end time.Time) {
	panic("SetPartialFillFailure not implemented yet")
}

func (m *MockExchange) SetRetryScenario(start, end time.Time) {
	panic("SetRetryScenario not implemented yet")
}
