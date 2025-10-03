package storage

import (
	"context"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_BasicOperations(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Initialize storage
	err := storage.Initialize(ctx)
	require.NoError(t, err)

	// Test storing candles
	candles := []models.Candle{
		{
			Timestamp: time.Now().Add(-2 * time.Hour),
			Open:      "50000.00",
			High:      "51000.00",
			Low:       "49000.00",
			Close:     "50500.00",
			Volume:    "100.5",
			Pair:      "BTC/USD",
			Interval:  "1h",
		},
		{
			Timestamp: time.Now().Add(-1 * time.Hour),
			Open:      "50500.00",
			High:      "52000.00",
			Low:       "50000.00",
			Close:     "51500.00",
			Volume:    "150.2",
			Pair:      "BTC/USD",
			Interval:  "1h",
		},
	}

	err = storage.Store(ctx, candles)
	require.NoError(t, err)

	// Test querying candles
	req := QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-3 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    10,
		OrderBy:  "timestamp_asc",
	}

	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 2)
	assert.Equal(t, 2, resp.Total)
	assert.False(t, resp.HasMore)

	// Test getting latest candle
	latest, err := storage.GetLatest(ctx, "BTC/USD", "1h")
	require.NoError(t, err)
	assert.Equal(t, "51500.00", latest.Close)

	// Test health check
	err = storage.HealthCheck(ctx)
	require.NoError(t, err)

	// Test stats
	stats, err := storage.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.TotalCandles)
	assert.Equal(t, 1, stats.TotalPairs)

	// Test closing
	err = storage.Close()
	require.NoError(t, err)
}

func TestMemoryStorage_GapOperations(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)

	now := time.Now().UTC()

	// Create a gap
	gap := models.Gap{
		ID:        "test-gap-001",
		Pair:      "BTC/USD",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    models.GapStatusDetected,
		CreatedAt: now,
	}

	// Store gap
	err = storage.StoreGap(ctx, gap)
	require.NoError(t, err)

	// Get gaps
	gaps, err := storage.GetGaps(ctx, "BTC/USD", "1h")
	require.NoError(t, err)
	assert.Len(t, gaps, 1)
	assert.Equal(t, "test-gap-001", gaps[0].ID)

	// Get gap by ID
	retrievedGap, err := storage.GetGapByID(ctx, "test-gap-001")
	require.NoError(t, err)
	assert.Equal(t, gap.ID, retrievedGap.ID)

	// Mark gap as filled
	filledAt := now
	err = storage.MarkGapFilled(ctx, "test-gap-001", filledAt)
	require.NoError(t, err)

	// Verify gap is marked as filled
	updatedGap, err := storage.GetGapByID(ctx, "test-gap-001")
	require.NoError(t, err)
	assert.Equal(t, models.GapStatusFilled, updatedGap.Status)
	assert.NotNil(t, updatedGap.FilledAt)

	// Delete gap
	err = storage.DeleteGap(ctx, "test-gap-001")
	require.NoError(t, err)

	// Verify gap is deleted
	_, err = storage.GetGapByID(ctx, "test-gap-001")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap not found")
}

func TestMemoryStorage_GetGapsByStatus(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)

	now := time.Now().UTC()

	// Create gaps with different statuses
	gaps := []models.Gap{
		{
			ID:        "gap-detected-001",
			Pair:      "BTC/USD",
			Interval:  "1h",
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-1 * time.Hour),
			Status:    models.GapStatusDetected,
			CreatedAt: now.Add(-10 * time.Minute),
			Priority:  models.PriorityHigh,
		},
		{
			ID:        "gap-detected-002",
			Pair:      "ETH/USD",
			Interval:  "1h",
			StartTime: now.Add(-3 * time.Hour),
			EndTime:   now.Add(-2 * time.Hour),
			Status:    models.GapStatusDetected,
			CreatedAt: now.Add(-15 * time.Minute),
			Priority:  models.PriorityMedium,
		},
		{
			ID:        "gap-filling-001",
			Pair:      "BTC/USD",
			Interval:  "1h",
			StartTime: now.Add(-4 * time.Hour),
			EndTime:   now.Add(-3 * time.Hour),
			Status:    models.GapStatusFilling,
			CreatedAt: now.Add(-20 * time.Minute),
			Priority:  models.PriorityMedium,
		},
		{
			ID:        "gap-filled-001",
			Pair:      "BTC/USD",
			Interval:  "1h",
			StartTime: now.Add(-6 * time.Hour),
			EndTime:   now.Add(-5 * time.Hour),
			Status:    models.GapStatusFilled,
			CreatedAt: now.Add(-30 * time.Minute),
			Priority:  models.PriorityLow,
			FilledAt:  &[]time.Time{now.Add(-5 * time.Minute)}[0],
		},
		{
			ID:        "gap-permanent-001",
			Pair:      "ETH/USD",
			Interval:  "1h",
			StartTime: now.Add(-8 * time.Hour),
			EndTime:   now.Add(-7 * time.Hour),
			Status:    models.GapStatusPermanent,
			CreatedAt: now.Add(-40 * time.Minute),
			Priority:  models.PriorityLow,
		},
	}

	// Store all gaps
	for _, gap := range gaps {
		err = storage.StoreGap(ctx, gap)
		require.NoError(t, err)
	}

	// Test getting gaps by status
	tests := []struct {
		name          string
		status        models.GapStatus
		expectedCount int
		expectedIDs   []string
	}{
		{
			name:          "get_detected_gaps",
			status:        models.GapStatusDetected,
			expectedCount: 2,
			expectedIDs:   []string{"gap-detected-001", "gap-detected-002"},
		},
		{
			name:          "get_filling_gaps",
			status:        models.GapStatusFilling,
			expectedCount: 1,
			expectedIDs:   []string{"gap-filling-001"},
		},
		{
			name:          "get_filled_gaps",
			status:        models.GapStatusFilled,
			expectedCount: 1,
			expectedIDs:   []string{"gap-filled-001"},
		},
		{
			name:          "get_permanent_gaps",
			status:        models.GapStatusPermanent,
			expectedCount: 1,
			expectedIDs:   []string{"gap-permanent-001"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := storage.GetGapsByStatus(ctx, tt.status)
			require.NoError(t, err)
			assert.Len(t, result, tt.expectedCount)

			// Verify gap IDs
			gotIDs := make([]string, len(result))
			for i, gap := range result {
				gotIDs[i] = gap.ID
			}
			assert.ElementsMatch(t, tt.expectedIDs, gotIDs)

			// Verify all gaps have the correct status
			for _, gap := range result {
				assert.Equal(t, tt.status, gap.Status)
			}

			// Verify gaps are ordered by priority (descending) and created_at (ascending)
			if len(result) > 1 {
				for i := 0; i < len(result)-1; i++ {
					if result[i].Priority == result[i+1].Priority {
						assert.True(t, result[i].CreatedAt.Before(result[i+1].CreatedAt) || result[i].CreatedAt.Equal(result[i+1].CreatedAt),
							"Gaps with same priority should be ordered by creation time (ascending)")
					} else {
						assert.True(t, result[i].Priority > result[i+1].Priority,
							"Gaps should be ordered by priority (descending)")
					}
				}
			}
		})
	}
}

func TestMemoryStorage_EdgeCases(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)

	// Test empty candle store
	err = storage.Store(ctx, nil)
	require.NoError(t, err)

	err = storage.Store(ctx, []models.Candle{})
	require.NoError(t, err)

	// Test invalid candle
	invalidCandles := []models.Candle{
		{
			Timestamp: time.Time{}, // Invalid timestamp
			Open:      "50000.00",
			High:      "51000.00",
			Low:       "49000.00",
			Close:     "50500.00",
			Volume:    "100.5",
			Pair:      "BTC/USD",
			Interval:  "1h",
		},
	}

	err = storage.Store(ctx, invalidCandles)
	require.Error(t, err)

	// Test query with no results
	req := QueryRequest{
		Pair:     "NONEXISTENT/PAIR",
		Start:    time.Now().Add(-1 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
	}

	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 0)
	assert.Equal(t, 0, resp.Total)

	// Test GetLatest for nonexistent pair
	_, err = storage.GetLatest(ctx, "NONEXISTENT/PAIR", "1h")
	require.Error(t, err)
}

func TestMemoryStorage_ConcurrentOperations(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	// Test concurrent candle storage
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			candle := models.Candle{
				Timestamp: time.Now().Add(time.Duration(-id) * time.Hour),
				Open:      "50000.00",
				High:      "51000.00",
				Low:       "49000.00",
				Close:     "50500.00",
				Volume:    "100.5",
				Pair:      "BTC/USD",
				Interval:  "1h",
			}

			err := storage.Store(ctx, []models.Candle{candle})
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all candles were stored
	req := QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(time.Duration(-numGoroutines-1) * time.Hour),
		End:      time.Now().Add(1 * time.Hour),
		Interval: "1h",
	}

	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, numGoroutines, len(resp.Candles))
}
