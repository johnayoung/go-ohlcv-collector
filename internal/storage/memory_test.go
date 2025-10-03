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

func TestMemoryStorage_GetGapsByStatus(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	err := storage.Initialize(ctx)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Second)

	// Create gaps with different statuses
	gaps := []models.Gap{
		{
			ID:        "gap-detected-1",
			Pair:      "BTC-USD",
			Interval:  "1h",
			StartTime: now.Add(-3 * time.Hour),
			EndTime:   now.Add(-2 * time.Hour),
			Status:    models.GapStatusDetected,
			CreatedAt: now.Add(-1 * time.Hour),
			Priority:  models.PriorityHigh,
			Attempts:  0,
		},
		{
			ID:        "gap-detected-2",
			Pair:      "ETH-USD",
			Interval:  "5m",
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-1 * time.Hour),
			Status:    models.GapStatusDetected,
			CreatedAt: now.Add(-30 * time.Minute),
			Priority:  models.PriorityMedium,
			Attempts:  0,
		},
		{
			ID:        "gap-filling-1",
			Pair:      "BTC-USD",
			Interval:  "1h",
			StartTime: now.Add(-4 * time.Hour),
			EndTime:   now.Add(-3 * time.Hour),
			Status:    models.GapStatusFilling,
			CreatedAt: now.Add(-2 * time.Hour),
			Priority:  models.PriorityMedium,
			Attempts:  1,
		},
		{
			ID:        "gap-filled-1",
			Pair:      "BTC-USD",
			Interval:  "1h",
			StartTime: now.Add(-5 * time.Hour),
			EndTime:   now.Add(-4 * time.Hour),
			Status:    models.GapStatusFilled,
			CreatedAt: now.Add(-3 * time.Hour),
			FilledAt:  func() *time.Time { t := now.Add(-2 * time.Hour); return &t }(),
			Priority:  models.PriorityMedium,
			Attempts:  1,
		},
		{
			ID:        "gap-permanent-1",
			Pair:      "BTC-USD",
			Interval:  "1h",
			StartTime: now.Add(-6 * time.Hour),
			EndTime:   now.Add(-5 * time.Hour),
			Status:    models.GapStatusPermanent,
			CreatedAt: now.Add(-4 * time.Hour),
			Priority:  models.PriorityLow,
			Attempts:  3,
		},
	}

	// Store all gaps
	for _, gap := range gaps {
		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err)
	}

	// Test getting detected gaps
	detectedGaps, err := storage.GetGapsByStatus(ctx, models.GapStatusDetected)
	require.NoError(t, err)
	assert.Len(t, detectedGaps, 2)
	// Verify they're sorted by priority (DESC) then created_at (ASC)
	assert.Equal(t, "gap-detected-1", detectedGaps[0].ID) // High priority, created earlier
	assert.Equal(t, "gap-detected-2", detectedGaps[1].ID) // Medium priority, created later

	// Test getting filling gaps
	fillingGaps, err := storage.GetGapsByStatus(ctx, models.GapStatusFilling)
	require.NoError(t, err)
	assert.Len(t, fillingGaps, 1)
	assert.Equal(t, "gap-filling-1", fillingGaps[0].ID)

	// Test getting filled gaps
	filledGaps, err := storage.GetGapsByStatus(ctx, models.GapStatusFilled)
	require.NoError(t, err)
	assert.Len(t, filledGaps, 1)
	assert.Equal(t, "gap-filled-1", filledGaps[0].ID)

	// Test getting permanent gaps
	permanentGaps, err := storage.GetGapsByStatus(ctx, models.GapStatusPermanent)
	require.NoError(t, err)
	assert.Len(t, permanentGaps, 1)
	assert.Equal(t, "gap-permanent-1", permanentGaps[0].ID)

	// Test getting gaps with status that doesn't exist
	nonExistentGaps, err := storage.GetGapsByStatus(ctx, models.GapStatus("nonexistent"))
	require.NoError(t, err)
	assert.Len(t, nonExistentGaps, 0)
}
