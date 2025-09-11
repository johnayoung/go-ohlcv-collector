package storage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestDuckDBStorage creates a new in-memory DuckDB storage for testing
func createTestDuckDBStorage(t *testing.T) *DuckDBStorage {
	t.Helper()
	
	logger := slog.Default()
	storage, err := NewDuckDBStorage(":memory:", logger)
	require.NoError(t, err, "failed to create test DuckDB storage")
	
	return storage
}

// createTestCandles generates realistic test OHLCV data
func createTestCandles(pair string, interval string, count int, startTime time.Time) []models.Candle {
	candles := make([]models.Candle, count)
	basePrice := 50000.0
	
	for i := 0; i < count; i++ {
		// Calculate timestamp based on interval
		var timestamp time.Time
		switch interval {
		case "1m":
			timestamp = startTime.Add(time.Duration(i) * time.Minute)
		case "5m":
			timestamp = startTime.Add(time.Duration(i) * 5 * time.Minute)
		case "1h":
			timestamp = startTime.Add(time.Duration(i) * time.Hour)
		case "1d":
			timestamp = startTime.Add(time.Duration(i) * 24 * time.Hour)
		default:
			timestamp = startTime.Add(time.Duration(i) * time.Hour)
		}
		
		// Generate realistic OHLCV data with some price movement
		open := basePrice + float64(i)*10 + float64(i%5)*50
		priceRange := open * 0.02 // 2% price range
		high := open + priceRange*0.8
		low := open - priceRange*0.8
		close := open + (float64(i%3)-1)*priceRange*0.5
		volume := 100.0 + float64(i)*5.5
		
		candles[i] = models.Candle{
			Timestamp: timestamp,
			Open:      fmt.Sprintf("%.8f", open),
			High:      fmt.Sprintf("%.8f", high),
			Low:       fmt.Sprintf("%.8f", low),
			Close:     fmt.Sprintf("%.8f", close),
			Volume:    fmt.Sprintf("%.8f", volume),
			Pair:      pair,
			Interval:  interval,
		}
	}
	
	return candles
}

func TestDuckDBStorage_NewDuckDBStorage(t *testing.T) {
	tests := []struct {
		name       string
		dbPath     string
		expectError bool
	}{
		{
			name:        "in-memory database",
			dbPath:      ":memory:",
			expectError: false,
		},
		{
			name:        "valid file path",
			dbPath:      "/tmp/test.db",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := slog.Default()
			storage, err := NewDuckDBStorage(tt.dbPath, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, storage)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, storage)
				assert.Equal(t, tt.dbPath, storage.dbPath)
				
				// Test connection pool settings
				assert.NotNil(t, storage.db)
				
				// Clean up
				if storage != nil {
					_ = storage.Close()
				}
			}
		})
	}
}

func TestDuckDBStorage_Initialize(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	
	// Test successful initialization
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test idempotent initialization (should not fail on second call)
	err = storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Verify tables were created by checking they exist
	var candlesExists int
	err = storage.db.QueryRowContext(ctx, 
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'candles'").Scan(&candlesExists)
	require.NoError(t, err)
	assert.Equal(t, 1, candlesExists)
	
	var gapsExists int
	err = storage.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'gaps'").Scan(&gapsExists)
	require.NoError(t, err)
	assert.Equal(t, 1, gapsExists)
	
	// Test health check works after initialization
	err = storage.HealthCheck(ctx)
	require.NoError(t, err)
}

func TestDuckDBStorage_StoreBatch_BasicOperations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test storing valid candles
	testTime := time.Now().UTC().Truncate(time.Second)
	candles := createTestCandles("BTC-USD", "1h", 5, testTime)
	
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Verify candles were stored
	var count int
	err = storage.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM candles").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
	
	// Test storing empty batch (should not error)
	err = storage.StoreBatch(ctx, []models.Candle{})
	require.NoError(t, err)
	
	err = storage.StoreBatch(ctx, nil)
	require.NoError(t, err)
}

func TestDuckDBStorage_StoreBatch_InvalidCandles(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test storing invalid candles
	invalidCandles := []models.Candle{
		{
			Timestamp: time.Time{}, // Invalid timestamp
			Open:      "50000.00",
			High:      "51000.00",
			Low:       "49000.00",
			Close:     "50500.00",
			Volume:    "100.5",
			Pair:      "BTC-USD",
			Interval:  "1h",
		},
	}
	
	err = storage.StoreBatch(ctx, invalidCandles)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid candle")
	
	// Test storing candles with invalid decimal format
	invalidDecimalCandles := []models.Candle{
		{
			Timestamp: time.Now(),
			Open:      "invalid_decimal",
			High:      "51000.00",
			Low:       "49000.00",
			Close:     "50500.00",
			Volume:    "100.5",
			Pair:      "BTC-USD",
			Interval:  "1h",
		},
	}
	
	err = storage.StoreBatch(ctx, invalidDecimalCandles)
	require.Error(t, err)
}

func TestDuckDBStorage_StoreBatch_ConstraintViolations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	testTime := time.Now().UTC().Truncate(time.Second)
	
	// Store initial candles
	candles := createTestCandles("BTC-USD", "1h", 2, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Test primary key constraint violation (duplicate candle)
	duplicateCandles := []models.Candle{candles[0]} // Same timestamp, pair, interval
	err = storage.StoreBatch(ctx, duplicateCandles)
	require.Error(t, err)
	// DuckDB reports constraint violations with "primary key constraint" message
	assert.Contains(t, err.Error(), "primary key constraint")
}

func TestDuckDBStorage_Query_BasicOperations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Store test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 10, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Test basic query
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Limit:    5,
		OrderBy:  "timestamp_asc",
	}
	
	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 5)
	assert.Equal(t, 10, resp.Total)
	assert.True(t, resp.HasMore)
	assert.Equal(t, 5, resp.NextOffset)
	assert.Greater(t, resp.QueryTime, time.Duration(0))
	
	// Verify ordering
	for i := 1; i < len(resp.Candles); i++ {
		assert.True(t, resp.Candles[i-1].Timestamp.Before(resp.Candles[i].Timestamp))
	}
}

func TestDuckDBStorage_Query_Filters(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	testTime := time.Now().UTC().Truncate(time.Hour)
	
	// Store data for multiple pairs and intervals
	btcCandles := createTestCandles("BTC-USD", "1h", 5, testTime)
	ethCandles := createTestCandles("ETH-USD", "1h", 5, testTime)
	btcDailyCandles := createTestCandles("BTC-USD", "1d", 5, testTime)
	
	allCandles := append(append(btcCandles, ethCandles...), btcDailyCandles...)
	err = storage.StoreBatch(ctx, allCandles)
	require.NoError(t, err)
	
	// Test pair filter
	req := QueryRequest{
		Pair:    "BTC-USD",
		Limit:   20,
		OrderBy: "timestamp_asc",
	}
	
	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 10, resp.Total) // 5 hourly + 5 daily
	
	for _, candle := range resp.Candles {
		assert.Equal(t, "BTC-USD", candle.Pair)
	}
	
	// Test interval filter
	req = QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Limit:    20,
		OrderBy:  "timestamp_asc",
	}
	
	resp, err = storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 5, resp.Total)
	
	for _, candle := range resp.Candles {
		assert.Equal(t, "BTC-USD", candle.Pair)
		assert.Equal(t, "1h", candle.Interval)
	}
	
	// Test time range filter
	midTime := testTime.Add(2 * time.Hour)
	req = QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    testTime,
		End:      midTime,
		Limit:    20,
		OrderBy:  "timestamp_asc",
	}
	
	resp, err = storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 2, resp.Total) // Only candles at testTime and testTime+1h
	
	for _, candle := range resp.Candles {
		assert.True(t, candle.Timestamp.Before(midTime))
		assert.True(t, candle.Timestamp.Equal(testTime) || candle.Timestamp.After(testTime))
	}
}

func TestDuckDBStorage_Query_Pagination(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Store test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 20, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Test first page
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Limit:    5,
		Offset:   0,
		OrderBy:  "timestamp_asc",
	}
	
	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 5)
	assert.Equal(t, 20, resp.Total)
	assert.True(t, resp.HasMore)
	assert.Equal(t, 5, resp.NextOffset)
	
	// Test middle page
	req.Offset = 10
	resp, err = storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 5)
	assert.Equal(t, 20, resp.Total)
	assert.True(t, resp.HasMore)
	assert.Equal(t, 15, resp.NextOffset)
	
	// Test last page
	req.Offset = 15
	resp, err = storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 5)
	assert.Equal(t, 20, resp.Total)
	assert.False(t, resp.HasMore)
	assert.Equal(t, 20, resp.NextOffset)
}

func TestDuckDBStorage_Query_Ordering(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Store test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 10, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Test ascending order
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Limit:    10,
		OrderBy:  "timestamp_asc",
	}
	
	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 10)
	
	for i := 1; i < len(resp.Candles); i++ {
		assert.True(t, resp.Candles[i-1].Timestamp.Before(resp.Candles[i].Timestamp) ||
			resp.Candles[i-1].Timestamp.Equal(resp.Candles[i].Timestamp))
	}
	
	// Test descending order
	req.OrderBy = "timestamp_desc"
	resp, err = storage.Query(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.Candles, 10)
	
	for i := 1; i < len(resp.Candles); i++ {
		assert.True(t, resp.Candles[i-1].Timestamp.After(resp.Candles[i].Timestamp) ||
			resp.Candles[i-1].Timestamp.Equal(resp.Candles[i].Timestamp))
	}
}

func TestDuckDBStorage_GetLatest(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test GetLatest with no data
	latest, err := storage.GetLatest(ctx, "BTC-USD", "1h")
	require.NoError(t, err)
	assert.Nil(t, latest)
	
	// Store test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 10, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Test GetLatest with data
	latest, err = storage.GetLatest(ctx, "BTC-USD", "1h")
	require.NoError(t, err)
	require.NotNil(t, latest)
	
	// Should be the last candle by timestamp
	expectedTimestamp := testTime.Add(9 * time.Hour) // 10th candle (0-indexed)
	assert.Equal(t, expectedTimestamp, latest.Timestamp)
	assert.Equal(t, "BTC-USD", latest.Pair)
	assert.Equal(t, "1h", latest.Interval)
	
	// Test GetLatest for different pair (should return nil)
	latest, err = storage.GetLatest(ctx, "ETH-USD", "1h")
	require.NoError(t, err)
	assert.Nil(t, latest)
}

func TestDuckDBStorage_GapOperations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	now := time.Now().UTC().Truncate(time.Second)
	
	// Test storing a gap
	gap := models.Gap{
		ID:        "test-gap-001",
		Pair:      "BTC-USD",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    models.GapStatusDetected,
		CreatedAt: now,
		Priority:  models.PriorityMedium,
		Attempts:  0,
	}
	
	err = storage.StoreGap(ctx, gap)
	require.NoError(t, err)
	
	// Test getting gaps
	gaps, err := storage.GetGaps(ctx, "BTC-USD", "1h")
	require.NoError(t, err)
	assert.Len(t, gaps, 1)
	assert.Equal(t, "test-gap-001", gaps[0].ID)
	assert.Equal(t, models.GapStatusDetected, gaps[0].Status)
	
	// Test getting gap by ID
	retrievedGap, err := storage.GetGapByID(ctx, "test-gap-001")
	require.NoError(t, err)
	require.NotNil(t, retrievedGap)
	assert.Equal(t, gap.ID, retrievedGap.ID)
	assert.Equal(t, gap.Pair, retrievedGap.Pair)
	assert.Equal(t, gap.Status, retrievedGap.Status)
	
	// Test getting nonexistent gap by ID
	nonExistentGap, err := storage.GetGapByID(ctx, "nonexistent-gap")
	require.NoError(t, err)
	assert.Nil(t, nonExistentGap)
}

func TestDuckDBStorage_MarkGapFilled(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	now := time.Now().UTC().Truncate(time.Second)
	
	// Create and store a gap with "filling" status
	gap := models.Gap{
		ID:        "test-gap-filling",
		Pair:      "BTC-USD",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    models.GapStatusFilling,
		CreatedAt: now,
		Priority:  models.PriorityMedium,
		Attempts:  1,
	}
	
	err = storage.StoreGap(ctx, gap)
	require.NoError(t, err)
	
	// Mark gap as filled
	filledAt := now.Add(time.Hour)
	err = storage.MarkGapFilled(ctx, "test-gap-filling", filledAt)
	require.NoError(t, err)
	
	// Verify gap status was updated
	updatedGap, err := storage.GetGapByID(ctx, "test-gap-filling")
	require.NoError(t, err)
	require.NotNil(t, updatedGap)
	assert.Equal(t, models.GapStatusFilled, updatedGap.Status)
	assert.NotNil(t, updatedGap.FilledAt)
	assert.Equal(t, filledAt.Unix(), updatedGap.FilledAt.Unix())
	
	// Test marking nonexistent gap as filled
	err = storage.MarkGapFilled(ctx, "nonexistent-gap", filledAt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	
	// Test marking gap with wrong status as filled
	detectedGap := models.Gap{
		ID:        "test-gap-detected",
		Pair:      "BTC-USD",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    models.GapStatusDetected,
		CreatedAt: now,
		Priority:  models.PriorityMedium,
		Attempts:  0,
	}
	
	err = storage.StoreGap(ctx, detectedGap)
	require.NoError(t, err)
	
	err = storage.MarkGapFilled(ctx, "test-gap-detected", filledAt)
	require.Error(t, err)
}

func TestDuckDBStorage_DeleteGap(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	now := time.Now().UTC().Truncate(time.Second)
	
	// Create and store a gap
	gap := models.Gap{
		ID:        "test-gap-delete",
		Pair:      "BTC-USD",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    models.GapStatusDetected,
		CreatedAt: now,
		Priority:  models.PriorityMedium,
		Attempts:  0,
	}
	
	err = storage.StoreGap(ctx, gap)
	require.NoError(t, err)
	
	// Verify gap exists
	retrievedGap, err := storage.GetGapByID(ctx, "test-gap-delete")
	require.NoError(t, err)
	require.NotNil(t, retrievedGap)
	
	// Delete gap
	err = storage.DeleteGap(ctx, "test-gap-delete")
	require.NoError(t, err)
	
	// Verify gap was deleted
	deletedGap, err := storage.GetGapByID(ctx, "test-gap-delete")
	require.NoError(t, err)
	assert.Nil(t, deletedGap)
	
	// Test deleting nonexistent gap
	err = storage.DeleteGap(ctx, "nonexistent-gap")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDuckDBStorage_Migration(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test migration version 1 (should be idempotent)
	err = storage.Migrate(ctx, 1)
	require.NoError(t, err)
	
	// Test migration version 1 again (should not error)
	err = storage.Migrate(ctx, 1)
	require.NoError(t, err)
	
	// Test migration version 2
	err = storage.Migrate(ctx, 2)
	require.NoError(t, err)
	
	// Verify migration was recorded
	var count int
	err = storage.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migrations WHERE version = 2").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	
	// Test unknown migration version
	err = storage.Migrate(ctx, 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown migration version")
}

func TestDuckDBStorage_GetStats(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test stats with empty database
	stats, err := storage.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.TotalCandles)
	assert.Equal(t, 0, stats.TotalPairs)
	assert.True(t, stats.EarliestData.IsZero())
	assert.True(t, stats.LatestData.IsZero())
	
	// Store test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 10, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	ethCandles := createTestCandles("ETH-USD", "1h", 5, testTime.Add(time.Hour))
	err = storage.StoreBatch(ctx, ethCandles)
	require.NoError(t, err)
	
	// Test stats with data
	stats, err = storage.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(15), stats.TotalCandles)
	assert.Equal(t, 2, stats.TotalPairs)
	assert.Equal(t, testTime, stats.EarliestData.Truncate(time.Second))
	assert.NotZero(t, stats.QueryPerformance)
}

func TestDuckDBStorage_HealthCheck(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test health check
	err = storage.HealthCheck(ctx)
	require.NoError(t, err)
}

func TestDuckDBStorage_HealthCheckAfterClose(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test health check after closing database
	err = storage.Close()
	require.NoError(t, err)
	
	err = storage.HealthCheck(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database health check failed")
}

func TestDuckDBStorage_ConcurrentOperations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	const numGoroutines = 10
	const candlesPerGoroutine = 5
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Test concurrent candle storage
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			
			testTime := time.Now().UTC().Truncate(time.Hour).Add(time.Duration(id*100) * time.Hour)
			candles := createTestCandles(fmt.Sprintf("PAIR-%d", id), "1h", candlesPerGoroutine, testTime)
			
			err := storage.StoreBatch(ctx, candles)
			assert.NoError(t, err)
		}(i)
	}
	
	wg.Wait()
	
	// Verify all candles were stored
	stats, err := storage.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(numGoroutines*candlesPerGoroutine), stats.TotalCandles)
	assert.Equal(t, numGoroutines, stats.TotalPairs)
}

func TestDuckDBStorage_ConcurrentGapOperations(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	const numGoroutines = 10
	now := time.Now().UTC().Truncate(time.Second)
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Test concurrent gap storage
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			
			gap := models.Gap{
				ID:        fmt.Sprintf("concurrent-gap-%d", id),
				Pair:      "BTC-USD",
				Interval:  "1h",
				StartTime: now.Add(time.Duration(-id*2) * time.Hour),
				EndTime:   now.Add(time.Duration(-id*2+1) * time.Hour),
				Status:    models.GapStatusDetected,
				CreatedAt: now,
				Priority:  models.PriorityMedium,
				Attempts:  0,
			}
			
			err := storage.StoreGap(ctx, gap)
			assert.NoError(t, err)
		}(i)
	}
	
	wg.Wait()
	
	// Verify all gaps were stored
	gaps, err := storage.GetGaps(ctx, "BTC-USD", "1h")
	require.NoError(t, err)
	assert.Len(t, gaps, numGoroutines)
}

func TestDuckDBStorage_ErrorHandling(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test context cancellation
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	
	candles := createTestCandles("BTC-USD", "1h", 1, time.Now())
	err = storage.StoreBatch(cancelCtx, candles)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
	
	// Test invalid SQL operations (simulated by using closed connection)
	storage.Close()
	
	err = storage.StoreBatch(ctx, candles)
	require.Error(t, err)
}

func TestDuckDBStorage_PerformanceMetrics(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Store test data to generate performance metrics
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 100, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(t, err)
	
	// Perform various operations to generate metrics
	_, err = storage.GetLatest(ctx, "BTC-USD", "1h")
	require.NoError(t, err)
	
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Limit:    10,
		OrderBy:  "timestamp_asc",
	}
	_, err = storage.Query(ctx, req)
	require.NoError(t, err)
	
	err = storage.HealthCheck(ctx)
	require.NoError(t, err)
	
	// Check that performance metrics were recorded
	stats, err := storage.GetStats(ctx)
	require.NoError(t, err)
	
	assert.NotEmpty(t, stats.QueryPerformance)
	assert.Contains(t, stats.QueryPerformance, "insert_batch")
	assert.Contains(t, stats.QueryPerformance, "get_latest")
	assert.Contains(t, stats.QueryPerformance, "query")
	assert.Contains(t, stats.QueryPerformance, "health_check")
	
	// Verify all recorded times are positive
	for operation, duration := range stats.QueryPerformance {
		assert.Greater(t, duration, time.Duration(0), "operation %s should have positive duration", operation)
	}
}

func TestDuckDBStorage_Store_DelegateToStoreBatch(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test that Store delegates to StoreBatch
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 3, testTime)
	
	err = storage.Store(ctx, candles)
	require.NoError(t, err)
	
	// Verify candles were stored
	var count int
	err = storage.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM candles").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestDuckDBStorage_BulkInsertPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}
	
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	// Test bulk insert performance with large dataset
	testTime := time.Now().UTC().Truncate(time.Hour)
	largeDataset := createTestCandles("BTC-USD", "1h", 10000, testTime)
	
	start := time.Now()
	err = storage.StoreBatch(ctx, largeDataset)
	require.NoError(t, err)
	
	insertDuration := time.Since(start)
	rate := float64(len(largeDataset)) / insertDuration.Seconds()
	
	t.Logf("Inserted %d candles in %v (%.2f candles/second)", len(largeDataset), insertDuration, rate)
	
	// Verify performance is reasonable (should handle at least 1000 candles/second)
	assert.Greater(t, rate, 1000.0, "bulk insert should handle at least 1000 candles/second")
	
	// Verify all candles were stored correctly
	stats, err := storage.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeDataset)), stats.TotalCandles)
}

func TestDuckDBStorage_ComplexQuery(t *testing.T) {
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(t, err)
	
	testTime := time.Now().UTC().Truncate(time.Hour)
	
	// Store complex test dataset
	btcHourly := createTestCandles("BTC-USD", "1h", 24, testTime)           // 24 hours of hourly data
	btcDaily := createTestCandles("BTC-USD", "1d", 7, testTime)             // 7 days of daily data
	ethHourly := createTestCandles("ETH-USD", "1h", 12, testTime.Add(6*time.Hour)) // 12 hours offset
	
	err = storage.StoreBatch(ctx, btcHourly)
	require.NoError(t, err)
	err = storage.StoreBatch(ctx, btcDaily)
	require.NoError(t, err)
	err = storage.StoreBatch(ctx, ethHourly)
	require.NoError(t, err)
	
	// Complex query: Get BTC-USD hourly data for a specific time range with pagination
	startRange := testTime.Add(5 * time.Hour)
	endRange := testTime.Add(15 * time.Hour)
	
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    startRange,
		End:      endRange,
		Limit:    5,
		Offset:   2,
		OrderBy:  "timestamp_desc",
	}
	
	resp, err := storage.Query(ctx, req)
	require.NoError(t, err)
	
	// Should get 5 candles from the range (with offset 2)
	assert.Len(t, resp.Candles, 5)
	assert.Equal(t, 10, resp.Total) // 10 hours in range
	assert.True(t, resp.HasMore)
	assert.Equal(t, 7, resp.NextOffset)
	
	// Verify all results are in range and properly ordered (descending)
	for i, candle := range resp.Candles {
		assert.True(t, candle.Timestamp.After(startRange) || candle.Timestamp.Equal(startRange))
		assert.True(t, candle.Timestamp.Before(endRange))
		assert.Equal(t, "BTC-USD", candle.Pair)
		assert.Equal(t, "1h", candle.Interval)
		
		if i > 0 {
			assert.True(t, resp.Candles[i-1].Timestamp.After(candle.Timestamp))
		}
	}
}

func BenchmarkDuckDBStorage_StoreBatch(b *testing.B) {
	// Create a helper testing.T for the createTestDuckDBStorage function
	t := &testing.T{}
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(b, err)
	
	testTime := time.Now().UTC().Truncate(time.Hour)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		candles := createTestCandles("BTC-USD", "1h", 1000, testTime.Add(time.Duration(i*1000)*time.Hour))
		b.StartTimer()
		
		err := storage.StoreBatch(ctx, candles)
		require.NoError(b, err)
	}
}

func BenchmarkDuckDBStorage_Query(b *testing.B) {
	// Create a helper testing.T for the createTestDuckDBStorage function
	t := &testing.T{}
	storage := createTestDuckDBStorage(t)
	defer storage.Close()
	
	ctx := context.Background()
	err := storage.Initialize(ctx)
	require.NoError(b, err)
	
	// Pre-populate with test data
	testTime := time.Now().UTC().Truncate(time.Hour)
	candles := createTestCandles("BTC-USD", "1h", 10000, testTime)
	err = storage.StoreBatch(ctx, candles)
	require.NoError(b, err)
	
	req := QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    testTime,
		End:      testTime.Add(1000 * time.Hour),
		Limit:    100,
		OrderBy:  "timestamp_asc",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := storage.Query(ctx, req)
		require.NoError(b, err)
	}
}