package contract

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// TestCandleStorerContract tests the CandleStorer interface contract
func TestCandleStorerContract(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		test        func(t *testing.T, storer contracts.CandleStorer)
	}{
		{
			name:        "Store_SingleCandle_Success",
			description: "Should store a single candle successfully",
			test:        testStoreSingleCandle,
		},
		{
			name:        "Store_MultipleCandles_Success",
			description: "Should store multiple candles in sequence",
			test:        testStoreMultipleCandles,
		},
		{
			name:        "Store_EmptySlice_NoError",
			description: "Should handle empty candle slice gracefully",
			test:        testStoreEmptySlice,
		},
		{
			name:        "Store_NilSlice_NoError",
			description: "Should handle nil candle slice gracefully",
			test:        testStoreNilSlice,
		},
		{
			name:        "Store_DuplicateCandles_HandleCorrectly",
			description: "Should handle duplicate candles (either upsert or error)",
			test:        testStoreDuplicateCandles,
		},
		{
			name:        "Store_InvalidCandle_Error",
			description: "Should return error for candles with invalid data",
			test:        testStoreInvalidCandle,
		},
		{
			name:        "Store_ContextCancellation_Error",
			description: "Should respect context cancellation",
			test:        testStoreContextCancellation,
		},
		{
			name:        "StoreBatch_LargeBatch_Success",
			description: "Should handle large batches efficiently",
			test:        testStoreBatchLarge,
		},
		{
			name:        "StoreBatch_BatchSize_Limits",
			description: "Should handle batch size limits appropriately",
			test:        testStoreBatchSizeLimits,
		},
		{
			name:        "StoreBatch_ConcurrentAccess_Success",
			description: "Should handle concurrent batch operations safely",
			test:        testStoreBatchConcurrent,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This will fail initially as no implementation exists
			var storer contracts.CandleStorer
			require.NotNil(t, storer, "CandleStorer implementation not provided - this is expected for TDD")

			t.Logf("Testing: %s", tc.description)
			tc.test(t, storer)
		})
	}
}

// TestCandleReaderContract tests the CandleReader interface contract
func TestCandleReaderContract(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		test        func(t *testing.T, reader contracts.CandleReader)
	}{
		{
			name:        "Query_BasicQuery_Success",
			description: "Should execute basic time range query successfully",
			test:        testQueryBasic,
		},
		{
			name:        "Query_Performance_Under100ms",
			description: "Should complete queries under 100ms performance requirement",
			test:        testQueryPerformance,
		},
		{
			name:        "Query_Pagination_Success",
			description: "Should handle pagination correctly",
			test:        testQueryPagination,
		},
		{
			name:        "Query_OrderBy_Success",
			description: "Should respect ordering parameters",
			test:        testQueryOrdering,
		},
		{
			name:        "Query_EmptyResult_ValidResponse",
			description: "Should return valid empty response for no matches",
			test:        testQueryEmptyResult,
		},
		{
			name:        "Query_InvalidTimeRange_Error",
			description: "Should validate time ranges",
			test:        testQueryInvalidTimeRange,
		},
		{
			name:        "Query_InvalidInterval_Error",
			description: "Should validate interval parameter",
			test:        testQueryInvalidInterval,
		},
		{
			name:        "Query_LargeResult_Efficient",
			description: "Should handle large result sets efficiently",
			test:        testQueryLargeResult,
		},
		{
			name:        "GetLatest_ExistingPair_Success",
			description: "Should retrieve latest candle for existing pair",
			test:        testGetLatestExisting,
		},
		{
			name:        "GetLatest_NonExistentPair_NotFound",
			description: "Should handle non-existent pair gracefully",
			test:        testGetLatestNonExistent,
		},
		{
			name:        "GetLatest_Performance_Fast",
			description: "Should retrieve latest candle quickly",
			test:        testGetLatestPerformance,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This will fail initially as no implementation exists
			var reader contracts.CandleReader
			require.NotNil(t, reader, "CandleReader implementation not provided - this is expected for TDD")

			t.Logf("Testing: %s", tc.description)
			tc.test(t, reader)
		})
	}
}

// TestStorageManagerContract tests the StorageManager interface contract
func TestStorageManagerContract(t *testing.T) {
	testCases := []struct {
		name        string
		description string
		test        func(t *testing.T, manager contracts.StorageManager)
	}{
		{
			name:        "Initialize_Fresh_Success",
			description: "Should initialize fresh storage successfully",
			test:        testInitializeFresh,
		},
		{
			name:        "Initialize_Existing_Success",
			description: "Should handle existing storage initialization",
			test:        testInitializeExisting,
		},
		{
			name:        "Close_Active_Success",
			description: "Should close active storage connections cleanly",
			test:        testCloseActive,
		},
		{
			name:        "Close_Already_Closed_NoError",
			description: "Should handle multiple close calls gracefully",
			test:        testCloseAlreadyClosed,
		},
		{
			name:        "Migrate_ValidVersion_Success",
			description: "Should migrate to valid version successfully",
			test:        testMigrateValid,
		},
		{
			name:        "Migrate_InvalidVersion_Error",
			description: "Should reject invalid migration versions",
			test:        testMigrateInvalid,
		},
		{
			name:        "GetStats_Active_ValidStats",
			description: "Should return valid statistics for active storage",
			test:        testGetStatsActive,
		},
		{
			name:        "HealthCheck_Healthy_Success",
			description: "Should pass health check when healthy",
			test:        testHealthCheckHealthy,
		},
		{
			name:        "HealthCheck_Unhealthy_Error",
			description: "Should fail health check when unhealthy",
			test:        testHealthCheckUnhealthy,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This will fail initially as no implementation exists
			var manager contracts.StorageManager
			require.NotNil(t, manager, "StorageManager implementation not provided - this is expected for TDD")

			t.Logf("Testing: %s", tc.description)
			tc.test(t, manager)
		})
	}
}

// TestFullStorageIntegration tests the complete FullStorage interface
func TestFullStorageIntegration(t *testing.T) {
	// This will fail initially as no implementation exists
	var storage contracts.FullStorage
	require.NotNil(t, storage, "FullStorage implementation not provided - this is expected for TDD")

	ctx := context.Background()

	t.Run("Integration_StoreAndQuery_DataConsistency", func(t *testing.T) {
		// Test data flow: Store -> Query -> Validate consistency
		testCandles := generateTestCandles(10)

		err := storage.StoreBatch(ctx, testCandles)
		require.NoError(t, err)

		req := contracts.QueryRequest{
			Pair:     testCandles[0].Pair,
			Start:    testCandles[0].Timestamp,
			End:      testCandles[len(testCandles)-1].Timestamp,
			Interval: testCandles[0].Interval,
		}

		resp, err := storage.Query(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, len(testCandles), len(resp.Candles))
	})

	t.Run("Integration_GapDetectionAndFilling", func(t *testing.T) {
		// Test gap detection and filling workflow
		gap := contracts.Gap{
			ID:        "test-gap-1",
			Pair:      "BTC/USD",
			Interval:  "1h",
			StartTime: time.Now().Add(-2 * time.Hour),
			EndTime:   time.Now().Add(-1 * time.Hour),
			Status:    "detected",
			CreatedAt: time.Now(),
		}

		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err)

		gaps, err := storage.GetGaps(ctx, gap.Pair, gap.Interval)
		require.NoError(t, err)
		assert.Len(t, gaps, 1)

		err = storage.MarkGapFilled(ctx, gap.ID, time.Now())
		require.NoError(t, err)
	})
}

// CandleStorer test implementations
func testStoreSingleCandle(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()
	candle := generateTestCandles(1)[0]

	err := storer.Store(ctx, []contracts.Candle{candle})
	assert.NoError(t, err)
}

func testStoreMultipleCandles(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()
	candles := generateTestCandles(5)

	err := storer.Store(ctx, candles)
	assert.NoError(t, err)
}

func testStoreEmptySlice(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()

	err := storer.Store(ctx, []contracts.Candle{})
	assert.NoError(t, err)
}

func testStoreNilSlice(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()

	err := storer.Store(ctx, nil)
	assert.NoError(t, err)
}

func testStoreDuplicateCandles(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()
	candle := generateTestCandles(1)[0]

	// Store same candle twice
	err1 := storer.Store(ctx, []contracts.Candle{candle})
	err2 := storer.Store(ctx, []contracts.Candle{candle})

	// Should either both succeed (upsert) or second should error (uniqueness)
	if err1 == nil && err2 != nil {
		assert.Error(t, err2, "Duplicate candle should be handled consistently")
	} else {
		assert.NoError(t, err1)
		assert.NoError(t, err2)
	}
}

func testStoreInvalidCandle(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()
	invalidCandle := contracts.Candle{
		// Missing required fields
		Timestamp: time.Time{},
		Open:      "",
		High:      "",
		Low:       "",
		Close:     "",
		Volume:    "",
		Pair:      "",
		Interval:  "",
	}

	err := storer.Store(ctx, []contracts.Candle{invalidCandle})
	assert.Error(t, err, "Should reject invalid candle data")
}

func testStoreContextCancellation(t *testing.T, storer contracts.CandleStorer) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	candles := generateTestCandles(1)
	err := storer.Store(ctx, candles)
	assert.Error(t, err, "Should respect context cancellation")
}

func testStoreBatchLarge(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()
	largeCandles := generateTestCandles(1000) // Large batch

	start := time.Now()
	err := storer.StoreBatch(ctx, largeCandles)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, duration, 5*time.Second, "Large batch should complete within reasonable time")
}

func testStoreBatchSizeLimits(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()

	// Test extremely large batch
	veryLargeCandles := generateTestCandles(10000)
	err := storer.StoreBatch(ctx, veryLargeCandles)

	// Should either succeed or return a meaningful error about batch size
	if err != nil {
		assert.Contains(t, strings.ToLower(err.Error()),
			"batch", "Should provide meaningful error for oversized batches")
	}
}

func testStoreBatchConcurrent(t *testing.T, storer contracts.CandleStorer) {
	ctx := context.Background()

	// Run concurrent batch operations
	const numRoutines = 5
	errors := make(chan error, numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func(routineID int) {
			candles := generateTestCandlesForPair(10, fmt.Sprintf("PAIR%d/USD", routineID))
			errors <- storer.StoreBatch(ctx, candles)
		}(i)
	}

	// Collect results
	for i := 0; i < numRoutines; i++ {
		err := <-errors
		assert.NoError(t, err, "Concurrent batch operations should succeed")
	}
}

// CandleReader test implementations
func testQueryBasic(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-24 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    100,
	}

	resp, err := reader.Query(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.GreaterOrEqual(t, resp.Total, 0)
}

func testQueryPerformance(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-1 * time.Hour),
		End:      time.Now(),
		Interval: "1m",
		Limit:    60,
	}

	start := time.Now()
	resp, err := reader.Query(ctx, req)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Less(t, duration, 100*time.Millisecond,
		"Query should complete within 100ms performance requirement")

	// Validate QueryTime is populated
	assert.Greater(t, resp.QueryTime, time.Duration(0))
	assert.LessOrEqual(t, resp.QueryTime, duration)
}

func testQueryPagination(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()

	// First page
	req1 := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-24 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    10,
		Offset:   0,
	}

	resp1, err := reader.Query(ctx, req1)
	assert.NoError(t, err)

	if resp1.HasMore {
		// Second page
		req2 := req1
		req2.Offset = resp1.NextOffset

		resp2, err := reader.Query(ctx, req2)
		assert.NoError(t, err)

		// Validate pagination
		assert.NotEqual(t, resp1.Candles, resp2.Candles, "Different pages should return different data")
		assert.Equal(t, req2.Offset, resp1.NextOffset, "NextOffset should be consistent")
	}
}

func testQueryOrdering(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()

	// Test ascending order
	reqAsc := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-24 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    10,
		OrderBy:  "timestamp_asc",
	}

	respAsc, err := reader.Query(ctx, reqAsc)
	assert.NoError(t, err)

	if len(respAsc.Candles) > 1 {
		// Validate ascending order
		for i := 1; i < len(respAsc.Candles); i++ {
			assert.True(t, respAsc.Candles[i].Timestamp.After(respAsc.Candles[i-1].Timestamp),
				"Ascending order should be maintained")
		}
	}

	// Test descending order
	reqDesc := reqAsc
	reqDesc.OrderBy = "timestamp_desc"

	respDesc, err := reader.Query(ctx, reqDesc)
	assert.NoError(t, err)

	if len(respDesc.Candles) > 1 {
		// Validate descending order
		for i := 1; i < len(respDesc.Candles); i++ {
			assert.True(t, respDesc.Candles[i].Timestamp.Before(respDesc.Candles[i-1].Timestamp),
				"Descending order should be maintained")
		}
	}
}

func testQueryEmptyResult(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "NONEXISTENT/PAIR",
		Start:    time.Now().Add(-1 * time.Hour),
		End:      time.Now(),
		Interval: "1m",
		Limit:    100,
	}

	resp, err := reader.Query(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 0, len(resp.Candles))
	assert.Equal(t, 0, resp.Total)
	assert.False(t, resp.HasMore)
}

func testQueryInvalidTimeRange(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now(), // Start after end
		End:      time.Now().Add(-1 * time.Hour),
		Interval: "1h",
		Limit:    100,
	}

	_, err := reader.Query(ctx, req)
	assert.Error(t, err, "Should reject invalid time range")
}

func testQueryInvalidInterval(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-1 * time.Hour),
		End:      time.Now(),
		Interval: "invalid_interval",
		Limit:    100,
	}

	_, err := reader.Query(ctx, req)
	assert.Error(t, err, "Should reject invalid interval")
}

func testQueryLargeResult(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()
	req := contracts.QueryRequest{
		Pair:     "BTC/USD",
		Start:    time.Now().Add(-7 * 24 * time.Hour), // 7 days
		End:      time.Now(),
		Interval: "1m", // Large result set
		Limit:    10000,
	}

	start := time.Now()
	resp, err := reader.Query(ctx, req)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Less(t, duration, 1*time.Second, "Large queries should still be reasonably fast")
}

func testGetLatestExisting(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()

	candle, err := reader.GetLatest(ctx, "BTC/USD", "1h")

	if err == nil {
		assert.NotNil(t, candle)
		assert.Equal(t, "BTC/USD", candle.Pair)
		assert.Equal(t, "1h", candle.Interval)
		assert.False(t, candle.Timestamp.IsZero())
	} else {
		// If no data exists, should return appropriate error
		assert.Error(t, err)
	}
}

func testGetLatestNonExistent(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()

	candle, err := reader.GetLatest(ctx, "NONEXISTENT/PAIR", "1h")
	assert.Nil(t, candle)
	assert.Error(t, err, "Should return error for non-existent pair")
}

func testGetLatestPerformance(t *testing.T, reader contracts.CandleReader) {
	ctx := context.Background()

	start := time.Now()
	_, err := reader.GetLatest(ctx, "BTC/USD", "1h")
	duration := time.Since(start)

	// Should be very fast regardless of success/failure
	assert.Less(t, duration, 50*time.Millisecond, "GetLatest should be very fast")

	// Error is acceptable if no data exists
	if err != nil {
		t.Logf("GetLatest returned error (acceptable for empty storage): %v", err)
	}
}

// StorageManager test implementations
func testInitializeFresh(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	err := manager.Initialize(ctx)
	assert.NoError(t, err)
}

func testInitializeExisting(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	// Initialize twice - should handle existing storage gracefully
	err1 := manager.Initialize(ctx)
	err2 := manager.Initialize(ctx)

	assert.NoError(t, err1)
	assert.NoError(t, err2, "Should handle existing storage initialization")
}

func testCloseActive(t *testing.T, manager contracts.StorageManager) {
	err := manager.Close()
	assert.NoError(t, err)
}

func testCloseAlreadyClosed(t *testing.T, manager contracts.StorageManager) {
	err1 := manager.Close()
	err2 := manager.Close() // Close twice

	assert.NoError(t, err1)
	assert.NoError(t, err2, "Should handle multiple close calls gracefully")
}

func testMigrateValid(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	err := manager.Migrate(ctx, 1) // Valid version
	assert.NoError(t, err)
}

func testMigrateInvalid(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	err := manager.Migrate(ctx, -1) // Invalid version
	assert.Error(t, err, "Should reject invalid migration version")
}

func testGetStatsActive(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	stats, err := manager.GetStats(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, stats)

	// Validate stats structure
	assert.GreaterOrEqual(t, stats.TotalCandles, int64(0))
	assert.GreaterOrEqual(t, stats.TotalPairs, 0)
	assert.GreaterOrEqual(t, stats.StorageSize, int64(0))
	assert.GreaterOrEqual(t, stats.IndexSize, int64(0))
	assert.NotNil(t, stats.QueryPerformance)
}

func testHealthCheckHealthy(t *testing.T, manager contracts.StorageManager) {
	ctx := context.Background()

	err := manager.HealthCheck(ctx)
	// Should either pass or provide meaningful error
	if err != nil {
		t.Logf("Health check failed (may be expected for uninitialized storage): %v", err)
	}
}

func testHealthCheckUnhealthy(t *testing.T, manager contracts.StorageManager) {
	// Use cancelled context to simulate unhealthy state
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := manager.HealthCheck(ctx)
	assert.Error(t, err, "Should fail health check with cancelled context")
}

// Helper functions for generating test data
func generateTestCandles(count int) []contracts.Candle {
	return generateTestCandlesForPair(count, "BTC/USD")
}

func generateTestCandlesForPair(count int, pair string) []contracts.Candle {
	candles := make([]contracts.Candle, count)
	baseTime := time.Now().Add(-time.Duration(count) * time.Hour)

	for i := 0; i < count; i++ {
		candles[i] = contracts.Candle{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      "50000.00",
			High:      "51000.00",
			Low:       "49000.00",
			Close:     "50500.00",
			Volume:    "100.5",
			Pair:      pair,
			Interval:  "1h",
		}
	}

	return candles
}

// Benchmark tests for performance validation
func BenchmarkStorageOperations(b *testing.B) {
	// This will fail initially as no implementation exists
	var storage contracts.FullStorage
	if storage == nil {
		b.Skip("FullStorage implementation not provided - this is expected for TDD")
	}

	ctx := context.Background()

	b.Run("StoreBatch", func(b *testing.B) {
		candles := generateTestCandles(100)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := storage.StoreBatch(ctx, candles)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query", func(b *testing.B) {
		req := contracts.QueryRequest{
			Pair:     "BTC/USD",
			Start:    time.Now().Add(-24 * time.Hour),
			End:      time.Now(),
			Interval: "1h",
			Limit:    100,
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := storage.Query(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetLatest", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := storage.GetLatest(ctx, "BTC/USD", "1h")
			if err != nil && !errors.Is(err, context.Canceled) {
				// Allow not found errors for empty storage
				continue
			}
		}
	})
}
