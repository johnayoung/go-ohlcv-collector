// Package benchmark provides performance benchmarks for OHLCV data collection
// and validates performance requirements for storage operations and memory efficiency.
package benchmark

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

// BenchmarkStorageThroughput benchmarks storage throughput performance
// Target: >1000 candles/second as specified in performance requirements
func BenchmarkStorageThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	// Setup
	ctx := context.Background()
	memStorage := storage.NewMemoryStorage()

	// Pre-generate test data to avoid allocation during benchmark
	testCandles := generateBenchCandles(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := memStorage.StoreBatch(ctx, testCandles)
		if err != nil {
			b.Fatalf("StoreBatch failed: %v", err)
		}
	}

	// Calculate and report throughput
	duration := time.Duration(b.Elapsed().Nanoseconds())
	totalCandles := int64(b.N) * int64(len(testCandles))
	throughput := float64(totalCandles) / duration.Seconds()

	b.ReportMetric(throughput, "candles/sec")
}

// BenchmarkQueryPerformance benchmarks query response times
// Target: <100ms query response as specified in performance requirements
func BenchmarkQueryPerformance(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	// Setup with pre-loaded data
	ctx := context.Background()
	memStorage := storage.NewMemoryStorage()

	// Load test data
	testCandles := generateBenchCandles(10000)
	err := memStorage.StoreBatch(ctx, testCandles)
	if err != nil {
		b.Fatalf("Failed to setup test data: %v", err)
	}

	// Define query request
	baseTime := time.Now().Add(-time.Duration(10000) * time.Hour)
	queryReq := storage.QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    baseTime,
		End:      baseTime.Add(time.Duration(10000) * time.Hour),
		Limit:    1000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := memStorage.Query(ctx, queryReq)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// BenchmarkMemoryEfficiency benchmarks memory usage per candle
// Target: <50MB memory usage for reasonable dataset sizes
func BenchmarkMemoryEfficiency(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()

	b.Run("10k_candles", func(b *testing.B) {
		benchmarkMemoryUsage(b, ctx, 10000)
	})

	b.Run("50k_candles", func(b *testing.B) {
		benchmarkMemoryUsage(b, ctx, 50000)
	})

	b.Run("100k_candles", func(b *testing.B) {
		benchmarkMemoryUsage(b, ctx, 100000)
	})
}

// benchmarkMemoryUsage measures memory usage for a given number of candles
func benchmarkMemoryUsage(b *testing.B, ctx context.Context, candleCount int) {
	for i := 0; i < b.N; i++ {
		// Force GC for clean measurement
		runtime.GC()

		var beforeMem runtime.MemStats
		runtime.ReadMemStats(&beforeMem)

		// Create storage and load data
		memStorage := storage.NewMemoryStorage()
		testCandles := generateBenchCandles(candleCount)

		err := memStorage.StoreBatch(ctx, testCandles)
		if err != nil {
			b.Fatalf("StoreBatch failed: %v", err)
		}

		// Force GC and measure
		runtime.GC()

		var afterMem runtime.MemStats
		runtime.ReadMemStats(&afterMem)

		memUsed := afterMem.HeapAlloc - beforeMem.HeapAlloc
		memPerCandle := float64(memUsed) / float64(candleCount)

		b.ReportMetric(float64(memUsed)/(1024*1024), "MB_total")
		b.ReportMetric(memPerCandle, "bytes/candle")
	}
}

// BenchmarkConcurrentOperations benchmarks concurrent storage operations
func BenchmarkConcurrentOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()
	memStorage := storage.NewMemoryStorage()

	// Test with different concurrency levels
	concurrencyLevels := []int{1, 2, 4, 8}

	for _, workers := range concurrencyLevels {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			benchmarkConcurrentStorage(b, ctx, memStorage, workers)
		})
	}
}

// benchmarkConcurrentStorage measures concurrent storage performance
func benchmarkConcurrentStorage(b *testing.B, ctx context.Context, memStorage *storage.MemoryStorage, workers int) {
	candlesPerWorker := 1000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		start := time.Now()

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// Generate unique test data for each worker
				testCandles := generateBenchCandlesForPair(candlesPerWorker, fmt.Sprintf("PAIR%d-USD", workerID))

				err := memStorage.StoreBatch(ctx, testCandles)
				if err != nil {
					b.Errorf("Worker %d failed: %v", workerID, err)
				}
			}(w)
		}

		wg.Wait()
		duration := time.Since(start)

		totalCandles := workers * candlesPerWorker
		throughput := float64(totalCandles) / duration.Seconds()

		b.ReportMetric(throughput, "candles/sec")
	}
}

// generateBenchCandles creates test candles for benchmarking
func generateBenchCandles(count int) []models.Candle {
	return generateBenchCandlesForPair(count, "BTC-USD")
}

// generateBenchCandlesForPair creates test candles for a specific pair
func generateBenchCandlesForPair(count int, pair string) []models.Candle {
	candles := make([]models.Candle, count)
	baseTime := time.Now().Truncate(time.Hour).Add(-time.Duration(count) * time.Hour)

	for i := 0; i < count; i++ {
		candles[i] = models.Candle{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      "50000.00",
			High:      "50100.00",
			Low:       "49900.00",
			Close:     "50050.00",
			Volume:    "1000.00",
			Pair:      pair,
			Interval:  "1h",
		}
	}

	return candles
}
