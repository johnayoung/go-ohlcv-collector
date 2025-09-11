// Package benchmark provides performance benchmarks for data collection throughput
// and validates the performance requirements specified in the OHLCV collector design.
// Benchmarks test collection rates, storage operations, and memory efficiency.
package benchmark

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/collector"
	"github.com/johnayoung/go-ohlcv-collector/internal/config"
	"github.com/johnayoung/go-ohlcv-collector/internal/logger"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
	"github.com/shopspring/decimal"
)

// BenchmarkCollectorThroughput benchmarks data collection throughput
// Target: >1000 candles/second as specified in performance requirements
func BenchmarkCollectorThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	// Create test configuration
	cfg := &collector.Config{
		BatchSize:     1000,
		WorkerCount:   4,
		RateLimit:     60,
		RetryAttempts: 3,
	}

	// Setup in-memory storage for testing
	memStorage := storage.NewMemoryStorage(slog.Default())
	
	// Create mock exchange adapter
	exchange := &MockExchangeAdapter{
		candleData: generateTestCandles(10000), // Pre-generate test data
	}

	// Create collector with test configuration
	c := collector.NewWithConfig(exchange, memStorage, memStorage, cfg, slog.Default())

	// Benchmark parameters
	pair := "BTC-USD"
	interval := "1h"
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		_, err := c.CollectHistoricalData(ctx, pair, interval, startTime, endTime)
		if err != nil {
			b.Fatalf("Collection failed: %v", err)
		}
	}

	// Calculate and report throughput
	totalCandles := int64(b.N) * 24 // 24 hours of hourly data
	duration := time.Since(time.Now().Add(-time.Duration(b.Elapsed())))
	throughput := float64(totalCandles) / b.Elapsed().Seconds()
	
	b.ReportMetric(throughput, "candles/sec")
	
	// Verify we meet the performance target
	if throughput < 1000 {
		b.Errorf("Throughput %0.2f candles/sec below target of 1000 candles/sec", throughput)
	}
}

// BenchmarkStorageOperations benchmarks storage write and read performance
// Target: <100ms query response time
func BenchmarkStorageOperations(b *testing.B) {
	tests := []struct {
		name    string
		storage func() interface{}
	}{
		{
			name: "MemoryStorage",
			storage: func() interface{} {
				return storage.NewMemoryStorage(slog.Default())
			},
		},
		{
			name: "DuckDBStorage",
			storage: func() interface{} {
				tmpFile := fmt.Sprintf("/tmp/benchmark_%d.db", time.Now().UnixNano())
				db, err := storage.NewDuckDBStorage(tmpFile, slog.Default())
				if err != nil {
					b.Fatalf("Failed to create DuckDB storage: %v", err)
				}
				ctx := context.Background()
				if err := db.Initialize(ctx); err != nil {
					b.Fatalf("Failed to initialize DuckDB storage: %v", err)
				}
				// Clean up after test
				b.Cleanup(func() {
					db.Close()
					os.Remove(tmpFile)
				})
				return db
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkStorageWrites(b, tt.storage())
			benchmarkStorageReads(b, tt.storage())
		})
	}
}

// benchmarkStorageWrites benchmarks bulk storage operations
func benchmarkStorageWrites(b *testing.B, storage interface{}) {
	storer, ok := storage.(interface {
		StoreBatch(ctx context.Context, candles []models.Candle) error
	})
	if !ok {
		b.Skip("Storage doesn't implement batch operations")
	}

	// Generate test data
	testCandles := generateTestCandles(1000)
	
	b.Run("BulkWrites", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			ctx := context.Background()
			err := storer.StoreBatch(ctx, testCandles)
			if err != nil {
				b.Fatalf("Bulk write failed: %v", err)
			}
		}
		
		// Calculate write throughput
		totalCandles := int64(b.N) * int64(len(testCandles))
		throughput := float64(totalCandles) / b.Elapsed().Seconds()
		b.ReportMetric(throughput, "writes/sec")
	})
}

// benchmarkStorageReads benchmarks query performance
func benchmarkStorageReads(b *testing.B, storage interface{}) {
	reader, ok := storage.(interface {
		Query(ctx context.Context, req storage.QueryRequest) (*storage.QueryResponse, error)
		StoreBatch(ctx context.Context, candles []models.Candle) error
	})
	if !ok {
		b.Skip("Storage doesn't implement query operations")
	}

	// Setup test data
	testCandles := generateTestCandles(10000)
	ctx := context.Background()
	err := reader.StoreBatch(ctx, testCandles)
	if err != nil {
		b.Fatalf("Failed to setup test data: %v", err)
	}

	queryTests := []struct {
		name string
		req  storage.QueryRequest
	}{
		{
			name: "TimeRangeQuery",
			req: storage.QueryRequest{
				Pair:     "BTC-USD",
				Interval: "1h",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Limit:    1000,
			},
		},
		{
			name: "PaginatedQuery",
			req: storage.QueryRequest{
				Pair:     "BTC-USD",
				Interval: "1h",
				Limit:    100,
				Offset:   0,
			},
		},
		{
			name: "LargeResultQuery",
			req: storage.QueryRequest{
				Pair:     "BTC-USD",
				Interval: "1h",
				Limit:    5000,
			},
		},
	}

	for _, qt := range queryTests {
		b.Run(qt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				resp, err := reader.Query(ctx, qt.req)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
				if resp == nil {
					b.Fatalf("Nil response")
				}
			}
			
			// Verify response time is under target
			avgResponseTime := b.Elapsed() / time.Duration(b.N)
			if avgResponseTime > 100*time.Millisecond {
				b.Errorf("Average response time %v exceeds 100ms target", avgResponseTime)
			}
			
			b.ReportMetric(float64(avgResponseTime.Nanoseconds())/1e6, "ms/query")
		})
	}
}

// BenchmarkConcurrentCollection benchmarks concurrent data collection
// Tests worker pool efficiency and resource contention
func BenchmarkConcurrentCollection(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	workerCounts := []int{1, 2, 4, 8, 16}
	
	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("Workers_%d", workers), func(b *testing.B) {
			benchmarkConcurrentCollectionWithWorkers(b, workers)
		})
	}
}

// benchmarkConcurrentCollectionWithWorkers benchmarks collection with specific worker count
func benchmarkConcurrentCollectionWithWorkers(b *testing.B, workerCount int) {
	cfg := &collector.Config{
		BatchSize:     500,
		WorkerCount:   workerCount,
		RateLimit:     60,
		RetryAttempts: 2,
	}

	memStorage := storage.NewMemoryStorage(slog.Default())
	exchange := &MockExchangeAdapter{
		candleData: generateTestCandles(1000),
		delay:      1 * time.Millisecond, // Simulate network delay
	}

	c := collector.NewWithConfig(exchange, memStorage, memStorage, cfg, slog.Default())

	pairs := []string{"BTC-USD", "ETH-USD", "LTC-USD", "ADA-USD"}
	interval := "1h"
	startTime := time.Now().Add(-12 * time.Hour)
	endTime := time.Now()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		ctx := context.Background()
		
		// Collect data for multiple pairs concurrently
		for _, pair := range pairs {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				_, err := c.CollectHistoricalData(ctx, p, interval, startTime, endTime)
				if err != nil {
					b.Errorf("Collection failed for %s: %v", p, err)
				}
			}(pair)
		}
		
		wg.Wait()
	}

	// Report efficiency metrics
	totalOperations := int64(b.N) * int64(len(pairs))
	efficiency := float64(totalOperations) / b.Elapsed().Seconds()
	b.ReportMetric(efficiency, "concurrent_ops/sec")
}

// BenchmarkMemoryUsage benchmarks memory efficiency during collection
// Target: <50MB memory usage as specified in performance requirements
func BenchmarkMemoryUsage(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	cfg := &collector.Config{
		BatchSize:     1000,
		WorkerCount:   4,
		RateLimit:     60,
		RetryAttempts: 3,
	}

	// Use in-memory storage to test collector memory usage
	memStorage := storage.NewMemoryStorage(slog.Default())
	exchange := &MockExchangeAdapter{
		candleData: generateTestCandles(50000), // Large dataset
	}

	c := collector.NewWithConfig(exchange, memStorage, memStorage, cfg, slog.Default())

	// Force GC before measurement
	runtime.GC()
	runtime.GC() // Call twice for more accurate measurement

	var startMemStats, endMemStats runtime.MemStats
	runtime.ReadMemStats(&startMemStats)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		pairs := []string{"BTC-USD", "ETH-USD", "LTC-USD"}
		
		for _, pair := range pairs {
			_, err := c.CollectHistoricalData(ctx, pair, "1h", 
				time.Now().Add(-168*time.Hour), // 1 week
				time.Now())
			if err != nil {
				b.Fatalf("Collection failed: %v", err)
			}
		}
		
		// Force GC to get accurate memory measurement
		if i%10 == 0 {
			runtime.GC()
		}
	}

	b.StopTimer()
	
	// Measure final memory usage
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&endMemStats)

	// Calculate memory usage
	allocatedMB := float64(endMemStats.HeapAlloc-startMemStats.HeapAlloc) / (1024 * 1024)
	systemMB := float64(endMemStats.HeapSys-startMemStats.HeapSys) / (1024 * 1024)
	
	b.ReportMetric(allocatedMB, "MB_allocated")
	b.ReportMetric(systemMB, "MB_system")

	// Verify memory usage is within target
	if allocatedMB > 50 {
		b.Errorf("Memory usage %.2f MB exceeds 50MB target", allocatedMB)
	}
}

// BenchmarkValidationPerformance benchmarks data validation throughput
func BenchmarkValidationPerformance(b *testing.B) {
	// Create validator configuration
	loggerMgr, _ := logger.NewLoggerManager(config.LoggingConfig{
		Level:  "error", // Reduce logging for benchmarks
		Format: "text",
		Output: "stderr",
	})

	// Generate test candles with various validation scenarios
	validCandles := generateTestCandles(1000)
	invalidCandles := generateInvalidCandles(100)
	mixedCandles := append(validCandles[:800], invalidCandles...)

	benchmarks := []struct {
		name    string
		candles []models.Candle
	}{
		{"ValidCandles", validCandles},
		{"InvalidCandles", invalidCandles},
		{"MixedCandles", mixedCandles},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				validCount := 0
				for _, candle := range bm.candles {
					if err := candle.Validate(); err == nil {
						validCount++
					}
				}
				// Prevent compiler optimization
				_ = validCount
			}
			
			throughput := float64(len(bm.candles)*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(throughput, "validations/sec")
		})
	}
}

// BenchmarkSchedulerPerformance benchmarks the scheduler's job management
func BenchmarkSchedulerPerformance(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	cfg := collector.DefaultSchedulerConfig()
	cfg.MaxConcurrentJobs = 10
	cfg.JobTimeout = 30 * time.Second

	memStorage := storage.NewMemoryStorage(slog.Default())
	exchange := &MockExchangeAdapter{
		candleData: generateTestCandles(100),
		delay:      1 * time.Millisecond,
	}

	collectorCfg := &collector.Config{
		BatchSize:     100,
		WorkerCount:   4,
		RateLimit:     60,
		RetryAttempts: 2,
	}

	c := collector.NewWithConfig(exchange, memStorage, memStorage, collectorCfg, slog.Default())
	scheduler := collector.NewScheduler(c, cfg)

	ctx := context.Background()
	
	// Start scheduler
	if err := scheduler.Start(ctx); err != nil {
		b.Fatalf("Failed to start scheduler: %v", err)
	}
	defer scheduler.Stop(ctx)

	pairs := []string{"BTC-USD", "ETH-USD", "LTC-USD", "ADA-USD", "DOT-USD"}
	intervals := []string{"1m", "5m", "15m", "1h"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		
		// Schedule multiple jobs concurrently
		for _, pair := range pairs {
			for _, interval := range intervals {
				wg.Add(1)
				go func(p, intv string) {
					defer wg.Done()
					
					job := &collector.CollectionJob{
						Pair:      p,
						Interval:  intv,
						StartTime: time.Now().Add(-1 * time.Hour),
						EndTime:   time.Now(),
						Priority:  1,
					}
					
					if err := scheduler.ScheduleJob(ctx, job); err != nil {
						b.Errorf("Failed to schedule job: %v", err)
					}
				}(pair, interval)
			}
		}
		
		wg.Wait()
		
		// Wait for jobs to complete
		time.Sleep(100 * time.Millisecond)
	}

	// Report scheduling throughput
	totalJobs := int64(b.N) * int64(len(pairs)) * int64(len(intervals))
	throughput := float64(totalJobs) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "jobs_scheduled/sec")
}

// Helper functions for benchmark setup

// generateTestCandles creates realistic test candles for benchmarking
func generateTestCandles(count int) []models.Candle {
	candles := make([]models.Candle, count)
	basePrice := decimal.NewFromFloat(50000.0) // Starting at $50,000
	baseTime := time.Now().Add(-time.Duration(count) * time.Hour)
	
	for i := 0; i < count; i++ {
		// Generate realistic price movement
		change := decimal.NewFromFloat((float64(i%100) - 50) * 0.01) // ±0.5% change
		open := basePrice.Add(change)
		
		// Generate OHLC with realistic relationships
		highChange := decimal.NewFromFloat(float64(i%20) * 0.001)  // 0-2% higher
		lowChange := decimal.NewFromFloat(float64(i%15) * -0.001)   // 0-1.5% lower
		closeChange := decimal.NewFromFloat((float64(i%30) - 15) * 0.002) // ±3% from open
		
		high := open.Add(highChange)
		low := open.Add(lowChange)
		close := open.Add(closeChange)
		
		// Ensure OHLC relationships
		if high.LessThan(open) || high.LessThan(close) {
			if open.GreaterThan(close) {
				high = open
			} else {
				high = close
			}
		}
		
		if low.GreaterThan(open) || low.GreaterThan(close) {
			if open.LessThan(close) {
				low = open
			} else {
				low = close
			}
		}
		
		volume := decimal.NewFromFloat(float64(1000 + i%5000)) // Random volume
		
		candles[i] = models.Candle{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      open.String(),
			High:      high.String(),
			Low:       low.String(),
			Close:     close.String(),
			Volume:    volume.String(),
			Pair:      "BTC-USD",
			Interval:  "1h",
		}
		
		basePrice = close // Next candle starts where this one ended
	}
	
	return candles
}

// generateInvalidCandles creates candles with validation errors for testing
func generateInvalidCandles(count int) []models.Candle {
	candles := make([]models.Candle, count)
	baseTime := time.Now().Add(-time.Duration(count) * time.Hour)
	
	for i := 0; i < count; i++ {
		// Create various types of invalid candles
		switch i % 5 {
		case 0: // High < Open
			candles[i] = models.Candle{
				Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
				Open:      "100.0",
				High:      "95.0", // Invalid: high < open
				Low:       "90.0",
				Close:     "98.0",
				Volume:    "1000",
				Pair:      "BTC-USD",
				Interval:  "1h",
			}
		case 1: // Negative price
			candles[i] = models.Candle{
				Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
				Open:      "-100.0", // Invalid: negative price
				High:      "105.0",
				Low:       "95.0",
				Close:     "102.0",
				Volume:    "1000",
				Pair:      "BTC-USD",
				Interval:  "1h",
			}
		case 2: // Negative volume
			candles[i] = models.Candle{
				Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
				Open:      "100.0",
				High:      "105.0",
				Low:       "95.0",
				Close:     "102.0",
				Volume:    "-1000", // Invalid: negative volume
				Pair:      "BTC-USD",
				Interval:  "1h",
			}
		case 3: // Invalid format
			candles[i] = models.Candle{
				Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
				Open:      "not-a-number", // Invalid: not a number
				High:      "105.0",
				Low:       "95.0",
				Close:     "102.0",
				Volume:    "1000",
				Pair:      "BTC-USD",
				Interval:  "1h",
			}
		case 4: // Zero timestamp
			candles[i] = models.Candle{
				Timestamp: time.Time{}, // Invalid: zero timestamp
				Open:      "100.0",
				High:      "105.0",
				Low:       "95.0",
				Close:     "102.0",
				Volume:    "1000",
				Pair:      "BTC-USD",
				Interval:  "1h",
			}
		}
	}
	
	return candles
}

// Import runtime for memory benchmarks
import "runtime"

// MockExchangeAdapter for benchmark testing
type MockExchangeAdapter struct {
	candleData []models.Candle
	delay      time.Duration
	mu         sync.RWMutex
}

func (m *MockExchangeAdapter) FetchHistoricalData(ctx context.Context, pair, interval string, start, end time.Time) ([]models.Candle, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return subset of test data
	result := make([]models.Candle, 0, len(m.candleData))
	for _, candle := range m.candleData {
		if candle.Timestamp.After(start) && candle.Timestamp.Before(end) {
			candle.Pair = pair
			candle.Interval = interval
			result = append(result, candle)
		}
	}
	
	return result, nil
}

func (m *MockExchangeAdapter) GetTradingPairs(ctx context.Context) ([]models.TradingPair, error) {
	return []models.TradingPair{
		{Symbol: "BTC-USD", BaseAsset: "BTC", QuoteAsset: "USD", Active: true},
		{Symbol: "ETH-USD", BaseAsset: "ETH", QuoteAsset: "USD", Active: true},
		{Symbol: "LTC-USD", BaseAsset: "LTC", QuoteAsset: "USD", Active: true},
	}, nil
}

func (m *MockExchangeAdapter) GetRateLimit() int {
	return 60
}

func (m *MockExchangeAdapter) HealthCheck(ctx context.Context) error {
	return nil
}