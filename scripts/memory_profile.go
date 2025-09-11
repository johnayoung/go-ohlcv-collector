// Memory profiling script for OHLCV collector optimization validation
// This script validates memory usage patterns and optimization effectiveness
package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

func main() {
	fmt.Println("OHLCV Collector Memory Profiling")
	fmt.Println("=================================")

	// Force garbage collection for clean baseline
	runtime.GC()
	runtime.GC()

	var baselineMemStats runtime.MemStats
	runtime.ReadMemStats(&baselineMemStats)

	fmt.Printf("Baseline Memory Usage:\n")
	fmt.Printf("  Heap Alloc: %d bytes (%.2f MB)\n", baselineMemStats.HeapAlloc, float64(baselineMemStats.HeapAlloc)/(1024*1024))
	fmt.Printf("  Heap Sys: %d bytes (%.2f MB)\n", baselineMemStats.HeapSys, float64(baselineMemStats.HeapSys)/(1024*1024))
	fmt.Printf("  Goroutines: %d\n", runtime.NumGoroutine())

	// Test memory usage with large dataset
	fmt.Printf("\nTesting with 50,000 candles...\n")

	// Create storage
	memStorage := storage.NewMemoryStorage()

	// Generate test candles
	candleCount := 50000
	testCandles := generateTestCandles(candleCount)
	candleBaseTime := time.Now().Add(-time.Duration(candleCount) * time.Hour)

	// Store candles and measure memory
	ctx := context.Background()
	start := time.Now()

	err := memStorage.StoreBatch(ctx, testCandles)
	if err != nil {
		log.Fatal(err)
	}

	duration := time.Since(start)

	// Force GC and measure
	runtime.GC()
	runtime.GC()

	var afterMemStats runtime.MemStats
	runtime.ReadMemStats(&afterMemStats)

	fmt.Printf("\nAfter storing 50,000 candles:\n")
	fmt.Printf("  Storage Duration: %v\n", duration)
	fmt.Printf("  Heap Alloc: %d bytes (%.2f MB)\n", afterMemStats.HeapAlloc, float64(afterMemStats.HeapAlloc)/(1024*1024))
	fmt.Printf("  Heap Sys: %d bytes (%.2f MB)\n", afterMemStats.HeapSys, float64(afterMemStats.HeapSys)/(1024*1024))
	fmt.Printf("  Goroutines: %d\n", runtime.NumGoroutine())

	// Calculate memory increase
	allocIncrease := float64(afterMemStats.HeapAlloc-baselineMemStats.HeapAlloc) / (1024 * 1024)
	sysIncrease := float64(afterMemStats.HeapSys-baselineMemStats.HeapSys) / (1024 * 1024)

	fmt.Printf("\nMemory Usage Analysis:\n")
	fmt.Printf("  Heap Allocation Increase: %.2f MB\n", allocIncrease)
	fmt.Printf("  System Memory Increase: %.2f MB\n", sysIncrease)
	fmt.Printf("  Memory per Candle: %.2f bytes\n", float64(afterMemStats.HeapAlloc-baselineMemStats.HeapAlloc)/50000)
	fmt.Printf("  Storage Rate: %.2f candles/sec\n", float64(50000)/duration.Seconds())

	// Performance validation
	fmt.Printf("\nPerformance Validation:\n")

	// Target: <50MB memory usage
	if allocIncrease > 50 {
		fmt.Printf("  ❌ FAIL: Memory usage %.2f MB exceeds 50MB target\n", allocIncrease)
	} else {
		fmt.Printf("  ✅ PASS: Memory usage %.2f MB within 50MB target\n", allocIncrease)
	}

	// Target: >1000 candles/sec throughput
	throughput := float64(50000) / duration.Seconds()
	if throughput < 1000 {
		fmt.Printf("  ❌ FAIL: Throughput %.2f candles/sec below 1000 target\n", throughput)
	} else {
		fmt.Printf("  ✅ PASS: Throughput %.2f candles/sec exceeds 1000 target\n", throughput)
	}

	// Test query performance
	fmt.Printf("\nTesting query performance...\n")
	queryStart := time.Now()

	// Use realistic time range from stored candles
	req := storage.QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    candleBaseTime,
		End:      candleBaseTime.Add(time.Duration(candleCount) * time.Hour),
		Limit:    1000,
	}

	_, err = memStorage.Query(ctx, req)
	if err != nil {
		log.Fatal(err)
	}

	queryDuration := time.Since(queryStart)
	fmt.Printf("  Query Duration: %v\n", queryDuration)

	// Target: <100ms query response
	if queryDuration > 100*time.Millisecond {
		fmt.Printf("  ❌ FAIL: Query time %v exceeds 100ms target\n", queryDuration)
	} else {
		fmt.Printf("  ✅ PASS: Query time %v within 100ms target\n", queryDuration)
	}

	fmt.Printf("\nMemory profiling complete!\n")
}

// generateTestCandles creates realistic test candles for profiling
func generateTestCandles(count int) []models.Candle {
	candles := make([]models.Candle, count)
	baseTime := time.Now().Add(-time.Duration(count) * time.Hour)

	for i := 0; i < count; i++ {
		candles[i] = models.Candle{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      "50000.00",
			High:      "50100.00",
			Low:       "49900.00",
			Close:     "50050.00",
			Volume:    "1000.00",
			Pair:      "BTC-USD",
			Interval:  "1h",
		}
	}

	return candles
}
