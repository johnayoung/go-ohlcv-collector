// Quickstart scenario validation script
// This script validates core functionality components work correctly
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

func main() {
	fmt.Println("OHLCV Collector Quickstart Scenarios")
	fmt.Println("====================================")

	// Test Scenario 1: Basic Storage Operations
	fmt.Println("\nScenario 1: Basic Storage Operations")
	if err := testBasicStorage(); err != nil {
		log.Fatalf("Scenario 1 failed: %v", err)
	}
	fmt.Println("✅ PASS: Basic storage operations scenario")

	// Test Scenario 2: Model Validation
	fmt.Println("\nScenario 2: Model Validation")
	if err := testModelValidation(); err != nil {
		log.Fatalf("Scenario 2 failed: %v", err)
	}
	fmt.Println("✅ PASS: Model validation scenario")

	// Test Scenario 3: Query Operations
	fmt.Println("\nScenario 3: Query Operations")
	if err := testQueryOperations(); err != nil {
		log.Fatalf("Scenario 3 failed: %v", err)
	}
	fmt.Println("✅ PASS: Query operations scenario")

	// Test Scenario 4: Batch Operations
	fmt.Println("\nScenario 4: Batch Operations")
	if err := testBatchOperations(); err != nil {
		log.Fatalf("Scenario 4 failed: %v", err)
	}
	fmt.Println("✅ PASS: Batch operations scenario")

	fmt.Println("\nAll quickstart scenarios completed successfully! 🎉")
}

// testBasicStorage validates basic storage functionality
func testBasicStorage() error {
	ctx := context.Background()

	// Create memory storage for testing
	memStorage := storage.NewMemoryStorage()

	// Create test candle
	candle := models.Candle{
		Timestamp: time.Now().Truncate(time.Hour),
		Open:      "50000.00",
		High:      "50100.00",
		Low:       "49900.00",
		Close:     "50050.00",
		Volume:    "1000.00",
		Pair:      "BTC-USD",
		Interval:  "1h",
	}

	// Test single candle storage
	err := memStorage.Store(ctx, []models.Candle{candle})
	if err != nil {
		return fmt.Errorf("failed to store candle: %w", err)
	}

	fmt.Printf("  Stored single candle for %s\n", candle.Pair)
	return nil
}

// testModelValidation validates model functionality
func testModelValidation() error {
	// Test candle creation
	candle := models.Candle{
		Timestamp: time.Now(),
		Open:      "50000.00",
		High:      "51000.00",
		Low:       "49000.00",
		Close:     "50500.00",
		Volume:    "1500.50",
		Pair:      "ETH-USD",
		Interval:  "1d",
	}

	// Test validation
	if err := candle.Validate(); err != nil {
		return fmt.Errorf("candle validation failed: %w", err)
	}

	fmt.Printf("  Validated candle model for %s\n", candle.Pair)
	return nil
}

// testQueryOperations validates query functionality
func testQueryOperations() error {
	ctx := context.Background()
	memStorage := storage.NewMemoryStorage()

	// Generate and store test data
	testCandles := generateTestCandles(20)
	err := memStorage.StoreBatch(ctx, testCandles)
	if err != nil {
		return fmt.Errorf("failed to store test candles: %w", err)
	}

	// Test query
	queryReq := storage.QueryRequest{
		Pair:     "BTC-USD",
		Interval: "1h",
		Start:    time.Now().Add(-24 * time.Hour),
		End:      time.Now(),
		Limit:    10,
	}

	result, err := memStorage.Query(ctx, queryReq)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	fmt.Printf("  Retrieved %d candles from query\n", len(result.Candles))
	return nil
}

// testBatchOperations validates batch storage functionality
func testBatchOperations() error {
	ctx := context.Background()
	memStorage := storage.NewMemoryStorage()

	// Generate test data
	testCandles := generateTestCandles(100)

	// Test batch storage
	err := memStorage.StoreBatch(ctx, testCandles)
	if err != nil {
		return fmt.Errorf("batch storage failed: %w", err)
	}

	fmt.Printf("  Stored batch of %d candles\n", len(testCandles))
	return nil
}

// generateTestCandles creates realistic test candles
func generateTestCandles(count int) []models.Candle {
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
			Pair:      "BTC-USD",
			Interval:  "1h",
		}
	}

	return candles
}
