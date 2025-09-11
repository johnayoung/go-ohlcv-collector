# Quick Start Guide: OHLCV Data Collector

## Installation

```bash
go get github.com/johnayoung/go-ohlcv-collector
```

## Basic Usage

### 1. Create a Collector

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/johnayoung/go-ohlcv-collector/internal/collector"
    "github.com/johnayoung/go-ohlcv-collector/internal/exchange"
    "github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

func main() {
    // Create Coinbase exchange adapter
    coinbase := exchange.NewCoinbase(exchange.CoinbaseConfig{
        RateLimit: 10, // 10 requests per second
    })
    
    // Create DuckDB storage
    duckDB := storage.NewDuckDB(storage.DuckDBConfig{
        DatabasePath: "ohlcv_data.db",
    })
    defer duckDB.Close()
    
    // Initialize collector
    collector := collector.New(coinbase, duckDB)
    
    ctx := context.Background()
    
    // Collect BTC-USD daily data for last 30 days
    err := collector.CollectHistorical(ctx, collector.HistoricalRequest{
        Pair:     "BTC-USD",
        Interval: "1d",
        Start:    time.Now().AddDate(0, 0, -30),
        End:      time.Now(),
    })
    
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println("Data collection completed")
}
```

### 2. Query Collected Data

```go
// Query stored candles
candles, err := duckDB.Query(ctx, storage.QueryRequest{
    Pair:     "BTC-USD",
    Interval: "1d", 
    Start:    time.Now().AddDate(0, 0, -7),
    End:      time.Now(),
    OrderBy:  "timestamp_asc",
})

if err != nil {
    log.Fatal(err)
}

for _, candle := range candles.Candles {
    log.Printf("BTC-USD %s: O=%s H=%s L=%s C=%s V=%s", 
        candle.Timestamp.Format("2006-01-02"),
        candle.Open, candle.High, candle.Low, 
        candle.Close, candle.Volume)
}
```

### 3. Start Scheduled Collection

```go
// Start automatic hourly updates
scheduler := collector.NewScheduler(collector, collector.ScheduleConfig{
    Pairs:     []string{"BTC-USD", "ETH-USD"},
    Interval:  "1d",
    Frequency: time.Hour,
})

// Run scheduler (blocks until context cancelled)
err := scheduler.Start(ctx)
if err != nil {
    log.Fatal(err)
}
```

## CLI Usage

The library includes a CLI for quick operations:

```bash
# Install CLI
go install github.com/johnayoung/go-ohlcv-collector/cmd/ohlcv

# Collect historical data
ohlcv collect --pair BTC-USD --interval 1d --days 30 --storage duckdb://data.db

# Start scheduled collection
ohlcv schedule --pairs BTC-USD,ETH-USD --interval 1d --frequency 1h

# Check for data gaps
ohlcv gaps --pair BTC-USD --interval 1d

# Backfill missing data
ohlcv backfill --pair BTC-USD --interval 1d

# Query data
ohlcv query --pair BTC-USD --start 2024-01-01 --end 2024-01-31 --format json
```

## Configuration Options

### Exchange Configuration

```go
// Coinbase with custom settings
coinbase := exchange.NewCoinbase(exchange.CoinbaseConfig{
    BaseURL:     "https://api.coinbase.com",
    RateLimit:   10,   // requests per second
    Timeout:     30,   // seconds
    RetryCount:  3,
    UserAgent:   "MyApp/1.0",
})
```

### Storage Configuration  

```go
// DuckDB with performance tuning
duckDB := storage.NewDuckDB(storage.DuckDBConfig{
    DatabasePath:   "ohlcv_data.db",
    MemoryLimit:    "4GB",
    MaxConnections: 1,
    BatchSize:      10000,
    EnableWAL:      true,
})

// In-memory for testing
memory := storage.NewMemory()

// PostgreSQL for production
postgres := storage.NewPostgreSQL(storage.PostgreSQLConfig{
    ConnectionString: "postgres://user:pass@localhost/ohlcv",
    MaxConnections:   20,
    TablePrefix:      "ohlcv_",
})
```

### Validation Configuration

```go
collector := collector.New(coinbase, duckDB, collector.Config{
    Validation: collector.ValidationConfig{
        PriceSpikeThreshold:  500.0, // 500% price change alert
        VolumeSurgeMultiplier: 10.0,  // 10x volume alert
        EnableAnomalyDetection: true,
        LogValidationResults:   true,
    },
    
    GapDetection: collector.GapConfig{
        EnableAutoBackfill: true,
        MaxGapDuration:     24 * time.Hour,
        BackfillBatchSize:  1000,
    },
})
```

## Testing Your Integration

```go
func TestOHLCVCollection(t *testing.T) {
    // Use in-memory storage for testing
    storage := storage.NewMemory()
    
    // Create test collector with mock exchange
    mockExchange := &MockExchange{
        candles: generateTestCandles(10),
    }
    
    collector := collector.New(mockExchange, storage)
    
    ctx := context.Background()
    
    // Test collection
    err := collector.CollectHistorical(ctx, collector.HistoricalRequest{
        Pair:     "TEST-USD",
        Interval: "1d",
        Start:    time.Now().AddDate(0, 0, -10),
        End:      time.Now(),
    })
    
    assert.NoError(t, err)
    
    // Verify data was stored
    result, err := storage.Query(ctx, storage.QueryRequest{
        Pair:     "TEST-USD",
        Interval: "1d",
    })
    
    assert.NoError(t, err)
    assert.Equal(t, 10, len(result.Candles))
}
```

## Error Handling

The library provides structured error types:

```go
err := collector.CollectHistorical(ctx, req)
if err != nil {
    var exchangeErr *exchange.ExchangeError
    var storageErr *storage.StorageError
    
    switch {
    case errors.As(err, &exchangeErr):
        if exchangeErr.Retryable {
            log.Printf("Retryable exchange error: %v", exchangeErr)
            // Retry logic here
        } else {
            log.Printf("Permanent exchange error: %v", exchangeErr)
        }
        
    case errors.As(err, &storageErr):
        log.Printf("Storage error: %v", storageErr)
        
    default:
        log.Printf("Unknown error: %v", err)
    }
}
```

## Performance Tips

1. **Batch Size**: Use larger batch sizes (5000-10000) for better performance
2. **Concurrent Collection**: Collect multiple pairs concurrently but respect rate limits
3. **Storage Choice**: Use DuckDB for analytics, PostgreSQL for production scale
4. **Memory Management**: Enable memory pooling for high-frequency operations
5. **Indexing**: Ensure proper indexing on (pair, timestamp, interval) for queries

## Next Steps

- Add more trading pairs with `collector.AddPair()`
- Set up monitoring with `collector.GetMetrics()`
- Configure alerting for data quality issues
- Scale horizontally with multiple collector instances
- Integrate with your backtesting or analytics pipeline