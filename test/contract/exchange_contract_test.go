package contract

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
)

// TestExchangeAdapter_ContractCompliance tests that any implementation
// of ExchangeAdapter satisfies all interface contracts
func TestExchangeAdapter_ContractCompliance(t *testing.T) {
	// This test will fail initially as we don't have implementations yet
	// This is intentional for TDD approach

	// TODO: Replace nil with actual exchange adapter implementations
	var adapter contracts.ExchangeAdapter = nil

	if adapter == nil {
		t.Fatal("ExchangeAdapter implementation required")
	}

	// Test all embedded interfaces are satisfied
	if _, ok := adapter.(contracts.CandleFetcher); !ok {
		t.Error("ExchangeAdapter must implement CandleFetcher")
	}

	if _, ok := adapter.(contracts.PairProvider); !ok {
		t.Error("ExchangeAdapter must implement PairProvider")
	}

	if _, ok := adapter.(contracts.RateLimitInfo); !ok {
		t.Error("ExchangeAdapter must implement RateLimitInfo")
	}

	if _, ok := adapter.(contracts.HealthChecker); !ok {
		t.Error("ExchangeAdapter must implement HealthChecker")
	}
}

// TestCandleFetcher_FetchCandles tests the CandleFetcher interface contract
func TestCandleFetcher_FetchCandles(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var fetcher contracts.CandleFetcher = nil
	if fetcher == nil {
		t.Fatal("CandleFetcher implementation required")
	}

	tests := []struct {
		name        string
		req         contracts.FetchRequest
		setupCtx    func() context.Context
		expectError bool
		errorType   string
		validate    func(t *testing.T, resp *contracts.FetchResponse, err error)
	}{
		{
			name: "valid_fetch_request",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: false,
			validate: func(t *testing.T, resp *contracts.FetchResponse, err error) {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
				if resp == nil {
					t.Error("Response should not be nil")
					return
				}
				if len(resp.Candles) == 0 {
					t.Error("Response should contain candles")
				}

				// Validate candle structure
				for _, candle := range resp.Candles {
					if candle.Open == "" {
						t.Error("Open price required")
					}
					if candle.High == "" {
						t.Error("High price required")
					}
					if candle.Low == "" {
						t.Error("Low price required")
					}
					if candle.Close == "" {
						t.Error("Close price required")
					}
					if candle.Volume == "" {
						t.Error("Volume required")
					}
					if candle.Pair != "BTC-USD" {
						t.Errorf("Expected pair BTC-USD, got %s", candle.Pair)
					}
					if candle.Interval != "1h" {
						t.Errorf("Expected interval 1h, got %s", candle.Interval)
					}
					if candle.Timestamp.IsZero() {
						t.Error("Timestamp should not be zero")
					}
				}

				// Validate rate limit info
				if resp.RateLimit.Remaining < 0 {
					t.Errorf("Rate limit remaining should be >= 0, got %d", resp.RateLimit.Remaining)
				}
			},
		},
		{
			name: "invalid_pair",
			req: contracts.FetchRequest{
				Pair:     "",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "invalid_time_range",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now(),
				End:      time.Now().Add(-24 * time.Hour), // End before start
				Interval: "1h",
				Limit:    100,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "invalid_interval",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "invalid",
				Limit:    100,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "zero_limit",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    0,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "negative_limit",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    -1,
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "context_cancellation",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx
			},
			expectError: true,
			errorType:   "context",
		},
		{
			name: "context_timeout",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			setupCtx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
				defer cancel()
				time.Sleep(2 * time.Nanosecond) // Ensure timeout
				return ctx
			},
			expectError: true,
			errorType:   "context",
		},
		{
			name: "large_limit",
			req: contracts.FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-24 * time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    10000, // Should be rejected or handled gracefully
			},
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
			errorType:   "validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			resp, err := fetcher.FetchCandles(ctx, tt.req)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				switch tt.errorType {
				case "context":
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Expected context error, got: %v", err)
					}
				case "validation":
					if !strings.Contains(strings.ToLower(err.Error()), "invalid") {
						t.Errorf("Expected validation error containing 'invalid', got: %v", err)
					}
				}
			} else if tt.validate != nil {
				tt.validate(t, resp, err)
			}
		})
	}
}

// TestPairProvider_GetTradingPairs tests the PairProvider interface contract
func TestPairProvider_GetTradingPairs(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var provider contracts.PairProvider = nil
	if provider == nil {
		t.Fatal("PairProvider implementation required")
	}

	tests := []struct {
		name        string
		setupCtx    func() context.Context
		expectError bool
		validate    func(t *testing.T, pairs []contracts.TradingPair, err error)
	}{
		{
			name:        "successful_fetch",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: false,
			validate: func(t *testing.T, pairs []contracts.TradingPair, err error) {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
				if len(pairs) == 0 {
					t.Error("Should return at least one trading pair")
				}

				for _, pair := range pairs {
					if pair.Symbol == "" {
						t.Error("Symbol required")
					}
					if pair.BaseAsset == "" {
						t.Error("BaseAsset required")
					}
					if pair.QuoteAsset == "" {
						t.Error("QuoteAsset required")
					}
					if pair.MinVolume == "" {
						t.Error("MinVolume required")
					}
					if pair.MaxVolume == "" {
						t.Error("MaxVolume required")
					}
					if pair.PriceStep == "" {
						t.Error("PriceStep required")
					}
				}
			},
		},
		{
			name: "context_cancellation",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expectError: true,
		},
		{
			name: "context_timeout",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
				defer cancel()
				time.Sleep(2 * time.Nanosecond)
				return ctx
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			pairs, err := provider.GetTradingPairs(ctx)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else if tt.validate != nil {
				tt.validate(t, pairs, err)
			}
		})
	}
}

// TestPairProvider_GetPairInfo tests the PairProvider GetPairInfo method
func TestPairProvider_GetPairInfo(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var provider contracts.PairProvider = nil
	if provider == nil {
		t.Fatal("PairProvider implementation required")
	}

	tests := []struct {
		name        string
		pair        string
		setupCtx    func() context.Context
		expectError bool
		validate    func(t *testing.T, info *contracts.PairInfo, err error)
	}{
		{
			name:        "valid_pair",
			pair:        "BTC-USD",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: false,
			validate: func(t *testing.T, info *contracts.PairInfo, err error) {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
				if info == nil {
					t.Error("PairInfo should not be nil")
					return
				}
				if info.Symbol != "BTC-USD" {
					t.Errorf("Expected symbol BTC-USD, got %s", info.Symbol)
				}
				if info.LastPrice == "" {
					t.Error("LastPrice required")
				}
				if info.Volume24h == "" {
					t.Error("Volume24h required")
				}
				if info.PriceChange == "" {
					t.Error("PriceChange required")
				}
				if info.UpdatedAt.IsZero() {
					t.Error("UpdatedAt required")
				}
			},
		},
		{
			name:        "empty_pair",
			pair:        "",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
		},
		{
			name:        "nonexistent_pair",
			pair:        "INVALID-PAIR",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: true,
		},
		{
			name: "context_cancellation",
			pair: "BTC-USD",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			info, err := provider.GetPairInfo(ctx, tt.pair)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else if tt.validate != nil {
				tt.validate(t, info, err)
			}
		})
	}
}

// TestRateLimitInfo_GetLimits tests the RateLimitInfo GetLimits method
func TestRateLimitInfo_GetLimits(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var rateLimiter contracts.RateLimitInfo = nil
	if rateLimiter == nil {
		t.Fatal("RateLimitInfo implementation required")
	}

	limits := rateLimiter.GetLimits()

	if limits.RequestsPerSecond <= 0 {
		t.Error("RequestsPerSecond must be positive")
	}
	if limits.BurstSize <= 0 {
		t.Error("BurstSize must be positive")
	}
	if limits.WindowDuration <= 0 {
		t.Error("WindowDuration must be positive")
	}

	// Burst size should typically be >= requests per second
	if limits.BurstSize < limits.RequestsPerSecond {
		t.Error("BurstSize should be at least equal to RequestsPerSecond")
	}
}

// TestRateLimitInfo_WaitForLimit tests the RateLimitInfo WaitForLimit method
func TestRateLimitInfo_WaitForLimit(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var rateLimiter contracts.RateLimitInfo = nil
	if rateLimiter == nil {
		t.Fatal("RateLimitInfo implementation required")
	}

	tests := []struct {
		name        string
		setupCtx    func() context.Context
		expectError bool
		maxDuration time.Duration
	}{
		{
			name:        "successful_wait",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: false,
			maxDuration: 5 * time.Second,
		},
		{
			name: "context_cancellation",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expectError: true,
			maxDuration: 1 * time.Second,
		},
		{
			name: "context_timeout",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				return ctx
			},
			expectError: true,
			maxDuration: 200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			start := time.Now()

			err := rateLimiter.WaitForLimit(ctx)
			duration := time.Since(start)

			if duration >= tt.maxDuration {
				t.Errorf("Wait duration %v should be less than maximum %v", duration, tt.maxDuration)
			}

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected context error, got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
			}
		})
	}
}

// TestHealthChecker_HealthCheck tests the HealthChecker interface contract
func TestHealthChecker_HealthCheck(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var checker contracts.HealthChecker = nil
	if checker == nil {
		t.Fatal("HealthChecker implementation required")
	}

	tests := []struct {
		name        string
		setupCtx    func() context.Context
		expectError bool
		maxDuration time.Duration
	}{
		{
			name:        "healthy_service",
			setupCtx:    func() context.Context { return context.Background() },
			expectError: false,
			maxDuration: 10 * time.Second,
		},
		{
			name: "context_cancellation",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expectError: true,
			maxDuration: 1 * time.Second,
		},
		{
			name: "context_timeout",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				return ctx
			},
			expectError: true,
			maxDuration: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			start := time.Now()

			err := checker.HealthCheck(ctx)
			duration := time.Since(start)

			if duration >= tt.maxDuration {
				t.Errorf("Health check duration %v should be less than maximum %v", duration, tt.maxDuration)
			}

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected context error, got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
			}
		})
	}
}

// TestExchangeAdapter_Integration tests the complete ExchangeAdapter workflow
func TestExchangeAdapter_Integration(t *testing.T) {
	// This will fail initially - replace nil with actual implementation
	var adapter contracts.ExchangeAdapter = nil
	if adapter == nil {
		t.Fatal("ExchangeAdapter implementation required")
	}

	ctx := context.Background()

	// Test health check first
	err := adapter.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("Exchange should be healthy before integration test: %v", err)
	}

	// Test rate limiting
	limits := adapter.GetLimits()
	if limits.RequestsPerSecond <= 0 {
		t.Error("RequestsPerSecond should be positive")
	}

	// Test getting trading pairs
	pairs, err := adapter.GetTradingPairs(ctx)
	if err != nil {
		t.Fatalf("Failed to get trading pairs: %v", err)
	}
	if len(pairs) == 0 {
		t.Fatal("Should have trading pairs available")
	}

	// Test getting pair info for first available pair
	pairInfo, err := adapter.GetPairInfo(ctx, pairs[0].Symbol)
	if err != nil {
		t.Fatalf("Failed to get pair info: %v", err)
	}
	if pairInfo == nil {
		t.Fatal("PairInfo should not be nil")
	}

	// Test rate limiting before data fetch
	err = adapter.WaitForLimit(ctx)
	if err != nil {
		t.Fatalf("Failed to wait for rate limit: %v", err)
	}

	// Test fetching candles
	req := contracts.FetchRequest{
		Pair:     pairs[0].Symbol,
		Start:    time.Now().Add(-2 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    10,
	}

	resp, err := adapter.FetchCandles(ctx, req)
	if err != nil {
		t.Fatalf("Failed to fetch candles: %v", err)
	}
	if resp == nil {
		t.Fatal("Response should not be nil")
	}
	if len(resp.Candles) == 0 {
		t.Error("Response should contain candles")
	}
}

// Benchmark tests for performance validation
func BenchmarkCandleFetcher_FetchCandles(b *testing.B) {
	// This will fail initially - replace nil with actual implementation
	var fetcher contracts.CandleFetcher = nil
	if fetcher == nil {
		b.Skip("CandleFetcher implementation not available")
	}

	req := contracts.FetchRequest{
		Pair:     "BTC-USD",
		Start:    time.Now().Add(-24 * time.Hour),
		End:      time.Now(),
		Interval: "1h",
		Limit:    100,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fetcher.FetchCandles(ctx, req)
		if err != nil {
			b.Fatalf("FetchCandles failed: %v", err)
		}
	}
}

func BenchmarkPairProvider_GetTradingPairs(b *testing.B) {
	// This will fail initially - replace nil with actual implementation
	var provider contracts.PairProvider = nil
	if provider == nil {
		b.Skip("PairProvider implementation not available")
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.GetTradingPairs(ctx)
		if err != nil {
			b.Fatalf("GetTradingPairs failed: %v", err)
		}
	}
}

// Test helper functions for validation
func validateCandleData(t *testing.T, candle contracts.Candle) {
	t.Helper()

	if candle.Open == "" {
		t.Error("Open price required")
	}
	if candle.High == "" {
		t.Error("High price required")
	}
	if candle.Low == "" {
		t.Error("Low price required")
	}
	if candle.Close == "" {
		t.Error("Close price required")
	}
	if candle.Volume == "" {
		t.Error("Volume required")
	}
	if candle.Pair == "" {
		t.Error("Pair required")
	}
	if candle.Interval == "" {
		t.Error("Interval required")
	}
	if candle.Timestamp.IsZero() {
		t.Error("Timestamp required")
	}
}

func validateTradingPair(t *testing.T, pair contracts.TradingPair) {
	t.Helper()

	if pair.Symbol == "" {
		t.Error("Symbol required")
	}
	if pair.BaseAsset == "" {
		t.Error("BaseAsset required")
	}
	if pair.QuoteAsset == "" {
		t.Error("QuoteAsset required")
	}
	if pair.MinVolume == "" {
		t.Error("MinVolume required")
	}
	if pair.MaxVolume == "" {
		t.Error("MaxVolume required")
	}
	if pair.PriceStep == "" {
		t.Error("PriceStep required")
	}
}

func validateRateLimit(t *testing.T, rateLimit contracts.RateLimit) {
	t.Helper()

	if rateLimit.RequestsPerSecond <= 0 {
		t.Error("RequestsPerSecond must be positive")
	}
	if rateLimit.BurstSize <= 0 {
		t.Error("BurstSize must be positive")
	}
	if rateLimit.WindowDuration <= 0 {
		t.Error("WindowDuration must be positive")
	}
}
