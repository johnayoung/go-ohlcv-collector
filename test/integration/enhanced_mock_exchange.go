package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// ExchangeAPIError represents API errors from the exchange
type ExchangeAPIError struct {
	Code    int
	Message string
}

func (e *ExchangeAPIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.Code, e.Message)
}

// EnhancedMockExchangeAdapter provides comprehensive mock exchange functionality
type EnhancedMockExchangeAdapter struct {
	// Basic fields
	fetchDelay        time.Duration
	fetchCallCount    int64
	shouldFailFetch   bool
	rateLimitDelay    time.Duration
	healthCheckFails  bool
	tradingPairs      []contracts.TradingPair
	mu                sync.RWMutex

	// Enhanced fields
	testCandles       []contracts.Candle
	networkErrorCount int
	apiErrorCount     int
	responseDelay     time.Duration
	healthy           bool
}

// NewEnhancedMockExchangeAdapter creates an enhanced mock exchange adapter
func NewEnhancedMockExchangeAdapter() *EnhancedMockExchangeAdapter {
	return &EnhancedMockExchangeAdapter{
		fetchDelay: 100 * time.Millisecond,
		tradingPairs: []contracts.TradingPair{
			{Symbol: "BTC-USD", BaseAsset: "BTC", QuoteAsset: "USD", Active: true},
			{Symbol: "ETH-USD", BaseAsset: "ETH", QuoteAsset: "USD", Active: true},
		},
		healthy: true,
	}
}

// SetCandles sets the test candles data
func (e *EnhancedMockExchangeAdapter) SetCandles(candles []contracts.Candle) {
	e.testCandles = make([]contracts.Candle, len(candles))
	copy(e.testCandles, candles)
}

// FetchCandles overrides the parent method to use test data
func (e *EnhancedMockExchangeAdapter) FetchCandles(ctx context.Context, req contracts.FetchRequest) (*contracts.FetchResponse, error) {
	atomic.AddInt64(&e.fetchCallCount, 1)
	
	// Simulate response delay if set
	if e.responseDelay > 0 {
		select {
		case <-time.After(e.responseDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Simulate network errors
	if e.networkErrorCount > 0 {
		e.networkErrorCount--
		return nil, assert.AnError
	}

	// Simulate API errors
	if e.apiErrorCount > 0 {
		e.apiErrorCount--
		return nil, &ExchangeAPIError{Code: 500, Message: "Internal server error"}
	}

	e.mu.RLock()
	shouldFail := e.shouldFailFetch
	e.mu.RUnlock()

	if shouldFail {
		return nil, assert.AnError
	}

	// Filter test candles by request parameters
	var matchingCandles []contracts.Candle
	for _, candle := range e.testCandles {
		// Match pair and interval
		if candle.Pair != req.Pair || candle.Interval != req.Interval {
			continue
		}

		// Match time range - but adjust timestamps for recent requests
		candleTimestamp := candle.Timestamp
		
		// If this is a recent request (within 7 days), adjust timestamps to be current
		// This supports scheduled collection tests that expect fresh data
		if !req.Start.IsZero() && time.Since(req.Start) < 7*24*time.Hour {
			// Adjust timestamp to be within the requested range
			relativeTime := candle.Timestamp.Sub(time.Now().AddDate(0, 0, -30)) // Original test data starts 30 days ago
			candleTimestamp = req.Start.Add(relativeTime)
			
			// Ensure it's within the request range
			if candleTimestamp.Before(req.Start) {
				candleTimestamp = req.Start
			}
			if !req.End.IsZero() && candleTimestamp.After(req.End) {
				candleTimestamp = req.End
			}
		}

		// Check time range with adjusted timestamp
		if !req.Start.IsZero() && candleTimestamp.Before(req.Start) {
			continue
		}
		if !req.End.IsZero() && candleTimestamp.After(req.End) {
			continue
		}

		// Create a copy with adjusted timestamp
		adjustedCandle := candle
		adjustedCandle.Timestamp = candleTimestamp
		matchingCandles = append(matchingCandles, adjustedCandle)
	}

	// Apply limit if specified
	if req.Limit > 0 && len(matchingCandles) > req.Limit {
		matchingCandles = matchingCandles[:req.Limit]
	}

	return &contracts.FetchResponse{
		Candles: matchingCandles,
		RateLimit: contracts.RateLimitStatus{
			Remaining:  100,
			ResetTime:  time.Now().Add(time.Minute),
			RetryAfter: 0,
		},
	}, nil
}

// TriggerRateLimit sets up rate limiting for the next request
func (e *EnhancedMockExchangeAdapter) TriggerRateLimit() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rateLimitDelay = time.Second // 1 second delay
}

// SimulateNetworkError simulates network errors for the next count requests
func (e *EnhancedMockExchangeAdapter) SimulateNetworkError(count int) {
	e.networkErrorCount = count
}

// SimulateAPIError simulates API errors for the next count requests
func (e *EnhancedMockExchangeAdapter) SimulateAPIError(count int) {
	e.apiErrorCount = count
}

// SetResponseDelay sets the response delay for simulating slow requests
func (e *EnhancedMockExchangeAdapter) SetResponseDelay(delay time.Duration) {
	e.responseDelay = delay
}

// SetHealthy sets the health status of the exchange
func (e *EnhancedMockExchangeAdapter) SetHealthy(healthy bool) {
	e.healthy = healthy
}

// HealthCheck returns the current health status
func (e *EnhancedMockExchangeAdapter) HealthCheck(ctx context.Context) error {
	if !e.healthy {
		return assert.AnError
	}
	return nil
}

// GetTradingPairs returns available trading pairs
func (e *EnhancedMockExchangeAdapter) GetTradingPairs(ctx context.Context) ([]contracts.TradingPair, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tradingPairs, nil
}

// GetPairInfo returns information about a specific trading pair
func (e *EnhancedMockExchangeAdapter) GetPairInfo(ctx context.Context, pair string) (*contracts.PairInfo, error) {
	return &contracts.PairInfo{
		TradingPair: contracts.TradingPair{Symbol: pair, Active: true},
		LastPrice:   "50000.00",
		Volume24h:   "1000.0",
		UpdatedAt:   time.Now(),
	}, nil
}

// GetLimits returns rate limiting information
func (e *EnhancedMockExchangeAdapter) GetLimits() contracts.RateLimit {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return contracts.RateLimit{
		RequestsPerSecond: 10,
		BurstSize:         5,
		WindowDuration:    time.Second,
	}
}

// WaitForLimit waits for rate limiting if necessary
func (e *EnhancedMockExchangeAdapter) WaitForLimit(ctx context.Context) error {
	e.mu.RLock()
	delay := e.rateLimitDelay
	e.mu.RUnlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}