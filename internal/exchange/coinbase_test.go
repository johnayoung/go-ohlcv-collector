package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// Test fixtures and mock data
const (
	btcUSDPair    = "BTC-USD"
	ethUSDPair    = "ETH-USD"
	invalidPair   = "INVALID-PAIR"
	testInterval  = "1h"
	testTimestamp = int64(1640995200) // 2022-01-01 00:00:00 UTC
)

// Mock server responses based on real Coinbase API structure
var (
	validCandlesResponse = struct {
		Candles []coinbaseCandle `json:"candles"`
	}{
		Candles: []coinbaseCandle{
			{
				Start:  testTimestamp,
				Open:   "47000.00",
				High:   "47500.00",
				Low:    "46500.00",
				Close:  "47200.00",
				Volume: "1.23456789",
			},
			{
				Start:  testTimestamp + 3600,
				Open:   "47200.00",
				High:   "47800.00",
				Low:    "47000.00",
				Close:  "47600.00",
				Volume: "2.34567890",
			},
		},
	}

	validProductsResponse = struct {
		Products []coinbaseProduct `json:"products"`
	}{
		Products: []coinbaseProduct{
			{
				ProductID:       "BTC-USD",
				BaseCurrencyID:  "BTC",
				QuoteCurrencyID: "USD",
				BaseIncrement:   "0.00000001",
				QuoteIncrement:  "0.01",
				BaseMinSize:     "0.00100000",
				BaseMaxSize:     "10000.00000000",
				QuoteMinSize:    "1.00",
				QuoteMaxSize:    "1000000.00",
				TradingDisabled: false,
				Status:          "online",
				Price:           "47500.00",
				Volume24h:       "1234.56789012",
				MidMarketPrice:  "47500.00",
				BaseName:        "Bitcoin",
				QuoteName:       "US Dollar",
			},
			{
				ProductID:       "ETH-USD",
				BaseCurrencyID:  "ETH",
				QuoteCurrencyID: "USD",
				BaseIncrement:   "0.00000001",
				QuoteIncrement:  "0.01",
				BaseMinSize:     "0.00100000",
				BaseMaxSize:     "5000.00000000",
				QuoteMinSize:    "1.00",
				QuoteMaxSize:    "500000.00",
				TradingDisabled: false,
				Status:          "online",
				Price:           "3500.00",
				Volume24h:       "5678.90123456",
				MidMarketPrice:  "3500.00",
				BaseName:        "Ethereum",
				QuoteName:       "US Dollar",
			},
		},
	}

	rateLimitResponse = `{
		"error": {
			"type": "rate_limit_exceeded",
			"message": "Too many requests"
		}
	}`

	malformedJSONResponse = `{"candles": [{"start": "invalid_timestamp"}]`

	serverErrorResponse = `{
		"error": {
			"type": "internal_server_error",
			"message": "Internal server error"
		}
	}`

	clientErrorResponse = `{
		"error": {
			"type": "invalid_request",
			"message": "Invalid trading pair"
		}
	}`
)

// Test utilities
func createTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func createMockServer(responses map[string]func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if handler, exists := responses[path]; exists {
			handler(w, r)
		} else {
			http.NotFound(w, r)
		}
	}))
}

func TestNewCoinbaseAdapter(t *testing.T) {
	t.Run("creates adapter with default configuration", func(t *testing.T) {
		adapter := NewCoinbaseAdapter()

		assert.NotNil(t, adapter)
		assert.NotNil(t, adapter.httpClient)
		assert.NotNil(t, adapter.rateLimiter)
		assert.Equal(t, coinbaseBaseURL, adapter.baseURL)
		assert.NotNil(t, adapter.logger)
		assert.NotNil(t, adapter.pairCache)
		assert.Equal(t, 5*time.Minute, adapter.pairCacheTTL)
		assert.Equal(t, requestTimeout, adapter.httpClient.Timeout)
	})

	t.Run("creates adapter with custom logger", func(t *testing.T) {
		logger := createTestLogger()
		adapter := NewCoinbaseAdapterWithLogger(logger)

		assert.NotNil(t, adapter)
		assert.Equal(t, logger, adapter.logger)
	})
}

func TestCoinbaseAdapter_FetchCandles(t *testing.T) {
	ctx := context.Background()

	t.Run("fetches candles successfully", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				// Verify query parameters
				query := r.URL.Query()
				assert.NotEmpty(t, query.Get("start"))
				assert.NotEmpty(t, query.Get("end"))
				assert.Equal(t, "3600", query.Get("granularity"))

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+7200, 0), // 2 hours later
			Interval: testInterval,
			Limit:    10,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Len(t, response.Candles, 2)
		assert.Equal(t, btcUSDPair, response.Candles[0].Pair)
		assert.Equal(t, testInterval, response.Candles[0].Interval)
		assert.Equal(t, "47000.00", response.Candles[0].Open)
		assert.Equal(t, "47500.00", response.Candles[0].High)
		assert.Equal(t, "46500.00", response.Candles[0].Low)
		assert.Equal(t, "47200.00", response.Candles[0].Close)
		assert.Equal(t, "1.23456789", response.Candles[0].Volume)
	})

	t.Run("handles invalid request", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

		req := FetchRequest{
			Pair:     "", // Invalid: empty pair
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "invalid request")
	})

	t.Run("handles unsupported interval", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: "30s", // Unsupported interval
		}

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "unsupported interval")
	})

	t.Run("handles rate limiting", func(t *testing.T) {
		callCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				callCount++
				if callCount == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte(rateLimitResponse))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		startTime := time.Now()
		response, err := adapter.FetchCandles(ctx, req)
		elapsed := time.Since(startTime)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.GreaterOrEqual(t, elapsed, time.Second) // Should have waited
		assert.Equal(t, 2, callCount)                  // Should have retried
	})

	t.Run("handles server errors with retry", func(t *testing.T) {
		callCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				callCount++
				if callCount <= 2 {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(serverErrorResponse))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, 3, callCount) // Should have retried twice before success
	})

	t.Run("handles client errors without retry", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", invalidPair): func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(clientErrorResponse))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     invalidPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "client error")
	})

	t.Run("handles malformed JSON response", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(malformedJSONResponse))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "failed to parse candles response")
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond) // Simulate slow response
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "context")
	})

	t.Run("handles large time range with chunking", func(t *testing.T) {
		requestCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// Request data spanning more than maxCandlesPerRequest hours
		hoursToRequest := maxCandlesPerRequest + 100
		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+int64(hoursToRequest*3600), 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Greater(t, requestCount, 1) // Should have made multiple requests
	})

	t.Run("respects limit parameter", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				// Return more candles than the limit
				largeCandlesResponse := validCandlesResponse
				for i := 0; i < 10; i++ {
					largeCandlesResponse.Candles = append(largeCandlesResponse.Candles, coinbaseCandle{
						Start:  testTimestamp + int64((i+3)*3600),
						Open:   "47000.00",
						High:   "47500.00",
						Low:    "46500.00",
						Close:  "47200.00",
						Volume: "1.23456789",
					})
				}
				json.NewEncoder(w).Encode(largeCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+43200, 0), // 12 hours
			Interval: testInterval,
			Limit:    3, // Only want 3 candles
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.LessOrEqual(t, len(response.Candles), 3)
	})

	t.Run("includes pagination token when appropriate", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// Request more data than available, with a limit
		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+36000, 0), // 10 hours
			Interval: testInterval,
			Limit:    1, // Only want 1 candle
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Len(t, response.Candles, 1)
		// Should have next token since we limited results
		assert.NotEmpty(t, response.NextToken)
	})

	t.Run("handles network errors with retry", func(t *testing.T) {
		// Create a server that closes immediately to simulate network error
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Close connection immediately
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
			}
		}))
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, response)
		// Should contain network-related error
		assert.True(t, strings.Contains(err.Error(), "connection") ||
			strings.Contains(err.Error(), "network") ||
			strings.Contains(err.Error(), "EOF"))
	})
}

func TestCoinbaseAdapter_GetTradingPairs(t *testing.T) {
	ctx := context.Background()

	t.Run("fetches trading pairs successfully", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		pairs, err := adapter.GetTradingPairs(ctx)

		require.NoError(t, err)
		assert.Len(t, pairs, 2)

		btcPair := pairs[0]
		assert.Equal(t, "BTC-USD", btcPair.Symbol)
		assert.Equal(t, "BTC", btcPair.BaseAsset)
		assert.Equal(t, "USD", btcPair.QuoteAsset)
		assert.True(t, btcPair.Active)
		assert.Equal(t, "0.00100000", btcPair.MinVolume)
		assert.Equal(t, "10000.00000000", btcPair.MaxVolume)
		assert.Equal(t, "0.01", btcPair.PriceStep)
	})

	t.Run("uses cached data when available", func(t *testing.T) {
		callCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				callCount++
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// First call should hit the API
		pairs1, err1 := adapter.GetTradingPairs(ctx)
		require.NoError(t, err1)
		assert.Equal(t, 1, callCount)

		// Second call should use cache
		pairs2, err2 := adapter.GetTradingPairs(ctx)
		require.NoError(t, err2)
		assert.Equal(t, 1, callCount) // Should not increment
		assert.Equal(t, pairs1, pairs2)
	})

	t.Run("refreshes cache after TTL expires", func(t *testing.T) {
		callCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				callCount++
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL
		adapter.pairCacheTTL = 50 * time.Millisecond // Very short TTL for testing

		// First call
		_, err1 := adapter.GetTradingPairs(ctx)
		require.NoError(t, err1)
		assert.Equal(t, 1, callCount)

		// Wait for cache to expire
		time.Sleep(100 * time.Millisecond)

		// Second call should hit API again
		_, err2 := adapter.GetTradingPairs(ctx)
		require.NoError(t, err2)
		assert.Equal(t, 2, callCount)
	})

	t.Run("handles malformed response", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"products": [{"invalid": "data"}]}`))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		pairs, err := adapter.GetTradingPairs(ctx)

		require.NoError(t, err) // Should not error on malformed product data
		assert.NotNil(t, pairs)
	})

	t.Run("handles API errors", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(serverErrorResponse))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		pairs, err := adapter.GetTradingPairs(ctx)

		assert.Error(t, err)
		assert.Nil(t, pairs)
	})
}

func TestCoinbaseAdapter_GetPairInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("returns cached pair info", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// First populate the cache
		_, err := adapter.GetTradingPairs(ctx)
		require.NoError(t, err)

		// Then get specific pair info
		pairInfo, err := adapter.GetPairInfo(ctx, btcUSDPair)

		require.NoError(t, err)
		assert.NotNil(t, pairInfo)
		assert.Equal(t, btcUSDPair, pairInfo.Symbol)
		assert.Equal(t, "BTC", pairInfo.BaseAsset)
		assert.Equal(t, "USD", pairInfo.QuoteAsset)
	})

	t.Run("refreshes cache when pair not found", func(t *testing.T) {
		callCount := 0
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				callCount++
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// Get pair info without pre-populating cache
		pairInfo, err := adapter.GetPairInfo(ctx, btcUSDPair)

		require.NoError(t, err)
		assert.NotNil(t, pairInfo)
		assert.Equal(t, 1, callCount) // Should have called API to refresh cache
	})

	t.Run("returns error for non-existent pair", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validProductsResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		pairInfo, err := adapter.GetPairInfo(ctx, "NONEXISTENT-PAIR")

		assert.Error(t, err)
		assert.Nil(t, pairInfo)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("handles API errors when refreshing cache", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(serverErrorResponse))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		pairInfo, err := adapter.GetPairInfo(ctx, btcUSDPair)

		assert.Error(t, err)
		assert.Nil(t, pairInfo)
		assert.Contains(t, err.Error(), "failed to refresh trading pairs")
	})
}

func TestCoinbaseAdapter_RateLimit(t *testing.T) {
	t.Run("GetLimits returns correct configuration", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

		limits := adapter.GetLimits()

		assert.Equal(t, maxRequestsPerSecond, limits.RequestsPerSecond)
		assert.Equal(t, rateLimitBurst, limits.BurstSize)
		assert.Equal(t, rateLimitWindow, limits.WindowDuration)
	})

	t.Run("WaitForLimit respects rate limiting", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

		// Create a very restrictive rate limiter for testing
		adapter.rateLimiter = rate.NewLimiter(1, 1) // 1 req/sec, burst of 1

		ctx := context.Background()

		// First call should not wait
		start := time.Now()
		err1 := adapter.WaitForLimit(ctx)
		elapsed1 := time.Since(start)

		require.NoError(t, err1)
		assert.Less(t, elapsed1, 100*time.Millisecond)

		// Second call should wait
		start = time.Now()
		err2 := adapter.WaitForLimit(ctx)
		elapsed2 := time.Since(start)

		require.NoError(t, err2)
		assert.GreaterOrEqual(t, elapsed2, 900*time.Millisecond) // Should wait ~1 second
	})

	t.Run("WaitForLimit handles context cancellation", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.rateLimiter = rate.NewLimiter(0.1, 1) // Very slow rate

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Consume the burst capacity
		adapter.rateLimiter.Wait(context.Background())

		// This should be cancelled by context timeout
		err := adapter.WaitForLimit(ctx)

		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("getRateLimitStatus provides status information", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

		status := adapter.getRateLimitStatus()

		assert.GreaterOrEqual(t, status.Remaining, 0)
		assert.False(t, status.ResetTime.IsZero())
		assert.GreaterOrEqual(t, status.RetryAfter, 0*time.Second)
	})
}

func TestCoinbaseAdapter_HealthCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("passes when API is healthy", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				// Verify limit parameter is present
				query := r.URL.Query()
				assert.Equal(t, "1", query.Get("limit"))

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(struct {
					Products []coinbaseProduct `json:"products"`
				}{
					Products: []coinbaseProduct{validProductsResponse.Products[0]},
				})
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		err := adapter.HealthCheck(ctx)

		assert.NoError(t, err)
	})

	t.Run("fails when API returns error", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(serverErrorResponse))
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		err := adapter.HealthCheck(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "health check failed")
	})

	t.Run("respects health check timeout", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(10 * time.Second) // Longer than health check timeout
				w.WriteHeader(http.StatusOK)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		start := time.Now()
		err := adapter.HealthCheck(ctx)
		elapsed := time.Since(start)

		assert.Error(t, err)
		assert.Less(t, elapsed, 10*time.Second) // Should timeout before 10 seconds
	})

	t.Run("handles network errors", func(t *testing.T) {
		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = "http://localhost:9999" // Non-existent server

		err := adapter.HealthCheck(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "health check request failed")
	})
}

func TestCoinbaseAdapter_IntervalConversion(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	testCases := []struct {
		interval string
		expected int
		hasError bool
	}{
		{"1m", 60, false},
		{"1min", 60, false},
		{"5m", 300, false},
		{"5min", 300, false},
		{"15m", 900, false},
		{"15min", 900, false},
		{"1h", 3600, false},
		{"1hour", 3600, false},
		{"6h", 21600, false},
		{"6hour", 21600, false},
		{"1d", 86400, false},
		{"1day", 86400, false},
		{"30s", 0, true}, // Unsupported
		{"invalid", 0, true},
		{"", 0, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("converts %s", tc.interval), func(t *testing.T) {
			result, err := adapter.convertInterval(tc.interval)

			if tc.hasError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported interval")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCoinbaseAdapter_CandleConversion(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	t.Run("converts valid candle data", func(t *testing.T) {
		coinbaseCandle := coinbaseCandle{
			Start:  testTimestamp,
			Open:   "47000.00",
			High:   "47500.00",
			Low:    "46500.00",
			Close:  "47200.00",
			Volume: "1.23456789",
		}

		candle, err := adapter.convertCandleToModel(coinbaseCandle, btcUSDPair, testInterval)

		require.NoError(t, err)
		assert.NotNil(t, candle)
		assert.Equal(t, time.Unix(testTimestamp, 0).UTC(), candle.Timestamp)
		assert.Equal(t, "47000.00", candle.Open)
		assert.Equal(t, "47500.00", candle.High)
		assert.Equal(t, "46500.00", candle.Low)
		assert.Equal(t, "47200.00", candle.Close)
		assert.Equal(t, "1.23456789", candle.Volume)
		assert.Equal(t, btcUSDPair, candle.Pair)
		assert.Equal(t, testInterval, candle.Interval)
	})

	t.Run("handles invalid price data", func(t *testing.T) {
		coinbaseCandle := coinbaseCandle{
			Start:  testTimestamp,
			Open:   "invalid",
			High:   "47500.00",
			Low:    "46500.00",
			Close:  "47200.00",
			Volume: "1.23456789",
		}

		candle, err := adapter.convertCandleToModel(coinbaseCandle, btcUSDPair, testInterval)

		assert.Error(t, err)
		assert.Nil(t, candle)
	})
}

func TestCoinbaseAdapter_ProductConversion(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	t.Run("converts valid product data", func(t *testing.T) {
		product := coinbaseProduct{
			ProductID:       "BTC-USD",
			BaseCurrencyID:  "BTC",
			QuoteCurrencyID: "USD",
			BaseMinSize:     "0.001",
			BaseMaxSize:     "10000.0",
			QuoteIncrement:  "0.01",
			TradingDisabled: false,
		}

		pair := adapter.convertProductToTradingPair(product)

		assert.Equal(t, "BTC-USD", pair.Symbol)
		assert.Equal(t, "BTC", pair.BaseAsset)
		assert.Equal(t, "USD", pair.QuoteAsset)
		assert.True(t, pair.Active)
		assert.Equal(t, "0.001", pair.MinVolume)
		assert.Equal(t, "10000.0", pair.MaxVolume)
		assert.Equal(t, "0.01", pair.PriceStep)
	})

	t.Run("handles disabled trading pairs", func(t *testing.T) {
		product := coinbaseProduct{
			ProductID:       "OLD-PAIR",
			BaseCurrencyID:  "OLD",
			QuoteCurrencyID: "USD",
			TradingDisabled: true,
		}

		pair := adapter.convertProductToTradingPair(product)

		assert.Equal(t, "OLD-PAIR", pair.Symbol)
		assert.False(t, pair.Active)
	})

	t.Run("parses symbol from product ID when currencies not provided", func(t *testing.T) {
		product := coinbaseProduct{
			ProductID:       "BTC-EUR",
			BaseCurrencyID:  "",
			QuoteCurrencyID: "",
		}

		pair := adapter.convertProductToTradingPair(product)

		assert.Equal(t, "BTC-EUR", pair.Symbol)
		assert.Equal(t, "BTC", pair.BaseAsset)
		assert.Equal(t, "EUR", pair.QuoteAsset)
	})
}

func TestCoinbaseAdapter_ChunkCalculation(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	t.Run("single chunk for small time range", func(t *testing.T) {
		start := time.Unix(testTimestamp, 0)
		end := start.Add(2 * time.Hour)

		chunks, err := adapter.calculateChunks(start, end, 3600, 0) // 1-hour candles

		require.NoError(t, err)
		assert.Len(t, chunks, 1)
		assert.Equal(t, start, chunks[0].start)
		assert.Equal(t, end, chunks[0].end)
	})

	t.Run("multiple chunks for large time range", func(t *testing.T) {
		start := time.Unix(testTimestamp, 0)
		// Request more hours than maxCandlesPerRequest
		end := start.Add(time.Duration(maxCandlesPerRequest+100) * time.Hour)

		chunks, err := adapter.calculateChunks(start, end, 3600, 0) // 1-hour candles

		require.NoError(t, err)
		assert.Greater(t, len(chunks), 1)

		// Verify chunks are contiguous
		for i := 1; i < len(chunks); i++ {
			assert.Equal(t, chunks[i-1].end, chunks[i].start)
		}

		// Verify first and last chunks
		assert.Equal(t, start, chunks[0].start)
		assert.Equal(t, end, chunks[len(chunks)-1].end)
	})

	t.Run("respects limit parameter", func(t *testing.T) {
		start := time.Unix(testTimestamp, 0)
		end := start.Add(100 * time.Hour)
		limit := 50

		chunks, err := adapter.calculateChunks(start, end, 3600, limit)

		require.NoError(t, err)

		// Calculate total candles across all chunks
		totalCandles := 0
		for _, chunk := range chunks {
			candlesInChunk := int(chunk.end.Sub(chunk.start) / (3600 * time.Second))
			totalCandles += candlesInChunk
		}

		assert.LessOrEqual(t, totalCandles, limit)
	})

	t.Run("handles zero limit", func(t *testing.T) {
		start := time.Unix(testTimestamp, 0)
		end := start.Add(10 * time.Hour)

		chunks, err := adapter.calculateChunks(start, end, 3600, 0) // No limit

		require.NoError(t, err)
		assert.Greater(t, len(chunks), 0)
	})
}

func TestCoinbaseAdapter_TokenParsing(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	t.Run("encodes next token correctly", func(t *testing.T) {
		nextStart := time.Unix(testTimestamp+3600, 0)
		req := FetchRequest{
			Pair:     btcUSDPair,
			End:      time.Unix(testTimestamp+7200, 0),
			Interval: testInterval,
			Limit:    100,
		}

		token := adapter.encodeNextToken(nextStart, req)

		assert.NotEmpty(t, token)
		assert.Contains(t, token, btcUSDPair)
		assert.Contains(t, token, testInterval)
		assert.Contains(t, token, strconv.FormatInt(nextStart.Unix(), 10))
	})
}

func TestCoinbaseAdapter_RetryAfterParsing(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())

	testCases := []struct {
		name     string
		header   string
		expected time.Duration
	}{
		{"empty header", "", 0},
		{"numeric seconds", "120", 120 * time.Second},
		{"invalid numeric", "invalid", 0},
		{"HTTP date format", time.Now().Add(5 * time.Second).Format(time.RFC1123), 4 * time.Second}, // Allow 1s variance
		{"invalid date", "Invalid Date", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := adapter.parseRetryAfter(tc.header)

			if tc.name == "HTTP date format" {
				// For date parsing, allow some variance due to timing
				assert.True(t, result >= 3*time.Second && result <= 6*time.Second,
					"Expected ~5s, got %v", result)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCoinbaseAdapter_ValidationErrorHandling(t *testing.T) {
	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
	ctx := context.Background()

	testCases := []struct {
		name        string
		req         FetchRequest
		expectError bool
		errorMatch  string
	}{
		{
			name: "empty pair",
			req: FetchRequest{
				Pair:     "",
				Start:    time.Unix(testTimestamp, 0),
				End:      time.Unix(testTimestamp+3600, 0),
				Interval: testInterval,
			},
			expectError: true,
			errorMatch:  "trading pair cannot be empty",
		},
		{
			name: "empty interval",
			req: FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Unix(testTimestamp, 0),
				End:      time.Unix(testTimestamp+3600, 0),
				Interval: "",
			},
			expectError: true,
			errorMatch:  "interval cannot be empty",
		},
		{
			name: "zero start time",
			req: FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Time{},
				End:      time.Unix(testTimestamp+3600, 0),
				Interval: testInterval,
			},
			expectError: true,
			errorMatch:  "start time cannot be zero",
		},
		{
			name: "zero end time",
			req: FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Unix(testTimestamp, 0),
				End:      time.Time{},
				Interval: testInterval,
			},
			expectError: true,
			errorMatch:  "end time cannot be zero",
		},
		{
			name: "end before start",
			req: FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Unix(testTimestamp+3600, 0),
				End:      time.Unix(testTimestamp, 0),
				Interval: testInterval,
			},
			expectError: true,
			errorMatch:  "end time must be after start time",
		},
		{
			name: "negative limit",
			req: FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Unix(testTimestamp, 0),
				End:      time.Unix(testTimestamp+3600, 0),
				Interval: testInterval,
				Limit:    -1,
			},
			expectError: true,
			errorMatch:  "limit cannot be negative",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := adapter.FetchCandles(ctx, tc.req)

			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMatch)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCoinbaseAdapter_ConcurrentRequests(t *testing.T) {
	ctx := context.Background()
	server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
		fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
			// Add small delay to simulate real API
			time.Sleep(10 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(validCandlesResponse)
		},
	})
	defer server.Close()

	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
	adapter.baseURL = server.URL

	// Test concurrent requests don't interfere with each other
	numRequests := 5
	results := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			req := FetchRequest{
				Pair:     btcUSDPair,
				Start:    time.Unix(testTimestamp, 0),
				End:      time.Unix(testTimestamp+3600, 0),
				Interval: testInterval,
			}
			_, err := adapter.FetchCandles(ctx, req)
			results <- err
		}()
	}

	// Collect all results
	for i := 0; i < numRequests; i++ {
		err := <-results
		assert.NoError(t, err, "Request %d should not error", i+1)
	}
}

func TestCoinbaseAdapter_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("handles empty candles response", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(struct {
					Candles []coinbaseCandle `json:"candles"`
				}{
					Candles: []coinbaseCandle{}, // Empty candles
				})
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Len(t, response.Candles, 0)
		assert.Empty(t, response.NextToken)
	})

	t.Run("handles candles with invalid decimal values", func(t *testing.T) {
		invalidCandlesResponse := struct {
			Candles []coinbaseCandle `json:"candles"`
		}{
			Candles: []coinbaseCandle{
				{
					Start:  testTimestamp,
					Open:   "invalid_price",
					High:   "47500.00",
					Low:    "46500.00",
					Close:  "47200.00",
					Volume: "1.23456789",
				},
				{
					Start:  testTimestamp + 3600,
					Open:   "47000.00",
					High:   "47500.00",
					Low:    "46500.00",
					Close:  "47200.00",
					Volume: "2.34567890",
				},
			},
		}

		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(invalidCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+7200, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		// Should skip invalid candle and return only valid one
		assert.Len(t, response.Candles, 1)
		assert.Equal(t, "47000.00", response.Candles[0].Open)
	})

	t.Run("handles very large time ranges", func(t *testing.T) {
		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(validCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		// Request 1 year of hourly data
		start := time.Unix(testTimestamp, 0)
		end := start.AddDate(1, 0, 0)

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    start,
			End:      end,
			Interval: testInterval,
			Limit:    1000, // Reasonable limit
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.LessOrEqual(t, len(response.Candles), 1000)
	})

	t.Run("handles zero volume candles", func(t *testing.T) {
		zeroVolumeCandlesResponse := struct {
			Candles []coinbaseCandle `json:"candles"`
		}{
			Candles: []coinbaseCandle{
				{
					Start:  testTimestamp,
					Open:   "47000.00",
					High:   "47000.00",
					Low:    "47000.00",
					Close:  "47000.00",
					Volume: "0.0", // Zero volume
				},
			},
		}

		server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
			fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(zeroVolumeCandlesResponse)
			},
		})
		defer server.Close()

		adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
		adapter.baseURL = server.URL

		req := FetchRequest{
			Pair:     btcUSDPair,
			Start:    time.Unix(testTimestamp, 0),
			End:      time.Unix(testTimestamp+3600, 0),
			Interval: testInterval,
		}

		response, err := adapter.FetchCandles(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Len(t, response.Candles, 1)
		assert.Equal(t, "0.0", response.Candles[0].Volume)
	})
}

// Benchmark tests
func BenchmarkCoinbaseAdapter_FetchCandles(b *testing.B) {
	server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
		fmt.Sprintf("/api/v3/brokerage/products/%s/candles", btcUSDPair): func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(validCandlesResponse)
		},
	})
	defer server.Close()

	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
	adapter.baseURL = server.URL

	req := FetchRequest{
		Pair:     btcUSDPair,
		Start:    time.Unix(testTimestamp, 0),
		End:      time.Unix(testTimestamp+3600, 0),
		Interval: testInterval,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := adapter.FetchCandles(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCoinbaseAdapter_GetTradingPairs(b *testing.B) {
	server := createMockServer(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v3/brokerage/products": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(validProductsResponse)
		},
	})
	defer server.Close()

	adapter := NewCoinbaseAdapterWithLogger(createTestLogger())
	adapter.baseURL = server.URL
	adapter.pairCacheTTL = 1 * time.Nanosecond // Force cache miss every time

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := adapter.GetTradingPairs(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}
