package exchange

import (
	"context"
	"testing"
	"time"
)

// TestInterfaceDefinitions verifies that our interface definitions are correct
// and that the types compile properly. This doesn't test implementations,
// but ensures the interfaces are well-formed.
func TestInterfaceDefinitions(t *testing.T) {
	// Test that interfaces can be declared (compile-time check)
	var (
		_ CandleFetcher   = (*testAdapter)(nil)
		_ PairProvider    = (*testAdapter)(nil)
		_ RateLimitInfo   = (*testAdapter)(nil)
		_ HealthChecker   = (*testAdapter)(nil)
		_ ExchangeAdapter = (*testAdapter)(nil)
	)
}

// testAdapter is a mock implementation to verify interface compliance
type testAdapter struct{}

func (t *testAdapter) FetchCandles(ctx context.Context, req FetchRequest) (*FetchResponse, error) {
	return nil, nil
}

func (t *testAdapter) GetTradingPairs(ctx context.Context) ([]TradingPair, error) {
	return nil, nil
}

func (t *testAdapter) GetPairInfo(ctx context.Context, pair string) (*PairInfo, error) {
	return nil, nil
}

func (t *testAdapter) GetLimits() RateLimit {
	return RateLimit{}
}

func (t *testAdapter) WaitForLimit(ctx context.Context) error {
	return nil
}

func (t *testAdapter) HealthCheck(ctx context.Context) error {
	return nil
}

// TestFetchRequestValidation tests the validation logic for FetchRequest
func TestFetchRequestValidation(t *testing.T) {
	tests := []struct {
		name        string
		request     FetchRequest
		expectError bool
	}{
		{
			name: "valid_request",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			expectError: false,
		},
		{
			name: "empty_pair",
			request: FetchRequest{
				Pair:     "",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			expectError: true,
		},
		{
			name: "empty_interval",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Now(),
				Interval: "",
				Limit:    100,
			},
			expectError: true,
		},
		{
			name: "zero_start_time",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Time{},
				End:      time.Now(),
				Interval: "1h",
				Limit:    100,
			},
			expectError: true,
		},
		{
			name: "zero_end_time",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Time{},
				Interval: "1h",
				Limit:    100,
			},
			expectError: true,
		},
		{
			name: "end_before_start",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now(),
				End:      time.Now().Add(-time.Hour),
				Interval: "1h",
				Limit:    100,
			},
			expectError: true,
		},
		{
			name: "negative_limit",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    -1,
			},
			expectError: true,
		},
		{
			name: "zero_limit",
			request: FetchRequest{
				Pair:     "BTC-USD",
				Start:    time.Now().Add(-time.Hour),
				End:      time.Now(),
				Interval: "1h",
				Limit:    0,
			},
			expectError: false, // Zero limit is valid (means no limit)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.expectError {
				if err == nil {
					t.Error("Expected validation error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error but got: %v", err)
				}
			}
		})
	}
}

// TestTradingPairValidation tests the validation logic for TradingPair
func TestTradingPairValidation(t *testing.T) {
	tests := []struct {
		name        string
		pair        TradingPair
		expectError bool
	}{
		{
			name: "valid_pair",
			pair: TradingPair{
				Symbol:     "BTC-USD",
				BaseAsset:  "BTC",
				QuoteAsset: "USD",
				Active:     true,
				MinVolume:  "0.001",
				MaxVolume:  "1000000",
				PriceStep:  "0.01",
			},
			expectError: false,
		},
		{
			name: "empty_symbol",
			pair: TradingPair{
				Symbol:     "",
				BaseAsset:  "BTC",
				QuoteAsset: "USD",
				Active:     true,
			},
			expectError: true,
		},
		{
			name: "empty_base_asset",
			pair: TradingPair{
				Symbol:     "BTC-USD",
				BaseAsset:  "",
				QuoteAsset: "USD",
				Active:     true,
			},
			expectError: true,
		},
		{
			name: "empty_quote_asset",
			pair: TradingPair{
				Symbol:     "BTC-USD",
				BaseAsset:  "BTC",
				QuoteAsset: "",
				Active:     true,
			},
			expectError: true,
		},
		{
			name: "empty_optional_fields",
			pair: TradingPair{
				Symbol:     "BTC-USD",
				BaseAsset:  "BTC",
				QuoteAsset: "USD",
				Active:     true,
				MinVolume:  "",
				MaxVolume:  "",
				PriceStep:  "",
			},
			expectError: false, // Optional fields can be empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pair.Validate()
			if tt.expectError {
				if err == nil {
					t.Error("Expected validation error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error but got: %v", err)
				}
			}
		})
	}
}

// TestRateLimitHelpers tests the helper methods for rate limit types
func TestRateLimitHelpers(t *testing.T) {
	t.Run("RateLimit_IsValid", func(t *testing.T) {
		tests := []struct {
			name     string
			limit    RateLimit
			expected bool
		}{
			{
				name: "valid_rate_limit",
				limit: RateLimit{
					RequestsPerSecond: 10,
					BurstSize:         15,
					WindowDuration:    time.Second,
				},
				expected: true,
			},
			{
				name: "zero_requests_per_second",
				limit: RateLimit{
					RequestsPerSecond: 0,
					BurstSize:         15,
					WindowDuration:    time.Second,
				},
				expected: false,
			},
			{
				name: "zero_burst_size",
				limit: RateLimit{
					RequestsPerSecond: 10,
					BurstSize:         0,
					WindowDuration:    time.Second,
				},
				expected: false,
			},
			{
				name: "zero_window_duration",
				limit: RateLimit{
					RequestsPerSecond: 10,
					BurstSize:         15,
					WindowDuration:    0,
				},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.limit.IsValid()
				if result != tt.expected {
					t.Errorf("Expected IsValid() to return %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("RateLimitStatus_HasCapacity", func(t *testing.T) {
		tests := []struct {
			name     string
			status   RateLimitStatus
			expected bool
		}{
			{
				name: "has_capacity",
				status: RateLimitStatus{
					Remaining: 5,
				},
				expected: true,
			},
			{
				name: "no_capacity",
				status: RateLimitStatus{
					Remaining: 0,
				},
				expected: false,
			},
			{
				name: "negative_capacity",
				status: RateLimitStatus{
					Remaining: -1,
				},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.status.HasCapacity()
				if result != tt.expected {
					t.Errorf("Expected HasCapacity() to return %v, got %v", tt.expected, result)
				}
			})
		}
	})

	t.Run("RateLimitStatus_NeedsWait", func(t *testing.T) {
		tests := []struct {
			name     string
			status   RateLimitStatus
			expected bool
		}{
			{
				name: "needs_wait",
				status: RateLimitStatus{
					RetryAfter: 5 * time.Second,
				},
				expected: true,
			},
			{
				name: "no_wait_needed",
				status: RateLimitStatus{
					RetryAfter: 0,
				},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.status.NeedsWait()
				if result != tt.expected {
					t.Errorf("Expected NeedsWait() to return %v, got %v", tt.expected, result)
				}
			})
		}
	})
}

// TestFetchRequestDuration tests the Duration helper method
func TestFetchRequestDuration(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()
	req := FetchRequest{
		Start: start,
		End:   end,
	}

	duration := req.Duration()
	expected := end.Sub(start)

	if duration != expected {
		t.Errorf("Expected duration %v, got %v", expected, duration)
	}
}
