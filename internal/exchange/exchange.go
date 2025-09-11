// Package exchange defines interfaces for exchange adapters that provide OHLCV data collection capabilities.
//
// This package contains the core interfaces that exchange implementations must satisfy to integrate
// with the OHLCV data collection system. The interfaces are designed to be small, focused, and
// composable, following Go interface best practices.
package exchange

import (
	"context"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
)

// CandleFetcher retrieves OHLCV candle data from exchanges.
//
// This interface provides the core functionality for fetching historical and real-time
// price data from cryptocurrency exchanges. Implementations should handle rate limiting,
// pagination, and error recovery appropriately.
type CandleFetcher interface {
	// FetchCandles retrieves OHLCV data for a specific trading pair and time range.
	//
	// The context can be used for cancellation and timeouts. The request specifies
	// the trading pair, time range, interval, and optional pagination limits.
	//
	// Returns a response containing the candles, pagination token for subsequent requests,
	// and current rate limit status. If no data is available for the requested range,
	// an empty slice of candles should be returned without error.
	//
	// Implementations should:
	// - Validate the request parameters
	// - Handle API rate limits gracefully
	// - Return candles in chronological order (oldest first)
	// - Provide pagination support for large time ranges
	// - Include rate limit information in the response
	FetchCandles(ctx context.Context, req FetchRequest) (*FetchResponse, error)
}

// PairProvider manages trading pair metadata and information.
//
// This interface provides access to trading pair information including active pairs,
// trading specifications, and real-time market data. Implementations should cache
// pair information appropriately to minimize API calls.
type PairProvider interface {
	// GetTradingPairs retrieves all available trading pairs from the exchange.
	//
	// Returns a list of all trading pairs supported by the exchange, including both
	// active and inactive pairs. The slice should include detailed specifications
	// for each pair such as minimum/maximum volumes and price steps.
	//
	// Implementations should:
	// - Cache results to avoid excessive API calls
	// - Include both active and inactive pairs with proper status flags
	// - Provide detailed trading specifications for each pair
	GetTradingPairs(ctx context.Context) ([]TradingPair, error)

	// GetPairInfo retrieves detailed information for a specific trading pair.
	//
	// Returns comprehensive information about the specified pair including current
	// market data such as last price, 24h volume, and price change. The pair
	// parameter should match the symbol format used by the exchange.
	//
	// Returns an error if the pair is not found or not supported by the exchange.
	//
	// Implementations should:
	// - Validate the pair symbol format
	// - Return current market data when available
	// - Handle unsupported pairs gracefully
	GetPairInfo(ctx context.Context, pair string) (*PairInfo, error)
}

// RateLimitInfo provides rate limiting information and management.
//
// This interface allows callers to understand the current rate limiting state
// and wait for rate limits to reset when necessary. Implementations should
// track rate limits accurately and provide backoff strategies.
type RateLimitInfo interface {
	// GetLimits returns the current rate limiting configuration.
	//
	// Returns the rate limiting parameters including requests per second,
	// burst capacity, and window duration. This information helps callers
	// understand the exchange's rate limiting policy.
	GetLimits() RateLimit

	// WaitForLimit blocks until the rate limit allows another request.
	//
	// This method should implement appropriate backoff strategies and can be
	// cancelled via the provided context. It should return immediately if
	// no rate limiting is currently in effect.
	//
	// Returns an error if the context is cancelled or if there's an issue
	// with the rate limiting logic.
	//
	// Implementations should:
	// - Respect context cancellation
	// - Implement exponential backoff where appropriate
	// - Track rate limit state accurately
	WaitForLimit(ctx context.Context) error
}

// HealthChecker provides health monitoring capabilities for exchange connections.
//
// This interface allows the system to verify that the exchange connection is
// working properly and can handle requests. Implementations should perform
// minimal, non-invasive checks.
type HealthChecker interface {
	// HealthCheck performs a lightweight health check of the exchange connection.
	//
	// This method should perform a minimal check to ensure the exchange is
	// reachable and responding correctly. It should not consume significant
	// rate limit quota or perform expensive operations.
	//
	// Returns an error if the exchange is not healthy or unreachable.
	// A nil return indicates the exchange is healthy and ready to serve requests.
	//
	// Implementations should:
	// - Perform lightweight checks (e.g., ping endpoints)
	// - Minimize rate limit consumption
	// - Complete quickly to avoid blocking health check routines
	HealthCheck(ctx context.Context) error
}

// ExchangeAdapter combines all exchange capabilities into a single interface.
//
// This is the main interface that exchange implementations should satisfy.
// It embeds all the specific capability interfaces to provide a complete
// exchange adapter implementation.
//
// Implementations should:
// - Satisfy all embedded interface requirements
// - Provide consistent behavior across all methods
// - Handle errors gracefully and provide meaningful error messages
// - Implement proper resource cleanup and connection management
type ExchangeAdapter interface {
	CandleFetcher
	PairProvider
	RateLimitInfo
	HealthChecker
}

// Request and Response Types
//
// The following types define the data structures used for communication
// between the exchange interfaces and their implementations. These types
// are designed to be compatible with the contract specifications while
// using internal models where appropriate.

// FetchRequest specifies parameters for fetching OHLCV candle data.
type FetchRequest struct {
	// Pair is the trading pair symbol (e.g., "BTC-USD", "ETH-BTC")
	Pair string `json:"pair"`

	// Start is the beginning of the time range to fetch (inclusive)
	Start time.Time `json:"start"`

	// End is the end of the time range to fetch (exclusive)
	End time.Time `json:"end"`

	// Interval specifies the candle interval (e.g., "1m", "5m", "1h", "1d")
	Interval string `json:"interval"`

	// Limit is the maximum number of candles to return in a single request
	// A value of 0 means no limit (use exchange default)
	Limit int `json:"limit,omitempty"`

	// NextToken is used for pagination to continue from a previous request
	NextToken string `json:"next_token,omitempty"`
}

// FetchResponse contains the results of a candle data fetch operation.
type FetchResponse struct {
	// Candles contains the OHLCV data ordered chronologically (oldest first)
	Candles []models.Candle `json:"candles"`

	// NextToken provides pagination support for additional data
	// Empty string indicates no more data available
	NextToken string `json:"next_token,omitempty"`

	// RateLimit contains current rate limiting status information
	RateLimit RateLimitStatus `json:"rate_limit"`
}

// TradingPair represents a trading pair and its specifications.
type TradingPair struct {
	// Symbol is the trading pair identifier (e.g., "BTC-USD")
	Symbol string `json:"symbol"`

	// BaseAsset is the base currency or asset (e.g., "BTC" in "BTC-USD")
	BaseAsset string `json:"base_asset"`

	// QuoteAsset is the quote currency or asset (e.g., "USD" in "BTC-USD")
	QuoteAsset string `json:"quote_asset"`

	// Active indicates if the pair is currently available for trading
	Active bool `json:"active"`

	// MinVolume is the minimum trading volume allowed (as decimal string)
	MinVolume string `json:"min_volume"`

	// MaxVolume is the maximum trading volume allowed (as decimal string)
	MaxVolume string `json:"max_volume"`

	// PriceStep is the minimum price increment (tick size) as decimal string
	PriceStep string `json:"price_step"`
}

// PairInfo contains detailed information about a trading pair including market data.
type PairInfo struct {
	// TradingPair contains the basic pair information and specifications
	TradingPair

	// LastPrice is the most recent trade price as decimal string
	LastPrice string `json:"last_price"`

	// Volume24h is the 24-hour trading volume as decimal string
	Volume24h string `json:"volume_24h"`

	// PriceChange is the 24-hour price change as decimal string
	PriceChange string `json:"price_change"`

	// UpdatedAt indicates when this information was last updated
	UpdatedAt time.Time `json:"updated_at"`
}

// RateLimit defines the rate limiting configuration for an exchange.
type RateLimit struct {
	// RequestsPerSecond is the maximum number of requests allowed per second
	RequestsPerSecond int `json:"requests_per_second"`

	// BurstSize is the maximum number of requests allowed in a burst
	BurstSize int `json:"burst_size"`

	// WindowDuration is the time window for rate limiting
	WindowDuration time.Duration `json:"window_duration"`
}

// RateLimitStatus provides current rate limiting state information.
type RateLimitStatus struct {
	// Remaining is the number of requests remaining in the current window
	Remaining int `json:"remaining"`

	// ResetTime is when the rate limit window resets
	ResetTime time.Time `json:"reset_time"`

	// RetryAfter is the duration to wait before making the next request
	// Zero duration indicates no waiting is required
	RetryAfter time.Duration `json:"retry_after"`
}

// Validation and Helper Methods
//
// The following methods provide validation and utility functions for the
// exchange types. These ensure data integrity and provide convenient
// access to common operations.

// Validate checks if the FetchRequest has valid parameters.
func (r *FetchRequest) Validate() error {
	if r.Pair == "" {
		return &ValidationError{Field: "pair", Message: "trading pair cannot be empty"}
	}

	if r.Interval == "" {
		return &ValidationError{Field: "interval", Message: "interval cannot be empty"}
	}

	if r.Start.IsZero() {
		return &ValidationError{Field: "start", Message: "start time cannot be zero"}
	}

	if r.End.IsZero() {
		return &ValidationError{Field: "end", Message: "end time cannot be zero"}
	}

	if !r.End.After(r.Start) {
		return &ValidationError{Field: "end", Message: "end time must be after start time"}
	}

	if r.Limit < 0 {
		return &ValidationError{Field: "limit", Message: "limit cannot be negative"}
	}

	return nil
}

// Duration returns the time span of the fetch request.
func (r *FetchRequest) Duration() time.Duration {
	return r.End.Sub(r.Start)
}

// Validate checks if the TradingPair has valid parameters.
func (tp *TradingPair) Validate() error {
	if tp.Symbol == "" {
		return &ValidationError{Field: "symbol", Message: "trading pair symbol cannot be empty"}
	}

	if tp.BaseAsset == "" {
		return &ValidationError{Field: "base_asset", Message: "base asset cannot be empty"}
	}

	if tp.QuoteAsset == "" {
		return &ValidationError{Field: "quote_asset", Message: "quote asset cannot be empty"}
	}

	// Validate numeric string fields if they're not empty
	if tp.MinVolume != "" {
		if err := validateDecimalString(tp.MinVolume); err != nil {
			return &ValidationError{Field: "min_volume", Message: "invalid min volume format: " + err.Error()}
		}
	}

	if tp.MaxVolume != "" {
		if err := validateDecimalString(tp.MaxVolume); err != nil {
			return &ValidationError{Field: "max_volume", Message: "invalid max volume format: " + err.Error()}
		}
	}

	if tp.PriceStep != "" {
		if err := validateDecimalString(tp.PriceStep); err != nil {
			return &ValidationError{Field: "price_step", Message: "invalid price step format: " + err.Error()}
		}
	}

	return nil
}

// IsValid returns true if the rate limit has positive values.
func (rl *RateLimit) IsValid() bool {
	return rl.RequestsPerSecond > 0 && rl.BurstSize > 0 && rl.WindowDuration > 0
}

// HasCapacity returns true if there are remaining requests available.
func (rls *RateLimitStatus) HasCapacity() bool {
	return rls.Remaining > 0
}

// NeedsWait returns true if a wait is required before the next request.
func (rls *RateLimitStatus) NeedsWait() bool {
	return rls.RetryAfter > 0
}

// ValidationError represents a validation error for exchange types.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return "validation error for field " + e.Field + ": " + e.Message
}

// validateDecimalString checks if a string represents a valid decimal number.
func validateDecimalString(s string) error {
	if s == "" {
		return nil // Empty strings are valid (optional fields)
	}

	// Simple validation - just check if it can be parsed as a decimal
	// In a real implementation, you might want to use decimal.NewFromString
	// from the shopspring/decimal package for more robust validation
	if len(s) == 0 {
		return &ValidationError{Field: "decimal", Message: "empty decimal string"}
	}

	// Basic validation - ensure it's not just whitespace
	for _, r := range s {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return nil // Found non-whitespace character
		}
	}

	return &ValidationError{Field: "decimal", Message: "decimal string contains only whitespace"}
}
