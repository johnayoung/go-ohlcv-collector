// Package models provides data structures and validation for OHLCV market data.
// This package contains core data models for financial market data including
// candles, gaps, jobs, and validation structures.
package models

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Candle represents OHLCV price and volume data for a specific trading pair at a time interval
// This matches the contracts.Candle structure exactly
type Candle struct {
	Timestamp time.Time `json:"timestamp" db:"timestamp"`
	Open      string    `json:"open" db:"open"`
	High      string    `json:"high" db:"high"`
	Low       string    `json:"low" db:"low"`
	Close     string    `json:"close" db:"close"`
	Volume    string    `json:"volume" db:"volume"`
	Pair      string    `json:"pair" db:"pair"`
	Interval  string    `json:"interval" db:"interval"`
}

// ValidationError represents a candle validation error with specific field context.
// It provides structured error information including the field name that failed
// validation and a descriptive error message.
type ValidationError struct {
	Field   string // Field is the name of the field that failed validation
	Message string // Message is a descriptive error message explaining the validation failure
}

// Error implements the error interface for ValidationError.
// It returns a formatted string containing the field name and validation message.
func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error for field %s: %s", e.Field, e.Message)
}

// Validate performs comprehensive validation on the candle data.
// It validates that all price fields are valid decimal numbers greater than zero,
// volume is non-negative, OHLC relationships are correct (high >= max(open, close),
// low <= min(open, close)), and required fields are not empty.
// Returns a ValidationError if any validation fails.
func (c *Candle) Validate() error {
	// Check timestamp is not null
	if c.Timestamp.IsZero() {
		return &ValidationError{Field: "timestamp", Message: "timestamp cannot be null or zero"}
	}

	// Parse decimal values for validation
	open, err := decimal.NewFromString(c.Open)
	if err != nil {
		return &ValidationError{Field: "open", Message: fmt.Sprintf("invalid open price format: %v", err)}
	}

	high, err := decimal.NewFromString(c.High)
	if err != nil {
		return &ValidationError{Field: "high", Message: fmt.Sprintf("invalid high price format: %v", err)}
	}

	low, err := decimal.NewFromString(c.Low)
	if err != nil {
		return &ValidationError{Field: "low", Message: fmt.Sprintf("invalid low price format: %v", err)}
	}

	close, err := decimal.NewFromString(c.Close)
	if err != nil {
		return &ValidationError{Field: "close", Message: fmt.Sprintf("invalid close price format: %v", err)}
	}

	volume, err := decimal.NewFromString(c.Volume)
	if err != nil {
		return &ValidationError{Field: "volume", Message: fmt.Sprintf("invalid volume format: %v", err)}
	}

	// All prices must be > 0
	zero := decimal.Zero
	if open.LessThanOrEqual(zero) {
		return &ValidationError{Field: "open", Message: "open price must be greater than 0"}
	}
	if high.LessThanOrEqual(zero) {
		return &ValidationError{Field: "high", Message: "high price must be greater than 0"}
	}
	if low.LessThanOrEqual(zero) {
		return &ValidationError{Field: "low", Message: "low price must be greater than 0"}
	}
	if close.LessThanOrEqual(zero) {
		return &ValidationError{Field: "close", Message: "close price must be greater than 0"}
	}

	// Volume must be >= 0
	if volume.LessThan(zero) {
		return &ValidationError{Field: "volume", Message: "volume must be greater than or equal to 0"}
	}

	// High >= max(Open, Close)
	maxOpenClose := decimal.Max(open, close)
	if high.LessThan(maxOpenClose) {
		return &ValidationError{
			Field:   "high",
			Message: fmt.Sprintf("high price (%s) must be greater than or equal to max(open, close) (%s)", high, maxOpenClose),
		}
	}

	// Low <= min(Open, Close)
	minOpenClose := decimal.Min(open, close)
	if low.GreaterThan(minOpenClose) {
		return &ValidationError{
			Field:   "low",
			Message: fmt.Sprintf("low price (%s) must be less than or equal to min(open, close) (%s)", low, minOpenClose),
		}
	}

	// Validate required string fields
	if c.Pair == "" {
		return &ValidationError{Field: "pair", Message: "pair cannot be empty"}
	}
	if c.Interval == "" {
		return &ValidationError{Field: "interval", Message: "interval cannot be empty"}
	}

	return nil
}

// GetOpenDecimal returns the open price as a decimal.Decimal for precise calculations.
// Returns an error if the open price string cannot be parsed as a decimal.
func (c *Candle) GetOpenDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(c.Open)
}

// GetHighDecimal returns the high price as a decimal.Decimal for precise calculations.
// Returns an error if the high price string cannot be parsed as a decimal.
func (c *Candle) GetHighDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(c.High)
}

// GetLowDecimal returns the low price as a decimal.Decimal for precise calculations.
// Returns an error if the low price string cannot be parsed as a decimal.
func (c *Candle) GetLowDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(c.Low)
}

// GetCloseDecimal returns the close price as a decimal.Decimal for precise calculations.
// Returns an error if the close price string cannot be parsed as a decimal.
func (c *Candle) GetCloseDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(c.Close)
}

// GetVolumeDecimal returns the volume as a decimal.Decimal for precise calculations.
// Returns an error if the volume string cannot be parsed as a decimal.
func (c *Candle) GetVolumeDecimal() (decimal.Decimal, error) {
	return decimal.NewFromString(c.Volume)
}

// GetTypicalPrice calculates the typical price using the formula: (High + Low + Close) / 3.
// This is commonly used in technical analysis as a representative price for the period.
// Returns an error if any of the price fields cannot be parsed as decimals.
func (c *Candle) GetTypicalPrice() (decimal.Decimal, error) {
	high, err := c.GetHighDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse high price: %w", err)
	}

	low, err := c.GetLowDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse low price: %w", err)
	}

	close, err := c.GetCloseDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse close price: %w", err)
	}

	sum := high.Add(low).Add(close)
	three := decimal.NewFromInt(3)
	return sum.Div(three), nil
}

// GetBodySize calculates the absolute difference between open and close prices.
// This represents the size of the candle body and is useful for technical analysis.
// Returns an error if open or close prices cannot be parsed as decimals.
func (c *Candle) GetBodySize() (decimal.Decimal, error) {
	open, err := c.GetOpenDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse open price: %w", err)
	}

	close, err := c.GetCloseDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse close price: %w", err)
	}

	return close.Sub(open).Abs(), nil
}

// GetRange calculates the price range using the formula: High - Low.
// This represents the total price movement during the time period.
// Returns an error if high or low prices cannot be parsed as decimals.
func (c *Candle) GetRange() (decimal.Decimal, error) {
	high, err := c.GetHighDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse high price: %w", err)
	}

	low, err := c.GetLowDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse low price: %w", err)
	}

	return high.Sub(low), nil
}

// IsBullish returns true if the close price is greater than the open price.
// This indicates positive price movement during the time period.
// Returns an error if open or close prices cannot be parsed as decimals.
func (c *Candle) IsBullish() (bool, error) {
	open, err := c.GetOpenDecimal()
	if err != nil {
		return false, fmt.Errorf("failed to parse open price: %w", err)
	}

	close, err := c.GetCloseDecimal()
	if err != nil {
		return false, fmt.Errorf("failed to parse close price: %w", err)
	}

	return close.GreaterThan(open), nil
}

// IsBearish returns true if the close price is less than the open price.
// This indicates negative price movement during the time period.
// Returns an error if open or close prices cannot be parsed as decimals.
func (c *Candle) IsBearish() (bool, error) {
	open, err := c.GetOpenDecimal()
	if err != nil {
		return false, fmt.Errorf("failed to parse open price: %w", err)
	}

	close, err := c.GetCloseDecimal()
	if err != nil {
		return false, fmt.Errorf("failed to parse close price: %w", err)
	}

	return close.LessThan(open), nil
}

// IsDoji returns true if the open price approximately equals the close price within the given threshold.
// A doji candle indicates indecision in the market with little net price movement.
// The threshold parameter defines the maximum acceptable difference between open and close.
// Returns an error if the body size cannot be calculated.
func (c *Candle) IsDoji(threshold decimal.Decimal) (bool, error) {
	bodySize, err := c.GetBodySize()
	if err != nil {
		return false, fmt.Errorf("failed to calculate body size: %w", err)
	}

	return bodySize.LessThanOrEqual(threshold), nil
}

// GetPriceChange calculates the absolute price change using the formula: Close - Open.
// Positive values indicate price appreciation, negative values indicate depreciation.
// Returns an error if open or close prices cannot be parsed as decimals.
func (c *Candle) GetPriceChange() (decimal.Decimal, error) {
	open, err := c.GetOpenDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse open price: %w", err)
	}

	close, err := c.GetCloseDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse close price: %w", err)
	}

	return close.Sub(open), nil
}

// GetPriceChangePercent calculates the percentage price change using the formula: ((Close - Open) / Open) * 100.
// This normalizes price changes for comparison across different price levels.
// Returns an error if open or close prices cannot be parsed, or if open price is zero.
func (c *Candle) GetPriceChangePercent() (decimal.Decimal, error) {
	open, err := c.GetOpenDecimal()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse open price: %w", err)
	}

	if open.IsZero() {
		return decimal.Zero, fmt.Errorf("cannot calculate percentage change with zero open price")
	}

	priceChange, err := c.GetPriceChange()
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to calculate price change: %w", err)
	}

	hundred := decimal.NewFromInt(100)
	return priceChange.Div(open).Mul(hundred), nil
}

// String returns a human-readable string representation of the candle.
// The format includes all key fields: pair, interval, timestamp, and OHLCV data.
// This method implements the fmt.Stringer interface.
func (c *Candle) String() string {
	return fmt.Sprintf("Candle{Pair: %s, Interval: %s, Timestamp: %s, O: %s, H: %s, L: %s, C: %s, V: %s}",
		c.Pair, c.Interval, c.Timestamp.Format(time.RFC3339), c.Open, c.High, c.Low, c.Close, c.Volume)
}

// NewCandle creates a new Candle instance with the provided parameters and validates it.
// All price and volume values should be provided as decimal strings.
// The timestamp should represent the start time of the candle period.
// Returns a ValidationError if any validation fails.
//
// Example:
//     candle, err := NewCandle(
//         time.Now(),
//         "100.50", "101.00", "100.00", "100.75", "1000.5",
//         "BTC-USD", "1m",
//     )
func NewCandle(timestamp time.Time, open, high, low, close, volume, pair, interval string) (*Candle, error) {
	candle := &Candle{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
		Pair:      pair,
		Interval:  interval,
	}

	if err := candle.Validate(); err != nil {
		return nil, fmt.Errorf("failed to create candle: %w", err)
	}

	return candle, nil
}