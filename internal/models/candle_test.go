package models

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data constants
const (
	testPair     = "BTC-USD"
	testInterval = "1h"
)

var (
	testTime = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
)

func TestNewCandle_ValidData(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		high     string
		low      string
		close    string
		volume   string
		expected bool
	}{
		{
			name:     "valid_bullish_candle",
			open:     "100.00",
			high:     "105.50",
			low:      "99.25",
			close:    "104.00",
			volume:   "1500.75",
			expected: true,
		},
		{
			name:     "valid_bearish_candle",
			open:     "100.00",
			high:     "102.00",
			low:      "95.50",
			close:    "96.75",
			volume:   "2000.00",
			expected: true,
		},
		{
			name:     "valid_doji_candle",
			open:     "100.00",
			high:     "101.00",
			low:      "99.00",
			close:    "100.00",
			volume:   "500.25",
			expected: true,
		},
		{
			name:     "valid_zero_volume",
			open:     "100.00",
			high:     "100.50",
			low:      "99.50",
			close:    "100.25",
			volume:   "0",
			expected: true,
		},
		{
			name:     "valid_high_precision",
			open:     "100.123456789",
			high:     "100.987654321",
			low:      "99.111111111",
			close:    "100.555555555",
			volume:   "1234.567890123",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle, err := NewCandle(testTime, tt.open, tt.high, tt.low, tt.close, tt.volume, testPair, testInterval)

			if tt.expected {
				assert.NoError(t, err)
				assert.NotNil(t, candle)
				assert.Equal(t, testTime, candle.Timestamp)
				assert.Equal(t, tt.open, candle.Open)
				assert.Equal(t, tt.high, candle.High)
				assert.Equal(t, tt.low, candle.Low)
				assert.Equal(t, tt.close, candle.Close)
				assert.Equal(t, tt.volume, candle.Volume)
				assert.Equal(t, testPair, candle.Pair)
				assert.Equal(t, testInterval, candle.Interval)
			} else {
				assert.Error(t, err)
				assert.Nil(t, candle)
			}
		})
	}
}

func TestCandle_Validate_TimestampValidation(t *testing.T) {
	tests := []struct {
		name        string
		timestamp   time.Time
		expectError bool
		errorField  string
	}{
		{
			name:        "valid_timestamp",
			timestamp:   testTime,
			expectError: false,
		},
		{
			name:        "zero_timestamp",
			timestamp:   time.Time{},
			expectError: true,
			errorField:  "timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: tt.timestamp,
				Open:      "100.00",
				High:      "105.00",
				Low:       "95.00",
				Close:     "102.00",
				Volume:    "1000.00",
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			if tt.expectError {
				assert.Error(t, err)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
				assert.Equal(t, tt.errorField, validationErr.Field)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCandle_Validate_PriceFormatValidation(t *testing.T) {
	tests := []struct {
		name        string
		open        string
		high        string
		low         string
		close       string
		volume      string
		expectError bool
		errorField  string
	}{
		{
			name:        "invalid_open_format",
			open:        "invalid",
			high:        "105.00",
			low:         "95.00",
			close:       "102.00",
			volume:      "1000.00",
			expectError: true,
			errorField:  "open",
		},
		{
			name:        "invalid_high_format",
			open:        "100.00",
			high:        "not_a_number",
			low:         "95.00",
			close:       "102.00",
			volume:      "1000.00",
			expectError: true,
			errorField:  "high",
		},
		{
			name:        "invalid_low_format",
			open:        "100.00",
			high:        "105.00",
			low:         "abc.def",
			close:       "102.00",
			volume:      "1000.00",
			expectError: true,
			errorField:  "low",
		},
		{
			name:        "invalid_close_format",
			open:        "100.00",
			high:        "105.00",
			low:         "95.00",
			close:       "12.34.56",
			volume:      "1000.00",
			expectError: true,
			errorField:  "close",
		},
		{
			name:        "invalid_volume_format",
			open:        "100.00",
			high:        "105.00",
			low:         "95.00",
			close:       "102.00",
			volume:      "volume_text",
			expectError: true,
			errorField:  "volume",
		},
		{
			name:        "empty_open",
			open:        "",
			high:        "105.00",
			low:         "95.00",
			close:       "102.00",
			volume:      "1000.00",
			expectError: true,
			errorField:  "open",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      tt.open,
				High:      tt.high,
				Low:       tt.low,
				Close:     tt.close,
				Volume:    tt.volume,
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			assert.Error(t, err)
			var validationErr *ValidationError
			assert.ErrorAs(t, err, &validationErr)
			assert.Equal(t, tt.errorField, validationErr.Field)
			assert.Contains(t, validationErr.Message, "format")
		})
	}
}

func TestCandle_Validate_PositivePriceValidation(t *testing.T) {
	tests := []struct {
		name        string
		open        string
		high        string
		low         string
		close       string
		expectError bool
		errorField  string
	}{
		{
			name:        "zero_open_price",
			open:        "0",
			high:        "105.00",
			low:         "95.00",
			close:       "102.00",
			expectError: true,
			errorField:  "open",
		},
		{
			name:        "negative_open_price",
			open:        "-10.00",
			high:        "105.00",
			low:         "95.00",
			close:       "102.00",
			expectError: true,
			errorField:  "open",
		},
		{
			name:        "zero_high_price",
			open:        "100.00",
			high:        "0",
			low:         "95.00",
			close:       "102.00",
			expectError: true,
			errorField:  "high",
		},
		{
			name:        "negative_high_price",
			open:        "100.00",
			high:        "-5.00",
			low:         "95.00",
			close:       "102.00",
			expectError: true,
			errorField:  "high",
		},
		{
			name:        "zero_low_price",
			open:        "100.00",
			high:        "105.00",
			low:         "0",
			close:       "102.00",
			expectError: true,
			errorField:  "low",
		},
		{
			name:        "negative_low_price",
			open:        "100.00",
			high:        "105.00",
			low:         "-1.00",
			close:       "102.00",
			expectError: true,
			errorField:  "low",
		},
		{
			name:        "zero_close_price",
			open:        "100.00",
			high:        "105.00",
			low:         "95.00",
			close:       "0",
			expectError: true,
			errorField:  "close",
		},
		{
			name:        "negative_close_price",
			open:        "100.00",
			high:        "105.00",
			low:         "95.00",
			close:       "-50.00",
			expectError: true,
			errorField:  "close",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      tt.open,
				High:      tt.high,
				Low:       tt.low,
				Close:     tt.close,
				Volume:    "1000.00",
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			assert.Error(t, err)
			var validationErr *ValidationError
			assert.ErrorAs(t, err, &validationErr)
			assert.Equal(t, tt.errorField, validationErr.Field)
			assert.Contains(t, validationErr.Message, "must be greater than 0")
		})
	}
}

func TestCandle_Validate_VolumeValidation(t *testing.T) {
	tests := []struct {
		name        string
		volume      string
		expectError bool
	}{
		{
			name:        "valid_positive_volume",
			volume:      "1000.00",
			expectError: false,
		},
		{
			name:        "valid_zero_volume",
			volume:      "0",
			expectError: false,
		},
		{
			name:        "negative_volume",
			volume:      "-100.00",
			expectError: true,
		},
		{
			name:        "negative_small_volume",
			volume:      "-0.001",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      "100.00",
				High:      "105.00",
				Low:       "95.00",
				Close:     "102.00",
				Volume:    tt.volume,
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			if tt.expectError {
				assert.Error(t, err)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
				assert.Equal(t, "volume", validationErr.Field)
				assert.Contains(t, validationErr.Message, "greater than or equal to 0")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCandle_Validate_OHLCRelationships(t *testing.T) {
	tests := []struct {
		name        string
		open        string
		high        string
		low         string
		close       string
		expectError bool
		errorField  string
		description string
	}{
		{
			name:        "high_less_than_open_bullish",
			open:        "100.00",
			high:        "99.00", // Invalid: high < open
			low:         "95.00",
			close:       "102.00",
			expectError: true,
			errorField:  "high",
			description: "high must be >= max(open, close)",
		},
		{
			name:        "high_less_than_close_bearish",
			open:        "100.00",
			high:        "98.00", // Invalid: high < close
			low:         "95.00",
			close:       "99.00",
			expectError: true,
			errorField:  "high",
			description: "high must be >= max(open, close)",
		},
		{
			name:        "low_greater_than_open_bearish",
			open:        "100.00",
			high:        "105.00",
			low:         "101.00", // Invalid: low > open
			close:       "98.00",
			expectError: true,
			errorField:  "low",
			description: "low must be <= min(open, close)",
		},
		{
			name:        "low_greater_than_close_bullish",
			open:        "100.00",
			high:        "105.00",
			low:         "103.00", // Invalid: low > close
			close:       "102.00",
			expectError: true,
			errorField:  "low",
			description: "low must be <= min(open, close)",
		},
		{
			name:        "high_equals_max_open_close_bullish",
			open:        "100.00",
			high:        "102.00", // Valid: high == close (max)
			low:         "99.00",
			close:       "102.00",
			expectError: false,
		},
		{
			name:        "high_equals_max_open_close_bearish",
			open:        "102.00",
			high:        "102.00", // Valid: high == open (max)
			low:         "99.00",
			close:       "100.00",
			expectError: false,
		},
		{
			name:        "low_equals_min_open_close_bullish",
			open:        "100.00",
			high:        "105.00",
			low:         "100.00", // Valid: low == open (min)
			close:       "102.00",
			expectError: false,
		},
		{
			name:        "low_equals_min_open_close_bearish",
			open:        "102.00",
			high:        "105.00",
			low:         "100.00", // Valid: low == close (min)
			close:       "100.00",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      tt.open,
				High:      tt.high,
				Low:       tt.low,
				Close:     tt.close,
				Volume:    "1000.00",
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			if tt.expectError {
				assert.Error(t, err)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
				assert.Equal(t, tt.errorField, validationErr.Field)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCandle_Validate_RequiredFieldValidation(t *testing.T) {
	tests := []struct {
		name        string
		pair        string
		interval    string
		expectError bool
		errorField  string
	}{
		{
			name:        "empty_pair",
			pair:        "",
			interval:    testInterval,
			expectError: true,
			errorField:  "pair",
		},
		{
			name:        "empty_interval",
			pair:        testPair,
			interval:    "",
			expectError: true,
			errorField:  "interval",
		},
		{
			name:        "valid_fields",
			pair:        testPair,
			interval:    testInterval,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      "100.00",
				High:      "105.00",
				Low:       "95.00",
				Close:     "102.00",
				Volume:    "1000.00",
				Pair:      tt.pair,
				Interval:  tt.interval,
			}

			err := candle.Validate()
			if tt.expectError {
				assert.Error(t, err)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
				assert.Equal(t, tt.errorField, validationErr.Field)
				assert.Contains(t, validationErr.Message, "cannot be empty")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCandle_GetDecimalMethods(t *testing.T) {
	candle := &Candle{
		Timestamp: testTime,
		Open:      "100.123",
		High:      "105.456",
		Low:       "95.789",
		Close:     "102.321",
		Volume:    "1500.654",
		Pair:      testPair,
		Interval:  testInterval,
	}

	t.Run("GetOpenDecimal", func(t *testing.T) {
		open, err := candle.GetOpenDecimal()
		assert.NoError(t, err)
		assert.True(t, open.Equal(decimal.RequireFromString("100.123")))
	})

	t.Run("GetHighDecimal", func(t *testing.T) {
		high, err := candle.GetHighDecimal()
		assert.NoError(t, err)
		assert.True(t, high.Equal(decimal.RequireFromString("105.456")))
	})

	t.Run("GetLowDecimal", func(t *testing.T) {
		low, err := candle.GetLowDecimal()
		assert.NoError(t, err)
		assert.True(t, low.Equal(decimal.RequireFromString("95.789")))
	})

	t.Run("GetCloseDecimal", func(t *testing.T) {
		close, err := candle.GetCloseDecimal()
		assert.NoError(t, err)
		assert.True(t, close.Equal(decimal.RequireFromString("102.321")))
	})

	t.Run("GetVolumeDecimal", func(t *testing.T) {
		volume, err := candle.GetVolumeDecimal()
		assert.NoError(t, err)
		assert.True(t, volume.Equal(decimal.RequireFromString("1500.654")))
	})
}

func TestCandle_GetDecimalMethods_InvalidFormat(t *testing.T) {
	candle := &Candle{
		Open:   "invalid",
		High:   "not_a_number",
		Low:    "abc.def",
		Close:  "12.34.56",
		Volume: "volume_text",
	}

	t.Run("GetOpenDecimal_Invalid", func(t *testing.T) {
		_, err := candle.GetOpenDecimal()
		assert.Error(t, err)
	})

	t.Run("GetHighDecimal_Invalid", func(t *testing.T) {
		_, err := candle.GetHighDecimal()
		assert.Error(t, err)
	})

	t.Run("GetLowDecimal_Invalid", func(t *testing.T) {
		_, err := candle.GetLowDecimal()
		assert.Error(t, err)
	})

	t.Run("GetCloseDecimal_Invalid", func(t *testing.T) {
		_, err := candle.GetCloseDecimal()
		assert.Error(t, err)
	})

	t.Run("GetVolumeDecimal_Invalid", func(t *testing.T) {
		_, err := candle.GetVolumeDecimal()
		assert.Error(t, err)
	})
}

func TestCandle_GetTypicalPrice(t *testing.T) {
	tests := []struct {
		name     string
		high     string
		low      string
		close    string
		expected string
	}{
		{
			name:     "typical_calculation",
			high:     "105.00",
			low:      "95.00",
			close:    "102.00",
			expected: "100.666666666666666667", // (105 + 95 + 102) / 3
		},
		{
			name:     "equal_prices",
			high:     "100.00",
			low:      "100.00",
			close:    "100.00",
			expected: "100",
		},
		{
			name:     "high_precision",
			high:     "105.123456789",
			low:      "95.987654321",
			close:    "102.555555555",
			expected: "101.222222221666666667", // (105.123456789 + 95.987654321 + 102.555555555) / 3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  "100.00",
				High:  tt.high,
				Low:   tt.low,
				Close: tt.close,
			}

			result, err := candle.GetTypicalPrice()
			assert.NoError(t, err)
			assert.True(t, result.Equal(decimal.RequireFromString(tt.expected)))
		})
	}
}

func TestCandle_GetTypicalPrice_InvalidFormat(t *testing.T) {
	candle := &Candle{
		High:  "invalid",
		Low:   "95.00",
		Close: "102.00",
	}

	_, err := candle.GetTypicalPrice()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse high price")
}

func TestCandle_GetBodySize(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		close    string
		expected string
	}{
		{
			name:     "bullish_candle",
			open:     "100.00",
			close:    "105.50",
			expected: "5.5",
		},
		{
			name:     "bearish_candle",
			open:     "105.50",
			close:    "100.00",
			expected: "5.5",
		},
		{
			name:     "doji_candle",
			open:     "100.00",
			close:    "100.00",
			expected: "0",
		},
		{
			name:     "small_body",
			open:     "100.123",
			close:    "100.456",
			expected: "0.333",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}

			result, err := candle.GetBodySize()
			assert.NoError(t, err)
			assert.True(t, result.Equal(decimal.RequireFromString(tt.expected)))
		})
	}
}

func TestCandle_GetRange(t *testing.T) {
	tests := []struct {
		name     string
		high     string
		low      string
		expected string
	}{
		{
			name:     "normal_range",
			high:     "105.50",
			low:      "95.25",
			expected: "10.25",
		},
		{
			name:     "zero_range",
			high:     "100.00",
			low:      "100.00",
			expected: "0",
		},
		{
			name:     "high_precision_range",
			high:     "100.123456789",
			low:      "99.987654321",
			expected: "0.135802468",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				High: tt.high,
				Low:  tt.low,
			}

			result, err := candle.GetRange()
			assert.NoError(t, err)
			assert.True(t, result.Equal(decimal.RequireFromString(tt.expected)))
		})
	}
}

func TestCandle_IsBullish(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		close    string
		expected bool
	}{
		{
			name:     "bullish_candle",
			open:     "100.00",
			close:    "105.00",
			expected: true,
		},
		{
			name:     "bearish_candle",
			open:     "105.00",
			close:    "100.00",
			expected: false,
		},
		{
			name:     "doji_candle",
			open:     "100.00",
			close:    "100.00",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}

			result, err := candle.IsBullish()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCandle_IsBearish(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		close    string
		expected bool
	}{
		{
			name:     "bearish_candle",
			open:     "105.00",
			close:    "100.00",
			expected: true,
		},
		{
			name:     "bullish_candle",
			open:     "100.00",
			close:    "105.00",
			expected: false,
		},
		{
			name:     "doji_candle",
			open:     "100.00",
			close:    "100.00",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}

			result, err := candle.IsBearish()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCandle_IsDoji(t *testing.T) {
	tests := []struct {
		name      string
		open      string
		close     string
		threshold string
		expected  bool
	}{
		{
			name:      "perfect_doji",
			open:      "100.00",
			close:     "100.00",
			threshold: "0.01",
			expected:  true,
		},
		{
			name:      "small_doji_within_threshold",
			open:      "100.00",
			close:     "100.005",
			threshold: "0.01",
			expected:  true,
		},
		{
			name:      "not_doji_exceeds_threshold",
			open:      "100.00",
			close:     "100.02",
			threshold: "0.01",
			expected:  false,
		},
		{
			name:      "large_difference",
			open:      "100.00",
			close:     "105.00",
			threshold: "0.01",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}
			threshold := decimal.RequireFromString(tt.threshold)

			result, err := candle.IsDoji(threshold)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCandle_GetPriceChange(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		close    string
		expected string
	}{
		{
			name:     "positive_change",
			open:     "100.00",
			close:    "105.50",
			expected: "5.5",
		},
		{
			name:     "negative_change",
			open:     "105.50",
			close:    "100.00",
			expected: "-5.5",
		},
		{
			name:     "zero_change",
			open:     "100.00",
			close:    "100.00",
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}

			result, err := candle.GetPriceChange()
			assert.NoError(t, err)
			assert.True(t, result.Equal(decimal.RequireFromString(tt.expected)))
		})
	}
}

func TestCandle_GetPriceChangePercent(t *testing.T) {
	tests := []struct {
		name     string
		open     string
		close    string
		expected string
	}{
		{
			name:     "positive_5_percent",
			open:     "100.00",
			close:    "105.00",
			expected: "5",
		},
		{
			name:     "negative_10_percent",
			open:     "100.00",
			close:    "90.00",
			expected: "-10",
		},
		{
			name:     "zero_change",
			open:     "100.00",
			close:    "100.00",
			expected: "0",
		},
		{
			name:     "small_positive_change",
			open:     "100.00",
			close:    "100.50",
			expected: "0.5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Open:  tt.open,
				Close: tt.close,
			}

			result, err := candle.GetPriceChangePercent()
			assert.NoError(t, err)
			assert.True(t, result.Equal(decimal.RequireFromString(tt.expected)))
		})
	}
}

func TestCandle_GetPriceChangePercent_ZeroOpen(t *testing.T) {
	candle := &Candle{
		Open:  "0",
		Close: "100.00",
	}

	_, err := candle.GetPriceChangePercent()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot calculate percentage change with zero open price")
}

func TestCandle_String(t *testing.T) {
	candle := &Candle{
		Timestamp: testTime,
		Open:      "100.00",
		High:      "105.50",
		Low:       "95.25",
		Close:     "102.75",
		Volume:    "1500.50",
		Pair:      testPair,
		Interval:  testInterval,
	}

	result := candle.String()
	expectedFormat := "Candle{Pair: BTC-USD, Interval: 1h, Timestamp: 2024-01-01T12:00:00Z, O: 100.00, H: 105.50, L: 95.25, C: 102.75, V: 1500.50}"
	assert.Equal(t, expectedFormat, result)
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Field:   "test_field",
		Message: "test message",
	}

	result := err.Error()
	expected := "validation error for field test_field: test message"
	assert.Equal(t, expected, result)
}

func TestCandle_Validate_EdgeCases(t *testing.T) {
	t.Run("very_small_positive_prices", func(t *testing.T) {
		candle := &Candle{
			Timestamp: testTime,
			Open:      "0.000001",
			High:      "0.000002",
			Low:       "0.000001",
			Close:     "0.000001",
			Volume:    "1000000.0",
			Pair:      testPair,
			Interval:  testInterval,
		}

		err := candle.Validate()
		assert.NoError(t, err)
	})

	t.Run("very_large_prices", func(t *testing.T) {
		candle := &Candle{
			Timestamp: testTime,
			Open:      "999999999.999999999",
			High:      "1000000000.000000000",
			Low:       "999999999.999999999",
			Close:     "1000000000.000000000",
			Volume:    "0.000000001",
			Pair:      testPair,
			Interval:  testInterval,
		}

		err := candle.Validate()
		assert.NoError(t, err)
	})

	t.Run("maximum_precision", func(t *testing.T) {
		candle := &Candle{
			Timestamp: testTime,
			Open:      "100.123456789012345678",
			High:      "100.987654321098765432",
			Low:       "100.111111111111111111",
			Close:     "100.555555555555555555",
			Volume:    "1234.567890123456789012",
			Pair:      testPair,
			Interval:  testInterval,
		}

		err := candle.Validate()
		assert.NoError(t, err)
	})

	t.Run("boundary_ohlc_relationships", func(t *testing.T) {
		// Test case where all OHLC values are equal (perfect doji with no range)
		candle := &Candle{
			Timestamp: testTime,
			Open:      "100.00",
			High:      "100.00",
			Low:       "100.00",
			Close:     "100.00",
			Volume:    "500.0",
			Pair:      testPair,
			Interval:  testInterval,
		}

		err := candle.Validate()
		assert.NoError(t, err)
	})
}

func TestCandle_Validate_ComprehensiveInvalidScenarios(t *testing.T) {
	tests := []struct {
		name        string
		setupCandle func() *Candle
		expectField string
		expectMsg   string
	}{
		{
			name: "high_exactly_one_less_than_open",
			setupCandle: func() *Candle {
				return &Candle{
					Timestamp: testTime,
					Open:      "100.00",
					High:      "99.99", // Just below open
					Low:       "95.00",
					Close:     "98.00",
					Volume:    "1000.0",
					Pair:      testPair,
					Interval:  testInterval,
				}
			},
			expectField: "high",
			expectMsg:   "must be greater than or equal to max(open, close)",
		},
		{
			name: "low_exactly_one_more_than_close",
			setupCandle: func() *Candle {
				return &Candle{
					Timestamp: testTime,
					Open:      "100.00",
					High:      "105.00",
					Low:       "98.01", // Just above close
					Close:     "98.00",
					Volume:    "1000.0",
					Pair:      testPair,
					Interval:  testInterval,
				}
			},
			expectField: "low",
			expectMsg:   "must be less than or equal to min(open, close)",
		},
		{
			name: "scientific_notation_prices",
			setupCandle: func() *Candle {
				return &Candle{
					Timestamp: testTime,
					Open:      "1e2",    // Valid: 100
					High:      "1.05e2", // Valid: 105
					Low:       "9.5e1",  // Valid: 95
					Close:     "1.02e2", // Valid: 102
					Volume:    "1.5e3",  // Valid: 1500
					Pair:      testPair,
					Interval:  testInterval,
				}
			},
			expectField: "",
			expectMsg:   "", // Should be valid
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := tt.setupCandle()
			err := candle.Validate()

			if tt.expectField == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				var validationErr *ValidationError
				require.ErrorAs(t, err, &validationErr)
				assert.Equal(t, tt.expectField, validationErr.Field)
				assert.Contains(t, validationErr.Message, tt.expectMsg)
			}
		})
	}
}

func TestCandle_Validate_DecimalPrecisionHandling(t *testing.T) {
	tests := []struct {
		name       string
		open       string
		high       string
		low        string
		close      string
		shouldPass bool
	}{
		{
			name:       "trailing_zeros",
			open:       "100.000",
			high:       "105.000",
			low:        "95.000",
			close:      "102.000",
			shouldPass: true,
		},
		{
			name:       "leading_zeros",
			open:       "00100.00",
			high:       "00105.00",
			low:        "0095.00",
			close:      "00102.00",
			shouldPass: true,
		},
		{
			name:       "no_decimal_point",
			open:       "100",
			high:       "105",
			low:        "95",
			close:      "102",
			shouldPass: true,
		},
		{
			name:       "fractional_only",
			open:       "0.100",
			high:       "0.105",
			low:        "0.095",
			close:      "0.102",
			shouldPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candle := &Candle{
				Timestamp: testTime,
				Open:      tt.open,
				High:      tt.high,
				Low:       tt.low,
				Close:     tt.close,
				Volume:    "1000.0",
				Pair:      testPair,
				Interval:  testInterval,
			}

			err := candle.Validate()
			if tt.shouldPass {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// Benchmark tests
func BenchmarkCandle_Validate(b *testing.B) {
	candle := &Candle{
		Timestamp: testTime,
		Open:      "100.123456789",
		High:      "105.987654321",
		Low:       "95.111111111",
		Close:     "102.555555555",
		Volume:    "1500.777777777",
		Pair:      testPair,
		Interval:  testInterval,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = candle.Validate()
	}
}

func BenchmarkCandle_GetTypicalPrice(b *testing.B) {
	candle := &Candle{
		High:  "105.123456789",
		Low:   "95.987654321",
		Close: "102.555555555",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = candle.GetTypicalPrice()
	}
}

func BenchmarkNewCandle(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewCandle(testTime, "100.123", "105.456", "95.789", "102.321", "1500.654", testPair, testInterval)
	}
}
