package gaps

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
)

// Mock implementations for testing
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) Query(ctx context.Context, req storage.QueryRequest) (*storage.QueryResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.QueryResponse), args.Error(1)
}

func (m *MockStorage) StoreGap(ctx context.Context, gap models.Gap) error {
	args := m.Called(ctx, gap)
	return args.Error(0)
}

func (m *MockStorage) GetGaps(ctx context.Context, pair, interval string) ([]models.Gap, error) {
	args := m.Called(ctx, pair, interval)
	return args.Get(0).([]models.Gap), args.Error(1)
}

func (m *MockStorage) GetGapByID(ctx context.Context, gapID string) (*models.Gap, error) {
	args := m.Called(ctx, gapID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Gap), args.Error(1)
}

func (m *MockStorage) MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error {
	args := m.Called(ctx, gapID, filledAt)
	return args.Error(0)
}

func (m *MockStorage) DeleteGap(ctx context.Context, gapID string) error {
	args := m.Called(ctx, gapID)
	return args.Error(0)
}

// Mock remaining storage methods to satisfy interface
func (m *MockStorage) Store(ctx context.Context, candles []models.Candle) error {
	args := m.Called(ctx, candles)
	return args.Error(0)
}

func (m *MockStorage) StoreBatch(ctx context.Context, candles []models.Candle) error {
	args := m.Called(ctx, candles)
	return args.Error(0)
}

func (m *MockStorage) GetLatest(ctx context.Context, pair, interval string) (*models.Candle, error) {
	args := m.Called(ctx, pair, interval)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Candle), args.Error(1)
}

func (m *MockStorage) Initialize(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockStorage) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStorage) Migrate(ctx context.Context, version int) error {
	args := m.Called(ctx, version)
	return args.Error(0)
}

func (m *MockStorage) GetStats(ctx context.Context) (*storage.StorageStats, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.StorageStats), args.Error(1)
}

func (m *MockStorage) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockGapValidator struct {
	mock.Mock
}

func (m *MockGapValidator) IsValidGapPeriod(ctx context.Context, pair string, start, end time.Time, interval string) (bool, string, error) {
	args := m.Called(ctx, pair, start, end, interval)
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *MockGapValidator) ShouldIgnoreGap(ctx context.Context, gap *models.Gap) (bool, string, error) {
	args := m.Called(ctx, gap)
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *MockGapValidator) ValidateGapData(ctx context.Context, gap *models.Gap, candles []contracts.Candle) error {
	args := m.Called(ctx, gap, candles)
	return args.Error(0)
}

type MockExchangeAdapter struct {
	mock.Mock
}

func (m *MockExchangeAdapter) FetchCandles(ctx context.Context, req contracts.FetchRequest) (*contracts.FetchResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.FetchResponse), args.Error(1)
}

func (m *MockExchangeAdapter) GetTradingPairs(ctx context.Context) ([]contracts.TradingPair, error) {
	args := m.Called(ctx)
	return args.Get(0).([]contracts.TradingPair), args.Error(1)
}

func (m *MockExchangeAdapter) GetPairInfo(ctx context.Context, pair string) (*contracts.PairInfo, error) {
	args := m.Called(ctx, pair)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*contracts.PairInfo), args.Error(1)
}

func (m *MockExchangeAdapter) GetLimits() contracts.RateLimit {
	args := m.Called()
	return args.Get(0).(contracts.RateLimit)
}

func (m *MockExchangeAdapter) WaitForLimit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockExchangeAdapter) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Helper functions for creating test data
func createTestCandle(timestamp time.Time, pair, interval string) models.Candle {
	return models.Candle{
		Timestamp: timestamp,
		Open:      "100.00",
		High:      "105.00",
		Low:       "98.00",
		Close:     "102.00",
		Volume:    "1000.00",
		Pair:      pair,
		Interval:  interval,
	}
}

func createContractCandle(timestamp time.Time, pair, interval string) contracts.Candle {
	return contracts.Candle{
		Timestamp: timestamp,
		Open:      "100.00",
		High:      "105.00",
		Low:       "98.00",
		Close:     "102.00",
		Volume:    "1000.00",
		Pair:      pair,
		Interval:  interval,
	}
}

func createContinuousCandles(start time.Time, count int, interval string, pair string) []models.Candle {
	duration, _ := parseIntervalDuration(interval)
	candles := make([]models.Candle, count)

	for i := 0; i < count; i++ {
		timestamp := start.Add(time.Duration(i) * duration)
		candles[i] = createTestCandle(timestamp, pair, interval)
	}

	return candles
}

func createContinuousContractCandles(start time.Time, count int, interval string, pair string) []contracts.Candle {
	duration, _ := parseIntervalDuration(interval)
	candles := make([]contracts.Candle, count)

	for i := 0; i < count; i++ {
		timestamp := start.Add(time.Duration(i) * duration)
		candles[i] = createContractCandle(timestamp, pair, interval)
	}

	return candles
}

// TestGapDetectorImpl_DetectGaps tests the main gap detection functionality
func TestGapDetectorImpl_DetectGaps(t *testing.T) {
	tests := []struct {
		name              string
		pair              string
		interval          string
		startTime         time.Time
		endTime           time.Time
		existingCandles   []models.Candle
		validatorResponse struct {
			isValid bool
			reason  string
			err     error
		}
		expectedGaps  int
		expectedError bool
	}{
		{
			name:      "continuous data - no gaps",
			pair:      "BTC-USD",
			interval:  "1h",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC),
			existingCandles: createContinuousCandles(
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 5, "1h", "BTC-USD",
			),
			validatorResponse: struct {
				isValid bool
				reason  string
				err     error
			}{true, "", nil},
			expectedGaps:  0,
			expectedError: false,
		},
		{
			name:      "single gap in middle",
			pair:      "BTC-USD",
			interval:  "1h",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 5, 0, 0, 0, time.UTC),
			existingCandles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Missing: 2:00
				createTestCandle(time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			validatorResponse: struct {
				isValid bool
				reason  string
				err     error
			}{true, "", nil},
			expectedGaps:  1,
			expectedError: false,
		},
		{
			name:      "multiple gaps",
			pair:      "ETH-USD",
			interval:  "5m",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			existingCandles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "ETH-USD", "5m"),
				createTestCandle(time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC), "ETH-USD", "5m"),
				// Gap: 0:10 - 0:15
				createTestCandle(time.Date(2024, 1, 1, 0, 20, 0, 0, time.UTC), "ETH-USD", "5m"),
				// Gap: 0:25
			},
			validatorResponse: struct {
				isValid bool
				reason  string
				err     error
			}{true, "", nil},
			expectedGaps:  2,
			expectedError: false,
		},
		{
			name:            "no existing data",
			pair:            "BTC-USD",
			interval:        "1h",
			startTime:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:         time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
			existingCandles: []models.Candle{},
			validatorResponse: struct {
				isValid bool
				reason  string
				err     error
			}{true, "", nil},
			expectedGaps:  1, // One large gap covering the entire period
			expectedError: false,
		},
		{
			name:          "invalid interval",
			pair:          "BTC-USD",
			interval:      "invalid",
			startTime:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:       time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			expectedGaps:  0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			mockStorage := new(MockStorage)
			mockValidator := new(MockGapValidator)

			if !tt.expectedError {
				// Setup storage query expectation
				queryResponse := &storage.QueryResponse{
					Candles: tt.existingCandles,
					Total:   len(tt.existingCandles),
				}
				mockStorage.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)

				if tt.expectedGaps > 0 {
					// Setup validator expectations
					mockValidator.On("IsValidGapPeriod", mock.Anything, tt.pair, mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), tt.interval).Return(
						tt.validatorResponse.isValid, tt.validatorResponse.reason, tt.validatorResponse.err,
					)

					// Setup storage gap expectations
					mockStorage.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil).Times(tt.expectedGaps)
				}
			}

			// Create detector
			detector := &GapDetectorImpl{
				storage:   mockStorage,
				validator: mockValidator,
				logger:    slog.Default(),
			}

			// Execute test
			gaps, err := detector.DetectGaps(context.Background(), tt.pair, tt.interval, tt.startTime, tt.endTime)

			// Assertions
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, gaps, tt.expectedGaps)

				// Verify gap properties
				for _, gap := range gaps {
					assert.Equal(t, tt.pair, gap.Pair)
					assert.Equal(t, tt.interval, gap.Interval)
					assert.Equal(t, models.GapStatusDetected, gap.Status)
					assert.True(t, gap.StartTime.Before(gap.EndTime))
					assert.True(t, gap.StartTime.After(tt.startTime) || gap.StartTime.Equal(tt.startTime))
					assert.True(t, gap.EndTime.Before(tt.endTime) || gap.EndTime.Equal(tt.endTime))
				}
			}

			mockStorage.AssertExpectations(t)
			mockValidator.AssertExpectations(t)
		})
	}
}

// TestGapDetectorImpl_DetectGapsInSequence tests gap detection within candle sequences
func TestGapDetectorImpl_DetectGapsInSequence(t *testing.T) {
	tests := []struct {
		name             string
		candles          []contracts.Candle
		expectedInterval string
		expectedGaps     int
		expectedError    bool
	}{
		{
			name:             "empty sequence",
			candles:          []contracts.Candle{},
			expectedInterval: "1h",
			expectedGaps:     0,
			expectedError:    false,
		},
		{
			name:             "single candle",
			candles:          createContinuousContractCandles(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 1, "1h", "BTC-USD"),
			expectedInterval: "1h",
			expectedGaps:     0,
			expectedError:    false,
		},
		{
			name:             "continuous sequence - no gaps",
			candles:          createContinuousContractCandles(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 5, "1h", "BTC-USD"),
			expectedInterval: "1h",
			expectedGaps:     0,
			expectedError:    false,
		},
		{
			name: "single gap in sequence",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Gap here (2:00 missing)
				createContractCandle(time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			expectedInterval: "1h",
			expectedGaps:     1,
			expectedError:    false,
		},
		{
			name: "multiple gaps in sequence",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "5m"),
				// Gap (0:05 missing)
				createContractCandle(time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC), "BTC-USD", "5m"),
				// Gap (0:15, 0:20 missing)
				createContractCandle(time.Date(2024, 1, 1, 0, 25, 0, 0, time.UTC), "BTC-USD", "5m"),
			},
			expectedInterval: "5m",
			expectedGaps:     2,
			expectedError:    false,
		},
		{
			name: "unordered sequence (should be sorted)",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Gap at 1:00
				createContractCandle(time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			expectedInterval: "1h",
			expectedGaps:     1, // Should detect gap between 0:00 and 2:00
			expectedError:    false,
		},
		{
			name:             "invalid interval",
			candles:          createContinuousContractCandles(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 3, "1h", "BTC-USD"),
			expectedInterval: "invalid",
			expectedGaps:     0,
			expectedError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector := &GapDetectorImpl{
				logger: slog.Default(),
			}

			gaps, err := detector.DetectGapsInSequence(context.Background(), tt.candles, tt.expectedInterval)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, gaps, tt.expectedGaps)

				// Verify gap properties
				for _, gap := range gaps {
					assert.Equal(t, tt.expectedInterval, gap.Interval)
					assert.Equal(t, models.GapStatusDetected, gap.Status)
					assert.True(t, gap.StartTime.Before(gap.EndTime))
					if len(tt.candles) > 0 {
						assert.Equal(t, tt.candles[0].Pair, gap.Pair)
					}
				}
			}
		})
	}
}

// TestGapDetectorImpl_DetectRecentGaps tests recent gap detection functionality
func TestGapDetectorImpl_DetectRecentGaps(t *testing.T) {
	tests := []struct {
		name           string
		pair           string
		interval       string
		lookbackPeriod time.Duration
		mockSetup      func(*MockStorage, *MockGapValidator)
		expectedGaps   int
		expectedError  bool
	}{
		{
			name:           "recent gaps found",
			pair:           "BTC-USD",
			interval:       "1h",
			lookbackPeriod: 24 * time.Hour,
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				// Calculate expected time range
				now := time.Now().UTC()
				startTime := now.Add(-24 * time.Hour)

				// Create candles with gaps
				candles := []models.Candle{
					createTestCandle(startTime, "BTC-USD", "1h"),
					createTestCandle(startTime.Add(time.Hour), "BTC-USD", "1h"),
					// Gap at startTime + 2h
					createTestCandle(startTime.Add(3*time.Hour), "BTC-USD", "1h"),
				}

				queryResponse := &storage.QueryResponse{
					Candles: candles,
					Total:   len(candles),
				}

				ms.On("Query", mock.Anything, mock.MatchedBy(func(req storage.QueryRequest) bool {
					return req.Pair == "BTC-USD" && req.Interval == "1h"
				})).Return(queryResponse, nil)

				mv.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "1h").Return(true, "", nil)
				ms.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil)
			},
			expectedGaps:  1,
			expectedError: false,
		},
		{
			name:           "no recent gaps",
			pair:           "ETH-USD",
			interval:       "5m",
			lookbackPeriod: time.Hour,
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				now := time.Now().UTC()
				startTime := now.Add(-time.Hour)

				// Create continuous candles
				candles := createContinuousCandles(startTime, 12, "5m", "ETH-USD")

				queryResponse := &storage.QueryResponse{
					Candles: candles,
					Total:   len(candles),
				}

				ms.On("Query", mock.Anything, mock.MatchedBy(func(req storage.QueryRequest) bool {
					return req.Pair == "ETH-USD" && req.Interval == "5m"
				})).Return(queryResponse, nil)
			},
			expectedGaps:  0,
			expectedError: false,
		},
		{
			name:           "storage error",
			pair:           "BTC-USD",
			interval:       "1h",
			lookbackPeriod: time.Hour,
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				ms.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(nil, errors.New("storage error"))
			},
			expectedGaps:  0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockValidator := new(MockGapValidator)

			if tt.mockSetup != nil {
				tt.mockSetup(mockStorage, mockValidator)
			}

			detector := &GapDetectorImpl{
				storage:   mockStorage,
				validator: mockValidator,
				logger:    slog.Default(),
			}

			gaps, err := detector.DetectRecentGaps(context.Background(), tt.pair, tt.interval, tt.lookbackPeriod)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, gaps, tt.expectedGaps)
			}

			mockStorage.AssertExpectations(t)
			mockValidator.AssertExpectations(t)
		})
	}
}

// TestGapDetectorImpl_ValidateGapPeriod tests gap validation functionality
func TestGapDetectorImpl_ValidateGapPeriod(t *testing.T) {
	tests := []struct {
		name           string
		pair           string
		startTime      time.Time
		endTime        time.Time
		interval       string
		mockSetup      func(*MockGapValidator)
		expectedValid  bool
		expectedReason string
		expectedError  bool
	}{
		{
			name:      "valid gap period",
			pair:      "BTC-USD",
			startTime: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			interval:  "1h",
			mockSetup: func(mv *MockGapValidator) {
				mv.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "1h").Return(true, "", nil)
			},
			expectedValid:  true,
			expectedReason: "",
			expectedError:  false,
		},
		{
			name:      "gap too recent",
			pair:      "ETH-USD",
			startTime: time.Now().UTC().Add(-10 * time.Minute),
			endTime:   time.Now().UTC().Add(-5 * time.Minute),
			interval:  "5m",
			mockSetup: func(mv *MockGapValidator) {
				mv.On("IsValidGapPeriod", mock.Anything, "ETH-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "5m").Return(false, "gap too recent", nil)
			},
			expectedValid:  false,
			expectedReason: "gap too recent",
			expectedError:  false,
		},
		{
			name:      "validator error",
			pair:      "BTC-USD",
			startTime: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			interval:  "1h",
			mockSetup: func(mv *MockGapValidator) {
				mv.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "1h").Return(false, "", errors.New("validation error"))
			},
			expectedValid:  false,
			expectedReason: "",
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockValidator := new(MockGapValidator)

			if tt.mockSetup != nil {
				tt.mockSetup(mockValidator)
			}

			detector := &GapDetectorImpl{
				validator: mockValidator,
				logger:    slog.Default(),
			}

			isValid, reason, err := detector.ValidateGapPeriod(context.Background(), tt.pair, tt.startTime, tt.endTime, tt.interval)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedValid, isValid)
				assert.Equal(t, tt.expectedReason, reason)
			}

			mockValidator.AssertExpectations(t)
		})
	}
}

// TestBackfillerImpl_StartGapFilling tests individual gap filling functionality
func TestBackfillerImpl_StartGapFilling(t *testing.T) {
	tests := []struct {
		name          string
		gapID         string
		mockSetup     func(*MockStorage, *MockExchangeAdapter)
		expectedError bool
	}{
		{
			name:  "successful gap filling",
			gapID: "gap-1",
			mockSetup: func(ms *MockStorage, me *MockExchangeAdapter) {
				gap, _ := models.NewGap("gap-1", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-1").Return(gap, nil)
				ms.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil).Twice() // Once for filling status, once for failure/success

				// Mock successful exchange fetch
				candles := []contracts.Candle{
					createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
					createContractCandle(time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				}
				response := &contracts.FetchResponse{
					Candles: candles,
				}
				me.On("FetchCandles", mock.Anything, mock.AnythingOfType("contracts.FetchRequest")).Return(response, nil)

				ms.On("Store", mock.Anything, mock.AnythingOfType("[]models.Candle")).Return(nil)
				ms.On("MarkGapFilled", mock.Anything, "gap-1", mock.AnythingOfType("time.Time")).Return(nil)
			},
			expectedError: false,
		},
		{
			name:  "gap not found",
			gapID: "non-existent",
			mockSetup: func(ms *MockStorage, me *MockExchangeAdapter) {
				ms.On("GetGapByID", mock.Anything, "non-existent").Return(nil, nil)
			},
			expectedError: true,
		},
		{
			name:  "exchange fetch error",
			gapID: "gap-2",
			mockSetup: func(ms *MockStorage, me *MockExchangeAdapter) {
				gap, _ := models.NewGap("gap-2", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-2").Return(gap, nil)
				ms.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil).Twice()

				me.On("FetchCandles", mock.Anything, mock.AnythingOfType("contracts.FetchRequest")).Return(nil, errors.New("exchange error"))
			},
			expectedError: true,
		},
		{
			name:  "storage error during gap update",
			gapID: "gap-3",
			mockSetup: func(ms *MockStorage, me *MockExchangeAdapter) {
				gap, _ := models.NewGap("gap-3", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-3").Return(gap, nil)
				ms.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(errors.New("storage error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockExchange := new(MockExchangeAdapter)

			if tt.mockSetup != nil {
				tt.mockSetup(mockStorage, mockExchange)
			}

			backfiller := &BackfillerImpl{
				storage:     mockStorage,
				exchange:    mockExchange,
				logger:      slog.Default(),
				activeFills: make(map[string]bool),
				metrics:     &BackfillMetrics{},
			}

			err := backfiller.StartGapFilling(context.Background(), tt.gapID)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
			mockExchange.AssertExpectations(t)
		})
	}
}

// TestBackfillerImpl_FillGapWithData tests filling gaps with provided data
func TestBackfillerImpl_FillGapWithData(t *testing.T) {
	tests := []struct {
		name          string
		gapID         string
		candles       []contracts.Candle
		mockSetup     func(*MockStorage)
		expectedError bool
	}{
		{
			name:  "successful fill with data",
			gapID: "gap-1",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			mockSetup: func(ms *MockStorage) {
				gap, _ := models.NewGap("gap-1", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-1").Return(gap, nil)
				ms.On("Store", mock.Anything, mock.AnythingOfType("[]models.Candle")).Return(nil)
				ms.On("MarkGapFilled", mock.Anything, "gap-1", mock.AnythingOfType("time.Time")).Return(nil)
			},
			expectedError: false,
		},
		{
			name:    "empty candles",
			gapID:   "gap-2",
			candles: []contracts.Candle{},
			mockSetup: func(ms *MockStorage) {
				gap, _ := models.NewGap("gap-2", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-2").Return(gap, nil)
				ms.On("Store", mock.Anything, mock.AnythingOfType("[]models.Candle")).Return(nil)
				ms.On("MarkGapFilled", mock.Anything, "gap-2", mock.AnythingOfType("time.Time")).Return(nil)
			},
			expectedError: false,
		},
		{
			name:  "storage error",
			gapID: "gap-3",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			mockSetup: func(ms *MockStorage) {
				gap, _ := models.NewGap("gap-3", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")

				ms.On("GetGapByID", mock.Anything, "gap-3").Return(gap, nil)
				ms.On("Store", mock.Anything, mock.AnythingOfType("[]models.Candle")).Return(errors.New("storage error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)

			if tt.mockSetup != nil {
				tt.mockSetup(mockStorage)
			}

			backfiller := &BackfillerImpl{
				storage: mockStorage,
				logger:  slog.Default(),
			}

			err := backfiller.FillGapWithData(context.Background(), tt.gapID, tt.candles)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStorage.AssertExpectations(t)
		})
	}
}

// TestBackfillerImpl_GetBackfillProgress tests backfill progress reporting
func TestBackfillerImpl_GetBackfillProgress(t *testing.T) {
	backfiller := &BackfillerImpl{
		isRunning: true,
		metrics: &BackfillMetrics{
			TotalGapsProcessed: 10,
			GapsFilled:         7,
			GapsFailed:         3,
			CandlesRetrieved:   150,
			LastBackfillRun:    time.Now().UTC(),
		},
		logger: slog.Default(),
	}

	progress, err := backfiller.GetBackfillProgress(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, progress)
	assert.True(t, progress.Active)
	assert.Equal(t, 7, progress.CompletedGaps)
	assert.Equal(t, 3, progress.FailedGaps)
	assert.Equal(t, 70.0, progress.SuccessRate) // 7/10 * 100
}

// TestBackfillerImpl_StopBackfill tests graceful backfill stopping
func TestBackfillerImpl_StopBackfill(t *testing.T) {
	tests := []struct {
		name          string
		isRunning     bool
		expectedError bool
	}{
		{
			name:          "stop running backfill",
			isRunning:     true,
			expectedError: false,
		},
		{
			name:          "stop already stopped backfill",
			isRunning:     false,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backfiller := &BackfillerImpl{
				isRunning: tt.isRunning,
				logger:    slog.Default(),
			}

			err := backfiller.StopBackfill(context.Background())

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.False(t, backfiller.isRunning)
			}
		})
	}
}

// TestGapValidatorImpl_IsValidGapPeriod tests gap period validation
func TestGapValidatorImpl_IsValidGapPeriod(t *testing.T) {
	tests := []struct {
		name         string
		pair         string
		start        time.Time
		end          time.Time
		interval     string
		expectValid  bool
		expectReason string
		expectError  bool
	}{
		{
			name:         "valid gap period",
			pair:         "BTC-USD",
			start:        time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:          time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			interval:     "1h",
			expectValid:  true,
			expectReason: "",
			expectError:  false,
		},
		{
			name:         "gap too short",
			pair:         "BTC-USD",
			start:        time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:          time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
			interval:     "1h",
			expectValid:  false,
			expectReason: "gap too short",
			expectError:  false,
		},
		{
			name:         "gap too recent",
			pair:         "BTC-USD",
			start:        time.Now().UTC().Add(-2 * time.Minute),
			end:          time.Now().UTC().Add(-1 * time.Minute),
			interval:     "1m",
			expectValid:  false,
			expectReason: "gap too recent",
			expectError:  false,
		},
		{
			name:         "invalid interval",
			pair:         "BTC-USD",
			start:        time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:          time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			interval:     "invalid",
			expectValid:  false,
			expectReason: "invalid interval",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &GapValidatorImpl{
				logger: slog.Default(),
			}

			isValid, reason, err := validator.IsValidGapPeriod(context.Background(), tt.pair, tt.start, tt.end, tt.interval)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectValid, isValid)
				assert.Equal(t, tt.expectReason, reason)
			}
		})
	}
}

// TestGapValidatorImpl_ShouldIgnoreGap tests gap ignore logic
func TestGapValidatorImpl_ShouldIgnoreGap(t *testing.T) {
	tests := []struct {
		name         string
		gap          *models.Gap
		expectIgnore bool
		expectReason string
		expectError  bool
	}{
		{
			name: "normal gap - should not ignore",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-1", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")
				return g
			}(),
			expectIgnore: false,
			expectReason: "",
			expectError:  false,
		},
		{
			name: "max retry attempts exceeded",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-2", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")
				g.Attempts = 6 // Exceeds max of 5
				return g
			}(),
			expectIgnore: true,
			expectReason: "max retry attempts exceeded",
			expectError:  false,
		},
		{
			name: "gap too old",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-3", "BTC-USD",
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
					"1h")
				g.CreatedAt = time.Now().UTC().Add(-31 * 24 * time.Hour) // More than 30 days old
				return g
			}(),
			expectIgnore: true,
			expectReason: "gap too old",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &GapValidatorImpl{
				logger: slog.Default(),
			}

			shouldIgnore, reason, err := validator.ShouldIgnoreGap(context.Background(), tt.gap)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectIgnore, shouldIgnore)
				assert.Equal(t, tt.expectReason, reason)
			}
		})
	}
}

// TestGapValidatorImpl_ValidateGapData tests gap data validation
func TestGapValidatorImpl_ValidateGapData(t *testing.T) {
	gapStart := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	gapEnd := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		gap           *models.Gap
		candles       []contracts.Candle
		expectedError bool
		errorContains string
	}{
		{
			name: "valid gap data",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-1", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			expectedError: false,
		},
		{
			name: "empty candles",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-2", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles:       []contracts.Candle{},
			expectedError: true,
			errorContains: "no candles provided",
		},
		{
			name: "candle outside gap range - before",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-3", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC), "BTC-USD", "1h"), // Before gap start
			},
			expectedError: true,
			errorContains: "outside gap range",
		},
		{
			name: "candle outside gap range - after",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-4", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC), "BTC-USD", "1h"), // After gap end
			},
			expectedError: true,
			errorContains: "outside gap range",
		},
		{
			name: "wrong pair",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-5", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "ETH-USD", "1h"), // Wrong pair
			},
			expectedError: true,
			errorContains: "does not match gap pair",
		},
		{
			name: "wrong interval",
			gap: func() *models.Gap {
				g, _ := models.NewGap("gap-6", "BTC-USD", gapStart, gapEnd, "1h")
				return g
			}(),
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), "BTC-USD", "5m"), // Wrong interval
			},
			expectedError: true,
			errorContains: "does not match gap interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &GapValidatorImpl{
				logger: slog.Default(),
			}

			err := validator.ValidateGapData(context.Background(), tt.gap, tt.candles)

			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestParseIntervalDuration tests interval parsing functionality
func TestParseIntervalDuration(t *testing.T) {
	tests := []struct {
		name        string
		interval    string
		expected    time.Duration
		expectError bool
	}{
		{"1 minute", "1m", time.Minute, false},
		{"5 minutes", "5m", 5 * time.Minute, false},
		{"15 minutes", "15m", 15 * time.Minute, false},
		{"30 minutes", "30m", 30 * time.Minute, false},
		{"1 hour", "1h", time.Hour, false},
		{"4 hours", "4h", 4 * time.Hour, false},
		{"8 hours", "8h", 8 * time.Hour, false},
		{"12 hours", "12h", 12 * time.Hour, false},
		{"1 day", "1d", 24 * time.Hour, false},
		{"1 week", "1w", 7 * 24 * time.Hour, false},
		{"1 month", "1M", 30 * 24 * time.Hour, false},
		{"custom 2 hours", "2h", 2 * time.Hour, false},
		{"custom 3 minutes", "3m", 3 * time.Minute, false},
		{"custom 7 days", "7d", 7 * 24 * time.Hour, false},
		{"invalid format", "invalid", 0, true},
		{"empty", "", 0, true},
		{"invalid unit", "5x", 0, true},
		{"invalid value", "ah", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := parseIntervalDuration(tt.interval)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, duration)
			}
		})
	}
}

// TestGenerateGapID tests gap ID generation
func TestGenerateGapID(t *testing.T) {
	pair := "BTC/USD"
	startTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := "1h"

	id1 := generateGapID(pair, startTime, endTime, interval)
	id2 := generateGapID(pair, startTime, endTime, interval)

	// IDs should be different due to UUID component
	assert.NotEqual(t, id1, id2)

	// IDs should contain cleaned pair (/ replaced with -)
	assert.Contains(t, id1, "BTC-USD")
	assert.NotContains(t, id1, "BTC/USD")

	// IDs should contain interval
	assert.Contains(t, id1, interval)

	// IDs should contain timestamps
	assert.Contains(t, id1, fmt.Sprintf("%d", startTime.Unix()))
	assert.Contains(t, id1, fmt.Sprintf("%d", endTime.Unix()))
}

// TestConcurrentGapDetection tests gap detection under concurrent load
func TestConcurrentGapDetection(t *testing.T) {
	mockStorage := new(MockStorage)
	mockValidator := new(MockGapValidator)

	// Setup mocks for concurrent access
	queryResponse := &storage.QueryResponse{
		Candles: []models.Candle{
			createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			// Gap at 1:00
			createTestCandle(time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
		},
		Total: 2,
	}

	mockStorage.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)
	mockValidator.On("IsValidGapPeriod", mock.Anything, mock.Anything, mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), mock.Anything).Return(true, "", nil)
	mockStorage.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil)

	detector := &GapDetectorImpl{
		storage:   mockStorage,
		validator: mockValidator,
		logger:    slog.Default(),
	}

	// Run concurrent gap detections
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	gapCounts := make(chan int, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			gaps, err := detector.DetectGaps(
				context.Background(),
				"BTC-USD",
				"1h",
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
			)

			if err != nil {
				errors <- err
				return
			}

			gapCounts <- len(gaps)
		}(i)
	}

	wg.Wait()
	close(errors)
	close(gapCounts)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent gap detection failed: %v", err)
	}

	// Check that all goroutines detected the same number of gaps
	expectedGaps := 1
	for count := range gapCounts {
		assert.Equal(t, expectedGaps, count, "Gap count should be consistent across concurrent runs")
	}

	mockStorage.AssertExpectations(t)
	mockValidator.AssertExpectations(t)
}

// TestLargeDatasetPerformance tests gap detection performance with large datasets
func TestLargeDatasetPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	mockStorage := new(MockStorage)
	mockValidator := new(MockGapValidator)

	// Create a large dataset with gaps
	const numCandles = 10000
	candles := make([]models.Candle, 0, numCandles)
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create candles with intentional gaps every 100 candles
	for i := 0; i < numCandles; i++ {
		if i%100 == 50 { // Skip every 100th candle to create gaps
			continue
		}
		timestamp := startTime.Add(time.Duration(i) * time.Minute)
		candles = append(candles, createTestCandle(timestamp, "BTC-USD", "1m"))
	}

	queryResponse := &storage.QueryResponse{
		Candles: candles,
		Total:   len(candles),
	}

	mockStorage.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)
	mockValidator.On("IsValidGapPeriod", mock.Anything, mock.Anything, mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), mock.Anything).Return(true, "", nil)
	mockStorage.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil)

	detector := &GapDetectorImpl{
		storage:   mockStorage,
		validator: mockValidator,
		logger:    slog.Default(),
	}

	// Measure performance
	start := time.Now()
	gaps, err := detector.DetectGaps(
		context.Background(),
		"BTC-USD",
		"1m",
		startTime,
		startTime.Add(time.Duration(numCandles)*time.Minute),
	)
	duration := time.Since(start)

	require.NoError(t, err)

	// Should detect approximately numCandles/100 gaps
	expectedGaps := numCandles / 100
	assert.InDelta(t, expectedGaps, len(gaps), float64(expectedGaps)*0.1, "Gap count should be within 10% of expected")

	// Performance assertion - should complete within reasonable time
	maxDuration := 5 * time.Second
	assert.Less(t, duration, maxDuration, "Gap detection should complete within %v for %d candles, took %v", maxDuration, numCandles, duration)

	t.Logf("Gap detection for %d candles completed in %v, found %d gaps", numCandles, duration, len(gaps))

	mockStorage.AssertExpectations(t)
	mockValidator.AssertExpectations(t)
}

// TestWeekendAndHolidayGaps tests gap detection during non-trading periods
func TestWeekendAndHolidayGaps(t *testing.T) {
	tests := []struct {
		name          string
		startTime     time.Time
		endTime       time.Time
		interval      string
		candles       []models.Candle
		mockValidator func(*MockGapValidator)
		expectedGaps  int
	}{
		{
			name:      "weekend gap should be ignored",
			startTime: time.Date(2024, 1, 5, 20, 0, 0, 0, time.UTC), // Friday 8 PM UTC
			endTime:   time.Date(2024, 1, 8, 4, 0, 0, 0, time.UTC),  // Monday 4 AM UTC
			interval:  "1h",
			candles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 5, 20, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Weekend gap
				createTestCandle(time.Date(2024, 1, 8, 4, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			mockValidator: func(mv *MockGapValidator) {
				// Weekend gaps should be marked as invalid
				mv.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "1h").Return(false, "weekend period", nil)
			},
			expectedGaps: 0,
		},
		{
			name:      "regular weekday gap should be detected",
			startTime: time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC), // Tuesday
			endTime:   time.Date(2024, 1, 2, 15, 0, 0, 0, time.UTC), // Tuesday
			interval:  "1h",
			candles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 2, 11, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Gap at 12:00
				createTestCandle(time.Date(2024, 1, 2, 13, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 2, 14, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			mockValidator: func(mv *MockGapValidator) {
				mv.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), "1h").Return(true, "", nil)
			},
			expectedGaps: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockValidator := new(MockGapValidator)

			queryResponse := &storage.QueryResponse{
				Candles: tt.candles,
				Total:   len(tt.candles),
			}

			mockStorage.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)

			if tt.mockValidator != nil {
				tt.mockValidator(mockValidator)
			}

			if tt.expectedGaps > 0 {
				mockStorage.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil).Times(tt.expectedGaps)
			}

			detector := &GapDetectorImpl{
				storage:   mockStorage,
				validator: mockValidator,
				logger:    slog.Default(),
			}

			gaps, err := detector.DetectGaps(context.Background(), "BTC-USD", tt.interval, tt.startTime, tt.endTime)

			assert.NoError(t, err)
			assert.Len(t, gaps, tt.expectedGaps)

			mockStorage.AssertExpectations(t)
			mockValidator.AssertExpectations(t)
		})
	}
}

// TestBoundaryConditions tests gap detection at data boundaries
func TestBoundaryConditions(t *testing.T) {
	tests := []struct {
		name         string
		startTime    time.Time
		endTime      time.Time
		interval     string
		candles      []models.Candle
		expectedGaps int
	}{
		{
			name:      "gap at start boundary",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC),
			interval:  "1h",
			candles: []models.Candle{
				// Missing candles at 0:00, 1:00
				createTestCandle(time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			expectedGaps: 1, // Gap from 0:00 to 2:00
		},
		{
			name:      "gap at end boundary",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC),
			interval:  "1h",
			candles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				// Missing candles at 2:00, 3:00
			},
			expectedGaps: 1, // Gap from 2:00 to 4:00
		},
		{
			name:      "exact boundary match",
			startTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:   time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			interval:  "1h",
			candles: []models.Candle{
				createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createTestCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			expectedGaps: 0, // Perfect match, no gaps
		},
		{
			name:         "single timestamp range",
			startTime:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTime:      time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			interval:     "1h",
			candles:      []models.Candle{}, // No candles
			expectedGaps: 1,                 // Should detect one gap for the single expected candle
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockValidator := new(MockGapValidator)

			queryResponse := &storage.QueryResponse{
				Candles: tt.candles,
				Total:   len(tt.candles),
			}

			mockStorage.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)

			if tt.expectedGaps > 0 {
				mockValidator.On("IsValidGapPeriod", mock.Anything, "BTC-USD", mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), tt.interval).Return(true, "", nil)
				mockStorage.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(nil).Times(tt.expectedGaps)
			}

			detector := &GapDetectorImpl{
				storage:   mockStorage,
				validator: mockValidator,
				logger:    slog.Default(),
			}

			gaps, err := detector.DetectGaps(context.Background(), "BTC-USD", tt.interval, tt.startTime, tt.endTime)

			assert.NoError(t, err)
			assert.Len(t, gaps, tt.expectedGaps)

			mockStorage.AssertExpectations(t)
			mockValidator.AssertExpectations(t)
		})
	}
}

// TestOverlappingDataHandling tests how overlapping data is handled
func TestOverlappingDataHandling(t *testing.T) {
	tests := []struct {
		name         string
		candles      []contracts.Candle
		interval     string
		expectedGaps int
	}{
		{
			name: "duplicate timestamps - no gaps",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"), // Duplicate
				createContractCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			interval:     "1h",
			expectedGaps: 0,
		},
		{
			name: "overlapping and continuous",
			candles: []contracts.Candle{
				createContractCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
				createContractCandle(time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC), "BTC-USD", "1h"), // Overlapping
				createContractCandle(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
			},
			interval:     "1h",
			expectedGaps: 0, // Should not detect gaps due to overlapping data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector := &GapDetectorImpl{
				logger: slog.Default(),
			}

			gaps, err := detector.DetectGapsInSequence(context.Background(), tt.candles, tt.interval)

			assert.NoError(t, err)
			assert.Len(t, gaps, tt.expectedGaps)
		})
	}
}

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		mockSetup     func(*MockStorage, *MockGapValidator)
		expectError   bool
		errorContains string
	}{
		{
			name: "storage query error",
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				ms.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(nil, errors.New("storage connection failed"))
			},
			expectError:   true,
			errorContains: "failed to query existing candles",
		},
		{
			name: "gap validation error",
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				queryResponse := &storage.QueryResponse{
					Candles: []models.Candle{
						createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
						// Gap at 1:00
						createTestCandle(time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
					},
					Total: 2,
				}
				ms.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)
				mv.On("IsValidGapPeriod", mock.Anything, mock.Anything, mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), mock.Anything).Return(false, "", errors.New("validation service unavailable"))
			},
			expectError:   false, // Should continue processing despite validation errors
			errorContains: "",
		},
		{
			name: "gap storage error",
			mockSetup: func(ms *MockStorage, mv *MockGapValidator) {
				queryResponse := &storage.QueryResponse{
					Candles: []models.Candle{
						createTestCandle(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
						// Gap at 1:00
						createTestCandle(time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), "BTC-USD", "1h"),
					},
					Total: 2,
				}
				ms.On("Query", mock.Anything, mock.AnythingOfType("storage.QueryRequest")).Return(queryResponse, nil)
				mv.On("IsValidGapPeriod", mock.Anything, mock.Anything, mock.AnythingOfType("time.Time"), mock.AnythingOfType("time.Time"), mock.Anything).Return(true, "", nil)
				ms.On("StoreGap", mock.Anything, mock.AnythingOfType("models.Gap")).Return(errors.New("storage full"))
			},
			expectError:   false, // Should continue processing despite storage errors
			errorContains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockStorage)
			mockValidator := new(MockGapValidator)

			if tt.mockSetup != nil {
				tt.mockSetup(mockStorage, mockValidator)
			}

			detector := &GapDetectorImpl{
				storage:   mockStorage,
				validator: mockValidator,
				logger:    slog.Default(),
			}

			gaps, err := detector.DetectGaps(
				context.Background(),
				"BTC-USD",
				"1h",
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
			)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, gaps)
			}

			mockStorage.AssertExpectations(t)
			mockValidator.AssertExpectations(t)
		})
	}
}

// TestGapPriorityAndClassification tests gap priority assignment
func TestGapPriorityAndClassification(t *testing.T) {
	tests := []struct {
		name             string
		gapStart         time.Time
		gapEnd           time.Time
		createdAt        time.Time
		expectedPriority models.GapPriority
	}{
		{
			name:             "recent short gap - low priority",
			gapStart:         time.Now().UTC().Add(-30 * time.Minute),
			gapEnd:           time.Now().UTC().Add(-15 * time.Minute),
			createdAt:        time.Now().UTC().Add(-30 * time.Minute),
			expectedPriority: models.PriorityLow,
		},
		{
			name:             "old gap - high priority",
			gapStart:         time.Now().UTC().Add(-48 * time.Hour),
			gapEnd:           time.Now().UTC().Add(-47 * time.Hour),
			createdAt:        time.Now().UTC().Add(-48 * time.Hour),
			expectedPriority: models.PriorityHigh,
		},
		{
			name:             "very old gap - critical priority",
			gapStart:         time.Now().UTC().Add(-8 * 24 * time.Hour),
			gapEnd:           time.Now().UTC().Add(-8*24*time.Hour + time.Hour),
			createdAt:        time.Now().UTC().Add(-8 * 24 * time.Hour),
			expectedPriority: models.PriorityCritical,
		},
		{
			name:             "long gap - critical priority",
			gapStart:         time.Now().UTC().Add(-8 * 24 * time.Hour),
			gapEnd:           time.Now().UTC().Add(-1 * 24 * time.Hour), // 7-day gap
			createdAt:        time.Now().UTC().Add(-2 * time.Hour),
			expectedPriority: models.PriorityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gap, err := models.NewGap("test-gap", "BTC-USD", tt.gapStart, tt.gapEnd, "1h")
			require.NoError(t, err)

			// Manually set created at time for testing
			gap.CreatedAt = tt.createdAt
			gap.UpdatePriority()

			assert.Equal(t, tt.expectedPriority, gap.Priority)
		})
	}
}
