package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// TestValidationPipeline_EndToEnd tests the complete validation pipeline
// from data collection through anomaly detection and reporting
func TestValidationPipeline_EndToEnd(t *testing.T) {
	ctx := context.Background()
	
	// Initialize validation pipeline - WILL FAIL until implemented
	validator, err := NewDataValidator(&ValidationConfig{
		PriceSpikeThreshold:     5.0,  // 500% spike threshold from research.md
		VolumeSurgeThreshold:    10.0, // 10x volume surge threshold from research.md
		TimestampDriftTolerance: time.Minute * 2,
		EnableLogicalChecks:     true,
		EnableAnomalyDetection:  true,
	})
	require.NoError(t, err, "Failed to create data validator")
	require.NotNil(t, validator, "Validator should not be nil")

	// Create test OHLCV data with various scenarios
	testCandles := createTestCandleData()
	
	// Test complete validation pipeline
	results, err := validator.ValidateCandles(ctx, testCandles)
	require.NoError(t, err, "Validation pipeline should not error")
	require.NotNil(t, results, "Validation results should not be nil")

	// Verify validation results structure
	assert.Greater(t, len(results.ValidationErrors), 0, "Should detect validation errors in test data")
	assert.Greater(t, len(results.AnomalyDetections), 0, "Should detect anomalies in test data")
	assert.NotNil(t, results.QualityMetrics, "Quality metrics should be present")
	
	// Test data quality metrics
	assert.GreaterOrEqual(t, results.QualityMetrics.TotalCandles, int64(len(testCandles)))
	assert.Greater(t, results.QualityMetrics.ValidationErrors, int64(0))
	assert.Greater(t, results.QualityMetrics.AnomaliesDetected, int64(0))
	assert.LessOrEqual(t, results.QualityMetrics.QualityScore, 1.0)
}

// TestLogicalConsistencyValidation tests OHLCV logical consistency rules
func TestLogicalConsistencyValidation(t *testing.T) {
	ctx := context.Background()
	
	validator, err := NewDataValidator(&ValidationConfig{
		EnableLogicalChecks: true,
	})
	require.NoError(t, err)

	tests := []struct {
		name           string
		candle         contracts.Candle
		expectError    bool
		errorType      string
	}{
		{
			name: "valid_candle_all_fields_consistent",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "100.50",
				High:      "105.75",
				Low:       "98.25",
				Close:     "103.00",
				Volume:    "1500.50",
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: false,
		},
		{
			name: "invalid_high_less_than_open",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "100.50",
				High:      "95.00", // High < Open - INVALID
				Low:       "90.25",
				Close:     "93.00",
				Volume:    "1500.50",
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: true,
			errorType:   "logical_consistency",
		},
		{
			name: "invalid_high_less_than_close",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "95.00",
				High:      "98.00", // High < Close - INVALID
				Low:       "90.25",
				Close:     "100.00",
				Volume:    "1500.50",
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: true,
			errorType:   "logical_consistency",
		},
		{
			name: "invalid_low_greater_than_open",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "95.00",
				High:      "105.00",
				Low:       "98.00", // Low > Open - INVALID
				Close:     "97.00",
				Volume:    "1500.50",
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: true,
			errorType:   "logical_consistency",
		},
		{
			name: "invalid_low_greater_than_close",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "98.00",
				High:      "105.00",
				Low:       "99.00", // Low > Close - INVALID
				Close:     "97.00",
				Volume:    "1500.50",
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: true,
			errorType:   "logical_consistency",
		},
		{
			name: "invalid_negative_volume",
			candle: contracts.Candle{
				Timestamp: time.Now(),
				Open:      "100.00",
				High:      "105.00",
				Low:       "95.00",
				Close:     "103.00",
				Volume:    "-100.50", // Negative volume - INVALID
				Pair:      "BTC-USD",
				Interval:  "1h",
			},
			expectError: true,
			errorType:   "logical_consistency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := validator.ValidateCandles(ctx, []contracts.Candle{tt.candle})
			require.NoError(t, err)

			if tt.expectError {
				assert.Greater(t, len(results.ValidationErrors), 0, "Should detect validation error")
				found := false
				for _, validationErr := range results.ValidationErrors {
					if validationErr.ErrorType == tt.errorType {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find error of type %s", tt.errorType)
			} else {
				assert.Equal(t, 0, len(results.ValidationErrors), "Should not detect validation errors")
			}
		})
	}
}

// TestAnomalyDetection tests price spike and volume surge detection
func TestAnomalyDetection(t *testing.T) {
	ctx := context.Background()
	
	validator, err := NewDataValidator(&ValidationConfig{
		PriceSpikeThreshold:    5.0,  // 500% spike from research.md
		VolumeSurgeThreshold:   10.0, // 10x volume surge from research.md
		EnableAnomalyDetection: true,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		candles       []contracts.Candle
		expectAnomaly bool
		anomalyType   string
	}{
		{
			name: "normal_price_movement",
			candles: []contracts.Candle{
				createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
				createCandle("103.00", "108.00", "101.00", "106.00", "1100.00", time.Now()),
			},
			expectAnomaly: false,
		},
		{
			name: "price_spike_500_percent",
			candles: []contracts.Candle{
				createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
				createCandle("103.00", "620.00", "103.00", "615.00", "1100.00", time.Now()), // 500%+ spike
			},
			expectAnomaly: true,
			anomalyType:   "price_spike",
		},
		{
			name: "volume_surge_10x",
			candles: []contracts.Candle{
				createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
				createCandle("103.00", "108.00", "101.00", "106.00", "11000.00", time.Now()), // 10x+ surge
			},
			expectAnomaly: true,
			anomalyType:   "volume_surge",
		},
		{
			name: "combined_price_and_volume_anomaly",
			candles: []contracts.Candle{
				createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
				createCandle("103.00", "720.00", "103.00", "715.00", "15000.00", time.Now()), // Both anomalies
			},
			expectAnomaly: true,
			anomalyType:   "combined_anomaly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := validator.ValidateCandles(ctx, tt.candles)
			require.NoError(t, err)

			if tt.expectAnomaly {
				assert.Greater(t, len(results.AnomalyDetections), 0, "Should detect anomalies")
				found := false
				for _, anomaly := range results.AnomalyDetections {
					if anomaly.AnomalyType == tt.anomalyType || 
					   (tt.anomalyType == "combined_anomaly" && 
					   (anomaly.AnomalyType == "price_spike" || anomaly.AnomalyType == "volume_surge")) {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find anomaly of type %s", tt.anomalyType)
			} else {
				assert.Equal(t, 0, len(results.AnomalyDetections), "Should not detect anomalies")
			}
		})
	}
}

// TestTimestampSequenceValidation tests timestamp ordering and drift tolerance
func TestTimestampSequenceValidation(t *testing.T) {
	ctx := context.Background()
	
	validator, err := NewDataValidator(&ValidationConfig{
		TimestampDriftTolerance: time.Minute * 2, // 2 minute tolerance from research.md
		EnableTimestampChecks:   true,
	})
	require.NoError(t, err)

	baseTime := time.Now().Truncate(time.Hour)
	
	tests := []struct {
		name        string
		candles     []contracts.Candle
		expectError bool
		errorType   string
	}{
		{
			name: "valid_sequence_proper_ordering",
			candles: []contracts.Candle{
				createCandleAtTime("100.00", baseTime),
				createCandleAtTime("101.00", baseTime.Add(time.Hour)),
				createCandleAtTime("102.00", baseTime.Add(2*time.Hour)),
			},
			expectError: false,
		},
		{
			name: "invalid_sequence_out_of_order",
			candles: []contracts.Candle{
				createCandleAtTime("100.00", baseTime.Add(time.Hour)),
				createCandleAtTime("101.00", baseTime), // Out of order
				createCandleAtTime("102.00", baseTime.Add(2*time.Hour)),
			},
			expectError: true,
			errorType:   "timestamp_sequence",
		},
		{
			name: "invalid_sequence_excessive_drift",
			candles: []contracts.Candle{
				createCandleAtTime("100.00", baseTime),
				createCandleAtTime("101.00", baseTime.Add(time.Hour+5*time.Minute)), // >2min drift
			},
			expectError: true,
			errorType:   "timestamp_drift",
		},
		{
			name: "valid_sequence_within_drift_tolerance",
			candles: []contracts.Candle{
				createCandleAtTime("100.00", baseTime),
				createCandleAtTime("101.00", baseTime.Add(time.Hour+90*time.Second)), // <2min drift
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := validator.ValidateCandles(ctx, tt.candles)
			require.NoError(t, err)

			if tt.expectError {
				assert.Greater(t, len(results.ValidationErrors), 0, "Should detect validation error")
				found := false
				for _, validationErr := range results.ValidationErrors {
					if validationErr.ErrorType == tt.errorType {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find error of type %s", tt.errorType)
			} else {
				assert.Equal(t, 0, len(results.ValidationErrors), "Should not detect validation errors")
			}
		})
	}
}

// TestValidationPipelineIntegrationWithDataCollection tests integration between
// data collection and validation pipeline
func TestValidationPipelineIntegrationWithDataCollection(t *testing.T) {
	ctx := context.Background()

	// Mock data collector - WILL FAIL until implemented
	collector, err := NewMockDataCollector()
	require.NoError(t, err)

	// Mock validator
	validator, err := NewDataValidator(&ValidationConfig{
		PriceSpikeThreshold:    5.0,
		VolumeSurgeThreshold:   10.0,
		EnableLogicalChecks:    true,
		EnableAnomalyDetection: true,
	})
	require.NoError(t, err)

	// Mock storage
	storage := NewValidationMockStorage()

	// Create validation pipeline
	pipeline, err := NewValidationPipeline(&ValidationPipelineConfig{
		Collector: collector,
		Validator: validator,
		Storage:   storage,
		BatchSize: 100,
	})
	require.NoError(t, err)

	// Test pipeline processing
	request := &DataCollectionRequest{
		Pair:      "BTC-USD",
		Interval:  "1h",
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
	}

	results, err := pipeline.ProcessData(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, results)

	// Verify integration results
	assert.Greater(t, results.ProcessedCandles, int64(0), "Should process candles")
	assert.GreaterOrEqual(t, results.ValidCandles, int64(0), "Should track valid candles")
	assert.GreaterOrEqual(t, results.InvalidCandles, int64(0), "Should track invalid candles")
	assert.NotNil(t, results.QualityMetrics, "Quality metrics should be present")
}

// TestConfigurableThresholdsAndRules tests that validation rules can be configured
func TestConfigurableThresholdsAndRules(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		config           *ValidationConfig
		testCandle       contracts.Candle
		previousCandle   contracts.Candle
		expectAnomalies  int
		expectErrors     int
	}{
		{
			name: "strict_thresholds_detect_more_anomalies",
			config: &ValidationConfig{
				PriceSpikeThreshold:    1.5, // 150% - strict
				VolumeSurgeThreshold:   2.0, // 2x - strict
				EnableAnomalyDetection: true,
			},
			testCandle:     createCandle("100.00", "180.00", "100.00", "175.00", "3000.00", time.Now()),
			previousCandle: createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
			expectAnomalies: 2, // Both price and volume
		},
		{
			name: "lenient_thresholds_detect_fewer_anomalies",
			config: &ValidationConfig{
				PriceSpikeThreshold:    10.0, // 1000% - lenient
				VolumeSurgeThreshold:   20.0, // 20x - lenient
				EnableAnomalyDetection: true,
			},
			testCandle:     createCandle("100.00", "180.00", "100.00", "175.00", "3000.00", time.Now()),
			previousCandle: createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-time.Hour)),
			expectAnomalies: 0, // Neither threshold exceeded
		},
		{
			name: "disabled_logical_checks",
			config: &ValidationConfig{
				EnableLogicalChecks: false,
			},
			testCandle:   createCandle("100.00", "95.00", "105.00", "103.00", "1000.00", time.Now()), // Invalid logic
			expectErrors: 0, // Logic checks disabled
		},
		{
			name: "enabled_logical_checks",
			config: &ValidationConfig{
				EnableLogicalChecks: true,
			},
			testCandle:   createCandle("100.00", "95.00", "105.00", "103.00", "1000.00", time.Now()), // Invalid logic
			expectErrors: 2, // High < Open, Low > High
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator, err := NewDataValidator(tt.config)
			require.NoError(t, err)

			var candles []contracts.Candle
			if !tt.previousCandle.Timestamp.IsZero() {
				candles = []contracts.Candle{tt.previousCandle, tt.testCandle}
			} else {
				candles = []contracts.Candle{tt.testCandle}
			}

			results, err := validator.ValidateCandles(ctx, candles)
			require.NoError(t, err)

			assert.Equal(t, tt.expectAnomalies, len(results.AnomalyDetections), 
				"Anomaly count should match expected for config")
			assert.Equal(t, tt.expectErrors, len(results.ValidationErrors), 
				"Error count should match expected for config")
		})
	}
}

// TestErrorScenarios tests various error conditions
func TestErrorScenarios(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		scenario    string
		expectError bool
	}{
		{
			name:        "validation_with_nil_candles",
			scenario:    "nil_input",
			expectError: true,
		},
		{
			name:        "validation_with_empty_candles",
			scenario:    "empty_input",
			expectError: false, // Empty input should be valid
		},
		{
			name:        "validation_with_corrupted_price_data",
			scenario:    "corrupted_prices",
			expectError: true,
		},
		{
			name:        "validation_with_missing_required_fields",
			scenario:    "missing_fields",
			expectError: true,
		},
		{
			name:        "validation_with_invalid_timestamps",
			scenario:    "invalid_timestamps",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator, err := NewDataValidator(&ValidationConfig{
				EnableLogicalChecks:    true,
				EnableAnomalyDetection: true,
			})
			require.NoError(t, err)

			var testData []contracts.Candle
			var validationErr error

			switch tt.scenario {
			case "nil_input":
				testData = nil
			case "empty_input":
				testData = []contracts.Candle{}
			case "corrupted_prices":
				testData = []contracts.Candle{
					{
						Timestamp: time.Now(),
						Open:      "not_a_number",
						High:      "105.00",
						Low:       "95.00",
						Close:     "100.00",
						Volume:    "1000.00",
						Pair:      "BTC-USD",
						Interval:  "1h",
					},
				}
			case "missing_fields":
				testData = []contracts.Candle{
					{
						Timestamp: time.Now(),
						// Missing required fields
						Pair:     "BTC-USD",
						Interval: "1h",
					},
				}
			case "invalid_timestamps":
				testData = []contracts.Candle{
					{
						Timestamp: time.Time{}, // Zero timestamp
						Open:      "100.00",
						High:      "105.00",
						Low:       "95.00",
						Close:     "100.00",
						Volume:    "1000.00",
						Pair:      "BTC-USD",
						Interval:  "1h",
					},
				}
			}

			_, validationErr = validator.ValidateCandles(ctx, testData)

			if tt.expectError {
				assert.Error(t, validationErr, "Should return error for scenario %s", tt.scenario)
			} else {
				assert.NoError(t, validationErr, "Should not return error for scenario %s", tt.scenario)
			}
		})
	}
}

// TestDataQualityMetricsAndReporting tests quality metrics calculation and reporting
func TestDataQualityMetricsAndReporting(t *testing.T) {
	ctx := context.Background()

	validator, err := NewDataValidator(&ValidationConfig{
		PriceSpikeThreshold:    5.0,
		VolumeSurgeThreshold:   10.0,
		EnableLogicalChecks:    true,
		EnableAnomalyDetection: true,
	})
	require.NoError(t, err)

	// Create mixed quality test data
	testCandles := []contracts.Candle{
		// Valid candles
		createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", time.Now().Add(-3*time.Hour)),
		createCandle("103.00", "108.00", "101.00", "106.00", "1100.00", time.Now().Add(-2*time.Hour)),
		
		// Invalid logical consistency
		createCandle("106.00", "104.00", "102.00", "105.00", "1200.00", time.Now().Add(-time.Hour)), // High < Open
		
		// Price anomaly
		createCandle("105.00", "630.00", "105.00", "625.00", "1300.00", time.Now()), // 500%+ spike
	}

	results, err := validator.ValidateCandles(ctx, testCandles)
	require.NoError(t, err)
	require.NotNil(t, results.QualityMetrics)

	metrics := results.QualityMetrics

	// Verify metrics calculation
	assert.Equal(t, int64(len(testCandles)), metrics.TotalCandles, "Total candles should match input")
	assert.Greater(t, metrics.ValidationErrors, int64(0), "Should count validation errors")
	assert.Greater(t, metrics.AnomaliesDetected, int64(0), "Should count anomalies")
	
	// Quality score should be between 0 and 1
	assert.GreaterOrEqual(t, metrics.QualityScore, 0.0, "Quality score should be >= 0")
	assert.LessOrEqual(t, metrics.QualityScore, 1.0, "Quality score should be <= 1")
	
	// Verify detailed metrics
	assert.Greater(t, len(metrics.ErrorsByType), 0, "Should categorize errors by type")
	assert.Greater(t, len(metrics.AnomaliesByType), 0, "Should categorize anomalies by type")
	
	// Verify processing metadata
	assert.Greater(t, metrics.ProcessingTimeMs, int64(0), "Should track processing time")
	assert.False(t, metrics.ProcessedAt.IsZero(), "Should record processing timestamp")
}

// Helper functions and types (these would normally be implemented)

// ValidationConfig represents configuration for the data validator
type ValidationConfig struct {
	PriceSpikeThreshold     float64
	VolumeSurgeThreshold    float64
	TimestampDriftTolerance time.Duration
	EnableLogicalChecks     bool
	EnableAnomalyDetection  bool
	EnableTimestampChecks   bool
}

// ValidationResults represents the results of data validation
type ValidationResults struct {
	ValidationErrors   []ValidationError
	AnomalyDetections  []AnomalyDetection
	QualityMetrics     *DataQualityMetrics
}

// ValidationError represents a data validation error
type ValidationError struct {
	CandleIndex int
	ErrorType   string
	Message     string
	Severity    string
}

// AnomalyDetection represents an detected anomaly
type AnomalyDetection struct {
	CandleIndex   int
	AnomalyType   string
	Severity      string
	Confidence    float64
	Description   string
}

// DataQualityMetrics represents data quality metrics
type DataQualityMetrics struct {
	TotalCandles       int64
	ValidCandles       int64
	ValidationErrors   int64
	AnomaliesDetected  int64
	QualityScore       float64
	ErrorsByType       map[string]int64
	AnomaliesByType    map[string]int64
	ProcessingTimeMs   int64
	ProcessedAt        time.Time
}

// Mock interfaces for testing (these will fail until implemented)

// DataValidator interface - WILL FAIL until implemented
type DataValidator interface {
	ValidateCandles(ctx context.Context, candles []contracts.Candle) (*ValidationResults, error)
}

// NewDataValidator creates a new data validator - WILL FAIL until implemented
func NewDataValidator(config *ValidationConfig) (DataValidator, error) {
	return nil, fmt.Errorf("DataValidator not implemented - TDD placeholder")
}

// MockDataCollector for testing - WILL FAIL until implemented
type MockDataCollector interface {
	CollectData(ctx context.Context, req *DataCollectionRequest) ([]contracts.Candle, error)
}

// DataCollectionRequest represents a data collection request
type DataCollectionRequest struct {
	Pair      string
	Interval  string
	StartTime time.Time
	EndTime   time.Time
}

// NewMockDataCollector creates a mock data collector - WILL FAIL until implemented
func NewMockDataCollector() (MockDataCollector, error) {
	return nil, fmt.Errorf("MockDataCollector not implemented - TDD placeholder")
}

// ValidationMockStorage for testing - WILL FAIL until implemented
type ValidationMockStorage interface {
	Store(ctx context.Context, candles []contracts.Candle) error
}

// NewValidationMockStorage creates a mock storage - WILL FAIL until implemented
func NewValidationMockStorage() ValidationMockStorage {
	return nil // This will fail until implemented - maintaining TDD approach
}

// ValidationPipeline combines data collection, validation, and storage
type ValidationPipeline interface {
	ProcessData(ctx context.Context, req *DataCollectionRequest) (*ValidationPipelineResults, error)
}

// ValidationPipelineConfig configures the validation pipeline
type ValidationPipelineConfig struct {
	Collector MockDataCollector
	Validator DataValidator
	Storage   ValidationMockStorage
	BatchSize int
}

// ValidationPipelineResults represents pipeline processing results
type ValidationPipelineResults struct {
	ProcessedCandles int64
	ValidCandles     int64
	InvalidCandles   int64
	QualityMetrics   *DataQualityMetrics
}

// NewValidationPipeline creates a new validation pipeline - WILL FAIL until implemented
func NewValidationPipeline(config *ValidationPipelineConfig) (ValidationPipeline, error) {
	return nil, fmt.Errorf("ValidationPipeline not implemented - TDD placeholder")
}

// Helper functions for creating test data

func createTestCandleData() []contracts.Candle {
	now := time.Now().Truncate(time.Hour)
	return []contracts.Candle{
		// Valid candles
		createCandle("100.00", "105.00", "98.00", "103.00", "1000.00", now.Add(-5*time.Hour)),
		createCandle("103.00", "108.00", "101.00", "106.00", "1100.00", now.Add(-4*time.Hour)),
		createCandle("106.00", "111.00", "104.00", "109.00", "1200.00", now.Add(-3*time.Hour)),
		
		// Invalid logical consistency
		createCandle("109.00", "107.00", "102.00", "105.00", "1300.00", now.Add(-2*time.Hour)), // High < Open
		
		// Price anomaly (500%+ spike)
		createCandle("105.00", "630.00", "105.00", "625.00", "1400.00", now.Add(-time.Hour)),
		
		// Volume anomaly (10x+ surge)
		createCandle("625.00", "630.00", "620.00", "628.00", "15000.00", now),
	}
}

func createCandle(open, high, low, close, volume string, timestamp time.Time) contracts.Candle {
	return contracts.Candle{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
		Pair:      "BTC-USD",
		Interval:  "1h",
	}
}

func createCandleAtTime(close string, timestamp time.Time) contracts.Candle {
	// Create a candle with consistent OHLC values at specified time
	price := decimal.RequireFromString(close)
	variation := decimal.NewFromFloat(2.0)
	
	return contracts.Candle{
		Timestamp: timestamp,
		Open:      price.String(),
		High:      price.Add(variation).String(),
		Low:       price.Sub(variation).String(),
		Close:     close,
		Volume:    "1000.00",
		Pair:      "BTC-USD",
		Interval:  "1h",
	}
}