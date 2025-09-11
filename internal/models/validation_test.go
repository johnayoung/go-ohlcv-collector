package models

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestNewValidationResult(t *testing.T) {
	id := "test-id"
	pair := "BTC-USD"
	candleTime := time.Now().UTC()

	result := NewValidationResult(id, pair, candleTime)

	assert.Equal(t, id, result.ID)
	assert.Equal(t, pair, result.Pair)
	assert.Equal(t, candleTime, result.CandleTimestamp)
	assert.NotZero(t, result.ValidationTime)
	assert.Empty(t, result.ChecksPassed)
	assert.Empty(t, result.Anomalies)
	assert.Equal(t, SeverityInfo, result.Severity)
	assert.Equal(t, ActionNone, result.ActionTaken)
}

func TestValidationResultBuilder(t *testing.T) {
	id := "test-id"
	pair := "BTC-USD"
	candleTime := time.Now().UTC()

	anomaly := *NewAnomaly(AnomalyTypePriceSpike, "Test spike", decimal.NewFromFloat(2.0), decimal.NewFromFloat(1.5), 0.9)

	result := NewValidationResultBuilder(id, pair, candleTime).
		AddPassedCheck("basic_validation").
		AddPassedCheck("timestamp_validation").
		AddAnomaly(anomaly).
		SetActionTaken(ActionAlerted).
		Build()

	assert.Equal(t, id, result.ID)
	assert.Equal(t, pair, result.Pair)
	assert.Len(t, result.ChecksPassed, 2)
	assert.Contains(t, result.ChecksPassed, "basic_validation")
	assert.Contains(t, result.ChecksPassed, "timestamp_validation")
	assert.Len(t, result.Anomalies, 1)
	assert.Equal(t, AnomalyTypePriceSpike, result.Anomalies[0].Type)
	assert.Equal(t, ActionAlerted, result.ActionTaken)
	// Severity should be escalated based on anomaly
	assert.True(t, result.HasSeverity(SeverityWarning))
}

func TestNewAnomaly(t *testing.T) {
	anomalyType := AnomalyTypePriceSpike
	description := "Price spike detected"
	value := decimal.NewFromFloat(6.0)
	threshold := decimal.NewFromFloat(5.0)
	confidence := 0.85

	anomaly := NewAnomaly(anomalyType, description, value, threshold, confidence)

	assert.Equal(t, anomalyType, anomaly.Type)
	assert.Equal(t, description, anomaly.Description)
	assert.True(t, value.Equal(anomaly.Value))
	assert.True(t, threshold.Equal(anomaly.Threshold))
	assert.Equal(t, confidence, anomaly.Confidence)
	assert.NotEqual(t, SeverityInfo, anomaly.Severity) // Should be elevated
}

func TestCreatePriceSpikeAnomaly(t *testing.T) {
	currentPrice := decimal.NewFromFloat(600.0)
	previousPrice := decimal.NewFromFloat(100.0)

	anomaly := CreatePriceSpikeAnomaly(currentPrice, previousPrice)

	assert.Equal(t, AnomalyTypePriceSpike, anomaly.Type)
	assert.Contains(t, anomaly.Description, "Price spike detected")
	assert.Contains(t, anomaly.Description, "500.00%")
	assert.True(t, anomaly.Value.Equal(decimal.NewFromFloat(6.0))) // 6x ratio
	assert.True(t, anomaly.Threshold.Equal(decimal.NewFromFloat(5.0)))
	assert.Greater(t, anomaly.Confidence, 0.0)
}

func TestCreateVolumeSurgeAnomaly(t *testing.T) {
	currentVolume := decimal.NewFromFloat(15000.0)
	previousVolume := decimal.NewFromFloat(1000.0)

	anomaly := CreateVolumeSurgeAnomaly(currentVolume, previousVolume)

	assert.Equal(t, AnomalyTypeVolumeSurge, anomaly.Type)
	assert.Contains(t, anomaly.Description, "Volume surge detected")
	assert.Contains(t, anomaly.Description, "15.00x")
	assert.True(t, anomaly.Value.Equal(decimal.NewFromFloat(15.0))) // 15x ratio
	assert.True(t, anomaly.Threshold.Equal(decimal.NewFromFloat(10.0)))
	assert.Greater(t, anomaly.Confidence, 0.0)
}

func TestDetectPriceSpike(t *testing.T) {
	threshold := decimal.NewFromFloat(5.0) // 500%

	tests := []struct {
		name         string
		currentHigh  decimal.Decimal
		previousHigh decimal.Decimal
		expected     bool
	}{
		{
			name:         "no_spike",
			currentHigh:  decimal.NewFromFloat(105.0),
			previousHigh: decimal.NewFromFloat(100.0),
			expected:     false,
		},
		{
			name:         "exact_threshold",
			currentHigh:  decimal.NewFromFloat(500.0),
			previousHigh: decimal.NewFromFloat(100.0),
			expected:     false, // Equal to threshold, not greater
		},
		{
			name:         "spike_detected",
			currentHigh:  decimal.NewFromFloat(600.0),
			previousHigh: decimal.NewFromFloat(100.0),
			expected:     true,
		},
		{
			name:         "zero_previous_price",
			currentHigh:  decimal.NewFromFloat(100.0),
			previousHigh: decimal.Zero,
			expected:     false, // Should handle zero gracefully
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectPriceSpike(tt.currentHigh, tt.previousHigh, threshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectVolumeSurge(t *testing.T) {
	threshold := decimal.NewFromFloat(10.0) // 10x

	tests := []struct {
		name            string
		currentVolume   decimal.Decimal
		previousVolume  decimal.Decimal
		expected        bool
	}{
		{
			name:           "no_surge",
			currentVolume:  decimal.NewFromFloat(1500.0),
			previousVolume: decimal.NewFromFloat(1000.0),
			expected:       false,
		},
		{
			name:           "exact_threshold",
			currentVolume:  decimal.NewFromFloat(10000.0),
			previousVolume: decimal.NewFromFloat(1000.0),
			expected:       false, // Equal to threshold, not greater
		},
		{
			name:           "surge_detected",
			currentVolume:  decimal.NewFromFloat(15000.0),
			previousVolume: decimal.NewFromFloat(1000.0),
			expected:       true,
		},
		{
			name:           "zero_previous_volume",
			currentVolume:  decimal.NewFromFloat(1000.0),
			previousVolume: decimal.Zero,
			expected:       false, // Should handle zero gracefully
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectVolumeSurge(tt.currentVolume, tt.previousVolume, threshold)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateOHLCVLogic(t *testing.T) {
	tests := []struct {
		name            string
		open            decimal.Decimal
		high            decimal.Decimal
		low             decimal.Decimal
		close           decimal.Decimal
		volume          decimal.Decimal
		expectedErrors  int
		expectedTypes   []string
	}{
		{
			name:           "valid_candle",
			open:           decimal.NewFromFloat(100.0),
			high:           decimal.NewFromFloat(105.0),
			low:            decimal.NewFromFloat(98.0),
			close:          decimal.NewFromFloat(103.0),
			volume:         decimal.NewFromFloat(1500.0),
			expectedErrors: 0,
		},
		{
			name:            "high_less_than_open",
			open:            decimal.NewFromFloat(100.0),
			high:            decimal.NewFromFloat(95.0), // Invalid
			low:             decimal.NewFromFloat(90.0),
			close:           decimal.NewFromFloat(93.0),
			volume:          decimal.NewFromFloat(1500.0),
			expectedErrors:  1,
			expectedTypes:   []string{"high_validation"},
		},
		{
			name:            "low_greater_than_close",
			open:            decimal.NewFromFloat(98.0),
			high:            decimal.NewFromFloat(105.0),
			low:             decimal.NewFromFloat(99.0), // Invalid
			close:           decimal.NewFromFloat(97.0),
			volume:          decimal.NewFromFloat(1500.0),
			expectedErrors:  1,
			expectedTypes:   []string{"low_validation"},
		},
		{
			name:            "negative_volume",
			open:            decimal.NewFromFloat(100.0),
			high:            decimal.NewFromFloat(105.0),
			low:             decimal.NewFromFloat(95.0),
			close:           decimal.NewFromFloat(103.0),
			volume:          decimal.NewFromFloat(-100.0), // Invalid
			expectedErrors:  1,
			expectedTypes:   []string{"non_negative_volume"},
		},
		{
			name:            "multiple_errors",
			open:            decimal.NewFromFloat(100.0),
			high:            decimal.NewFromFloat(95.0),  // Invalid: high < open
			low:             decimal.NewFromFloat(105.0), // Invalid: low > high and low > open
			close:           decimal.NewFromFloat(97.0),
			volume:          decimal.NewFromFloat(-50.0), // Invalid: negative volume
			expectedErrors:  3,
			expectedTypes:   []string{"high_validation", "low_validation", "non_negative_volume"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			anomalies := ValidateOHLCVLogic(tt.open, tt.high, tt.low, tt.close, tt.volume)
			
			assert.Len(t, anomalies, tt.expectedErrors, "Number of anomalies should match")
			
			// Check that all expected error types are present
			for _, expectedType := range tt.expectedTypes {
				found := false
				for _, anomaly := range anomalies {
					if anomaly.Type == AnomalyTypeLogicError && 
					   len(anomaly.Description) > 0 && 
					   anomaly.Description[:len(expectedType)] == expectedType {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find error type: %s", expectedType)
			}
		})
	}
}

func TestValidateTimestampSequence(t *testing.T) {
	baseTime := time.Now().Truncate(time.Hour)
	interval := time.Hour
	tolerance := 2 * time.Minute

	tests := []struct {
		name           string
		timestamps     []time.Time
		expectedErrors int
	}{
		{
			name: "valid_sequence",
			timestamps: []time.Time{
				baseTime,
				baseTime.Add(interval),
				baseTime.Add(2 * interval),
			},
			expectedErrors: 0,
		},
		{
			name: "out_of_order",
			timestamps: []time.Time{
				baseTime.Add(interval),
				baseTime, // Out of order
				baseTime.Add(2 * interval),
			},
			expectedErrors: 2, // Out of order + gap from recovery
		},
		{
			name: "excessive_drift",
			timestamps: []time.Time{
				baseTime,
				baseTime.Add(interval + 5*time.Minute), // >2min drift
			},
			expectedErrors: 1,
		},
		{
			name: "within_tolerance",
			timestamps: []time.Time{
				baseTime,
				baseTime.Add(interval + 90*time.Second), // <2min drift
			},
			expectedErrors: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			anomalies := ValidateTimestampSequence(tt.timestamps, interval, tolerance)
			assert.Len(t, anomalies, tt.expectedErrors)
		})
	}
}

func TestDetermineSeverity(t *testing.T) {
	tests := []struct {
		name         string
		anomalyType  AnomalyType
		value        decimal.Decimal
		threshold    decimal.Decimal
		confidence   float64
		expected     SeverityLevel
	}{
		{
			name:        "logic_error_always_severe",
			anomalyType: AnomalyTypeLogicError,
			value:       decimal.NewFromFloat(1.0),
			threshold:   decimal.NewFromFloat(1.0),
			confidence:  0.5,
			expected:    SeverityError,
		},
		{
			name:        "price_spike_critical",
			anomalyType: AnomalyTypePriceSpike,
			value:       decimal.NewFromFloat(15.0), // High ratio (15/5=3 > 2)
			threshold:   decimal.NewFromFloat(5.0),
			confidence:  0.95, // High confidence
			expected:    SeverityCritical,
		},
		{
			name:        "price_spike_warning",
			anomalyType: AnomalyTypePriceSpike,
			value:       decimal.NewFromFloat(6.0),
			threshold:   decimal.NewFromFloat(5.0),
			confidence:  0.6,
			expected:    SeverityWarning,
		},
		{
			name:        "volume_surge_critical",
			anomalyType: AnomalyTypeVolumeSurge,
			value:       decimal.NewFromFloat(60.0), // High ratio (60/10=6 > 5)
			threshold:   decimal.NewFromFloat(10.0),
			confidence:  0.95, // High confidence
			expected:    SeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetermineSeverity(tt.anomalyType, tt.value, tt.threshold, tt.confidence)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidationResultValidate(t *testing.T) {
	validResult := NewValidationResult("test-id", "BTC-USD", time.Now())

	tests := []struct {
		name        string
		modifier    func(*ValidationResult)
		shouldError bool
	}{
		{
			name:        "valid_result",
			modifier:    func(vr *ValidationResult) {},
			shouldError: false,
		},
		{
			name:        "empty_id",
			modifier:    func(vr *ValidationResult) { vr.ID = "" },
			shouldError: true,
		},
		{
			name:        "empty_pair",
			modifier:    func(vr *ValidationResult) { vr.Pair = "" },
			shouldError: true,
		},
		{
			name:        "zero_candle_timestamp",
			modifier:    func(vr *ValidationResult) { vr.CandleTimestamp = time.Time{} },
			shouldError: true,
		},
		{
			name:        "invalid_severity",
			modifier:    func(vr *ValidationResult) { vr.Severity = SeverityLevel("invalid") },
			shouldError: true,
		},
		{
			name:        "invalid_action",
			modifier:    func(vr *ValidationResult) { vr.ActionTaken = ActionType("invalid") },
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := *validResult // Copy
			tt.modifier(&result)

			err := result.Validate()
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAnomalyValidate(t *testing.T) {
	validAnomaly := NewAnomaly(AnomalyTypePriceSpike, "Test", decimal.NewFromInt(1), decimal.NewFromInt(1), 0.8)

	tests := []struct {
		name        string
		modifier    func(*Anomaly)
		shouldError bool
	}{
		{
			name:        "valid_anomaly",
			modifier:    func(a *Anomaly) {},
			shouldError: false,
		},
		{
			name:        "invalid_type",
			modifier:    func(a *Anomaly) { a.Type = AnomalyType("invalid") },
			shouldError: true,
		},
		{
			name:        "empty_description",
			modifier:    func(a *Anomaly) { a.Description = "" },
			shouldError: true,
		},
		{
			name:        "confidence_too_low",
			modifier:    func(a *Anomaly) { a.Confidence = -0.1 },
			shouldError: true,
		},
		{
			name:        "confidence_too_high",
			modifier:    func(a *Anomaly) { a.Confidence = 1.1 },
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			anomaly := *validAnomaly // Copy
			tt.modifier(&anomaly)

			err := anomaly.Validate()
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAggregateValidationResults(t *testing.T) {
	// Create test results
	result1 := NewValidationResultBuilder("id1", "BTC-USD", time.Now()).
		AddPassedCheck("check1").
		AddAnomaly(*NewAnomaly(AnomalyTypePriceSpike, "spike", decimal.NewFromInt(1), decimal.NewFromInt(1), 0.8)).
		Build()

	result2 := NewValidationResultBuilder("id2", "ETH-USD", time.Now()).
		AddPassedCheck("check1").
		AddPassedCheck("check2").
		AddAnomaly(*NewAnomaly(AnomalyTypeVolumeSurge, "surge", decimal.NewFromInt(1), decimal.NewFromInt(1), 0.9)).
		Build()

	results := []*ValidationResult{result1, result2}
	
	summary := AggregateValidationResults(results)
	
	assert.Equal(t, 2, summary.TotalResults)
	assert.Equal(t, 2, summary.TotalAnomalies)
	assert.Equal(t, 3, summary.TotalChecks) // 1 + 2
	assert.GreaterOrEqual(t, summary.QualityScore, 0.0)
	assert.LessOrEqual(t, summary.QualityScore, 1.0)
	assert.Equal(t, 1, summary.AnomalyBreakdown[AnomalyTypePriceSpike])
	assert.Equal(t, 1, summary.AnomalyBreakdown[AnomalyTypeVolumeSurge])
	assert.NotZero(t, summary.ProcessedAt)
}

func TestValidationResultHelperMethods(t *testing.T) {
	anomaly1 := *NewAnomaly(AnomalyTypePriceSpike, "spike", decimal.NewFromInt(1), decimal.NewFromInt(1), 0.9)
	anomaly2 := *NewAnomaly(AnomalyTypeVolumeSurge, "surge", decimal.NewFromInt(1), decimal.NewFromInt(1), 0.7)
	anomaly2.Severity = SeverityCritical

	result := NewValidationResultBuilder("id", "BTC-USD", time.Now()).
		AddAnomaly(anomaly1).
		AddAnomaly(anomaly2).
		Build()

	// Test HasSeverity
	assert.True(t, result.HasSeverity(SeverityWarning))
	assert.True(t, result.HasSeverity(SeverityCritical))
	
	// Create a result with lower severity to test false case
	lowSeverityResult := NewValidationResult("test", "BTC-USD", time.Now())
	lowSeverityResult.Severity = SeverityInfo
	assert.False(t, lowSeverityResult.HasSeverity(SeverityWarning))

	// Test GetAnomaliesByType
	priceAnomalies := result.GetAnomaliesByType(AnomalyTypePriceSpike)
	assert.Len(t, priceAnomalies, 1)

	volumeAnomalies := result.GetAnomaliesByType(AnomalyTypeVolumeSurge)
	assert.Len(t, volumeAnomalies, 1)

	// Test GetHighConfidenceAnomalies
	highConfidence := result.GetHighConfidenceAnomalies(0.8)
	assert.Len(t, highConfidence, 1) // Only anomaly1 has confidence >= 0.8

	// Test RequiresEscalation
	assert.True(t, result.RequiresEscalation()) // Has critical severity

	// Test GetActionRecommendation
	assert.Equal(t, ActionRejected, result.GetActionRecommendation()) // Critical severity

	// Test IsAcceptable
	assert.False(t, result.IsAcceptable()) // Has critical severity
}