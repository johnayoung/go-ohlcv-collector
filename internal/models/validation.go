// Package models provides data structures and validation logic for OHLCV data quality assessment.
//
// This package implements ValidationResult and Anomaly models as specified in the data model,
// with methods for anomaly detection and classification including:
// - Price spikes (500% threshold from research.md)
// - Volume surges (10x threshold from research.md)
// - Logic errors (OHLCV relationship violations)
// - Sequence gaps and timestamp validation
//
// The models include severity level management, escalation, confidence scoring,
// and comprehensive validation with meaningful error messages.
package models

import (
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// SeverityLevel represents the severity of a validation result or anomaly
type SeverityLevel string

const (
	SeverityInfo     SeverityLevel = "info"
	SeverityWarning  SeverityLevel = "warning"
	SeverityError    SeverityLevel = "error"
	SeverityCritical SeverityLevel = "critical"
)

// AnomalyType represents the type of anomaly detected
type AnomalyType string

const (
	AnomalyTypePriceSpike  AnomalyType = "price_spike"
	AnomalyTypeVolumeSurge AnomalyType = "volume_surge"
	AnomalyTypeLogicError  AnomalyType = "logic_error"
	AnomalyTypeSequenceGap AnomalyType = "sequence_gap"
)

// ActionType represents the action taken in response to validation
type ActionType string

const (
	ActionNone        ActionType = "none"
	ActionLogged      ActionType = "logged"
	ActionAlerted     ActionType = "alerted"
	ActionQuarantined ActionType = "quarantined"
	ActionRejected    ActionType = "rejected"
)

// ValidationResult tracks data quality validation outcomes
type ValidationResult struct {
	ID              string        `json:"id" db:"id"`
	CandleTimestamp time.Time     `json:"candle_timestamp" db:"candle_timestamp"`
	Pair            string        `json:"pair" db:"pair"`
	ValidationTime  time.Time     `json:"validation_time" db:"validation_time"`
	ChecksPassed    []string      `json:"checks_passed" db:"checks_passed"`
	Anomalies       []Anomaly     `json:"anomalies" db:"anomalies"`
	Severity        SeverityLevel `json:"severity" db:"severity"`
	ActionTaken     ActionType    `json:"action_taken" db:"action_taken"`
}

// Anomaly represents a detected data quality issue
type Anomaly struct {
	Type        AnomalyType     `json:"type" db:"type"`
	Description string          `json:"description" db:"description"`
	Value       decimal.Decimal `json:"value" db:"value"`
	Threshold   decimal.Decimal `json:"threshold" db:"threshold"`
	Confidence  float64         `json:"confidence" db:"confidence"`
	Severity    SeverityLevel   `json:"severity" db:"severity"`
}

// ValidationResultBuilder helps construct ValidationResult instances
type ValidationResultBuilder struct {
	result *ValidationResult
}

// NewValidationResult creates a new ValidationResult with required fields
func NewValidationResult(id, pair string, candleTimestamp time.Time) *ValidationResult {
	return &ValidationResult{
		ID:              id,
		CandleTimestamp: candleTimestamp,
		Pair:            pair,
		ValidationTime:  time.Now().UTC(),
		ChecksPassed:    make([]string, 0),
		Anomalies:       make([]Anomaly, 0),
		Severity:        SeverityInfo,
		ActionTaken:     ActionNone,
	}
}

// NewValidationResultBuilder creates a builder for ValidationResult
func NewValidationResultBuilder(id, pair string, candleTimestamp time.Time) *ValidationResultBuilder {
	return &ValidationResultBuilder{
		result: NewValidationResult(id, pair, candleTimestamp),
	}
}

// AddPassedCheck adds a successful validation check
func (vrb *ValidationResultBuilder) AddPassedCheck(checkName string) *ValidationResultBuilder {
	vrb.result.ChecksPassed = append(vrb.result.ChecksPassed, checkName)
	return vrb
}

// AddAnomaly adds an anomaly to the validation result
func (vrb *ValidationResultBuilder) AddAnomaly(anomaly Anomaly) *ValidationResultBuilder {
	vrb.result.Anomalies = append(vrb.result.Anomalies, anomaly)
	vrb.escalateSeverity(anomaly.Severity)
	return vrb
}

// SetActionTaken sets the action taken in response to validation
func (vrb *ValidationResultBuilder) SetActionTaken(action ActionType) *ValidationResultBuilder {
	vrb.result.ActionTaken = action
	return vrb
}

// Build returns the constructed ValidationResult
func (vrb *ValidationResultBuilder) Build() *ValidationResult {
	return vrb.result
}

// escalateSeverity updates the overall severity based on anomaly severity
func (vrb *ValidationResultBuilder) escalateSeverity(anomalySeverity SeverityLevel) {
	currentSeverity := vrb.result.Severity
	if shouldEscalateSeverity(currentSeverity, anomalySeverity) {
		vrb.result.Severity = anomalySeverity
	}
}

// NewAnomaly creates a new Anomaly instance
func NewAnomaly(anomalyType AnomalyType, description string, value, threshold decimal.Decimal, confidence float64) *Anomaly {
	return &Anomaly{
		Type:        anomalyType,
		Description: description,
		Value:       value,
		Threshold:   threshold,
		Confidence:  confidence,
		Severity:    DetermineSeverity(anomalyType, value, threshold, confidence),
	}
}

// CreatePriceSpikeAnomaly creates a price spike anomaly with 500% threshold from research.md
func CreatePriceSpikeAnomaly(currentPrice, previousPrice decimal.Decimal) *Anomaly {
	threshold := decimal.NewFromFloat(5.0) // 500% threshold
	spikeRatio := currentPrice.Div(previousPrice)
	confidence := calculatePriceSpikeConfidence(spikeRatio, threshold)

	description := fmt.Sprintf("Price spike detected: %.2f%% increase (current: %s, previous: %s)",
		spikeRatio.Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100)).InexactFloat64(),
		currentPrice.String(),
		previousPrice.String())

	return NewAnomaly(AnomalyTypePriceSpike, description, spikeRatio, threshold, confidence)
}

// CreateVolumeSurgeAnomaly creates a volume surge anomaly with 10x threshold from research.md
func CreateVolumeSurgeAnomaly(currentVolume, previousVolume decimal.Decimal) *Anomaly {
	threshold := decimal.NewFromFloat(10.0) // 10x threshold
	surgeRatio := currentVolume.Div(previousVolume)
	confidence := calculateVolumeSurgeConfidence(surgeRatio, threshold)

	description := fmt.Sprintf("Volume surge detected: %.2fx increase (current: %s, previous: %s)",
		surgeRatio.InexactFloat64(),
		currentVolume.String(),
		previousVolume.String())

	return NewAnomaly(AnomalyTypeVolumeSurge, description, surgeRatio, threshold, confidence)
}

// CreateLogicErrorAnomaly creates a logic error anomaly
func CreateLogicErrorAnomaly(errorType, description string, violatingValue decimal.Decimal) *Anomaly {
	confidence := 1.0         // Logic errors are always certain
	threshold := decimal.Zero // Not applicable for logic errors

	return NewAnomaly(AnomalyTypeLogicError, fmt.Sprintf("%s: %s", errorType, description), violatingValue, threshold, confidence)
}

// CreateSequenceGapAnomaly creates a sequence gap anomaly
func CreateSequenceGapAnomaly(expectedTime, actualTime time.Time, toleranceDuration time.Duration) *Anomaly {
	gap := actualTime.Sub(expectedTime)
	gapMinutes := gap.Minutes()
	toleranceMinutes := toleranceDuration.Minutes()

	confidence := math.Min(gapMinutes/toleranceMinutes, 1.0)
	description := fmt.Sprintf("Timestamp sequence gap detected: %.2f minutes gap (expected: %s, actual: %s)",
		gapMinutes, expectedTime.Format(time.RFC3339), actualTime.Format(time.RFC3339))

	return NewAnomaly(AnomalyTypeSequenceGap, description, decimal.NewFromFloat(gapMinutes), decimal.NewFromFloat(toleranceMinutes), confidence)
}

// DetectPriceSpike detects if there's a significant price spike
func DetectPriceSpike(currentHigh, previousHigh decimal.Decimal, threshold decimal.Decimal) bool {
	if previousHigh.IsZero() {
		return false
	}

	ratio := currentHigh.Div(previousHigh)
	return ratio.GreaterThan(threshold)
}

// DetectVolumeSurge detects if there's a significant volume surge
func DetectVolumeSurge(currentVolume, previousVolume decimal.Decimal, threshold decimal.Decimal) bool {
	if previousVolume.IsZero() {
		return false
	}

	ratio := currentVolume.Div(previousVolume)
	return ratio.GreaterThan(threshold)
}

// ValidateOHLCVLogic validates OHLCV logical consistency rules
func ValidateOHLCVLogic(open, high, low, close, volume decimal.Decimal) []Anomaly {
	var anomalies []Anomaly

	// Rule: High >= max(Open, Close)
	maxOpenClose := decimal.Max(open, close)
	if high.LessThan(maxOpenClose) {
		anomaly := CreateLogicErrorAnomaly("high_validation",
			fmt.Sprintf("High (%.8s) is less than max of Open (%.8s) and Close (%.8s)",
				high.String(), open.String(), close.String()), high)
		anomalies = append(anomalies, *anomaly)
	}

	// Rule: Low <= min(Open, Close)
	minOpenClose := decimal.Min(open, close)
	if low.GreaterThan(minOpenClose) {
		anomaly := CreateLogicErrorAnomaly("low_validation",
			fmt.Sprintf("Low (%.8s) is greater than min of Open (%.8s) and Close (%.8s)",
				low.String(), open.String(), close.String()), low)
		anomalies = append(anomalies, *anomaly)
	}

	// Rule: All prices > 0
	if open.LessThanOrEqual(decimal.Zero) {
		anomaly := CreateLogicErrorAnomaly("positive_price",
			fmt.Sprintf("Open price must be positive, got: %s", open.String()), open)
		anomalies = append(anomalies, *anomaly)
	}
	if high.LessThanOrEqual(decimal.Zero) {
		anomaly := CreateLogicErrorAnomaly("positive_price",
			fmt.Sprintf("High price must be positive, got: %s", high.String()), high)
		anomalies = append(anomalies, *anomaly)
	}
	if low.LessThanOrEqual(decimal.Zero) {
		anomaly := CreateLogicErrorAnomaly("positive_price",
			fmt.Sprintf("Low price must be positive, got: %s", low.String()), low)
		anomalies = append(anomalies, *anomaly)
	}
	if close.LessThanOrEqual(decimal.Zero) {
		anomaly := CreateLogicErrorAnomaly("positive_price",
			fmt.Sprintf("Close price must be positive, got: %s", close.String()), close)
		anomalies = append(anomalies, *anomaly)
	}

	// Rule: Volume >= 0
	if volume.LessThan(decimal.Zero) {
		anomaly := CreateLogicErrorAnomaly("non_negative_volume",
			fmt.Sprintf("Volume must be non-negative, got: %s", volume.String()), volume)
		anomalies = append(anomalies, *anomaly)
	}

	return anomalies
}

// ValidateTimestampSequence validates timestamp ordering and drift tolerance
func ValidateTimestampSequence(timestamps []time.Time, interval time.Duration, driftTolerance time.Duration) []Anomaly {
	var anomalies []Anomaly

	for i := 1; i < len(timestamps); i++ {
		current := timestamps[i]
		previous := timestamps[i-1]

		// Check ordering
		if current.Before(previous) {
			anomaly := CreateSequenceGapAnomaly(previous.Add(interval), current, driftTolerance)
			anomaly.Type = AnomalyTypeSequenceGap
			anomaly.Description = fmt.Sprintf("Timestamp out of order: %s comes after %s",
				current.Format(time.RFC3339), previous.Format(time.RFC3339))
			anomalies = append(anomalies, *anomaly)
			continue // Skip further checks for this pair if out of order
		}

		// Check expected interval
		expectedTime := previous.Add(interval)
		actualGap := current.Sub(expectedTime)

		if actualGap.Abs() > driftTolerance {
			anomaly := CreateSequenceGapAnomaly(expectedTime, current, driftTolerance)
			anomalies = append(anomalies, *anomaly)
		}
	}

	return anomalies
}

// DetermineSeverity determines severity based on anomaly type and confidence
func DetermineSeverity(anomalyType AnomalyType, value, threshold decimal.Decimal, confidence float64) SeverityLevel {
	switch anomalyType {
	case AnomalyTypeLogicError:
		return SeverityError // Logic errors are always severe
	case AnomalyTypePriceSpike:
		if !threshold.IsZero() {
			ratio := value.Div(threshold)
			if confidence >= 0.9 && ratio.GreaterThan(decimal.NewFromFloat(2.0)) {
				return SeverityCritical
			}
		}
		if confidence >= 0.7 {
			return SeverityError
		} else {
			return SeverityWarning
		}
	case AnomalyTypeVolumeSurge:
		if !threshold.IsZero() {
			ratio := value.Div(threshold)
			if confidence >= 0.9 && ratio.GreaterThan(decimal.NewFromFloat(5.0)) {
				return SeverityCritical
			}
		}
		if confidence >= 0.7 {
			return SeverityError
		} else {
			return SeverityWarning
		}
	case AnomalyTypeSequenceGap:
		if confidence >= 0.8 {
			return SeverityError
		} else {
			return SeverityWarning
		}
	default:
		return SeverityInfo
	}
}

// shouldEscalateSeverity determines if severity should be escalated
func shouldEscalateSeverity(current, proposed SeverityLevel) bool {
	severityLevels := map[SeverityLevel]int{
		SeverityInfo:     1,
		SeverityWarning:  2,
		SeverityError:    3,
		SeverityCritical: 4,
	}

	return severityLevels[proposed] > severityLevels[current]
}

// calculatePriceSpikeConfidence calculates confidence score for price spikes
func calculatePriceSpikeConfidence(ratio, threshold decimal.Decimal) float64 {
	if ratio.LessThanOrEqual(threshold) {
		return 0.0
	}

	// Confidence increases with how much the threshold is exceeded
	excess := ratio.Sub(threshold).Div(threshold)
	confidence := math.Min(0.5+excess.InexactFloat64()*0.1, 1.0)
	return confidence
}

// calculateVolumeSurgeConfidence calculates confidence score for volume surges
func calculateVolumeSurgeConfidence(ratio, threshold decimal.Decimal) float64 {
	if ratio.LessThanOrEqual(threshold) {
		return 0.0
	}

	// Confidence increases with how much the threshold is exceeded
	excess := ratio.Sub(threshold).Div(threshold)
	confidence := math.Min(0.6+excess.InexactFloat64()*0.08, 1.0)
	return confidence
}

// AggregateValidationResults combines multiple validation results
func AggregateValidationResults(results []*ValidationResult) *ValidationResultSummary {
	if len(results) == 0 {
		return &ValidationResultSummary{}
	}

	summary := &ValidationResultSummary{
		TotalResults:      len(results),
		SeverityBreakdown: make(map[SeverityLevel]int),
		AnomalyBreakdown:  make(map[AnomalyType]int),
		ActionBreakdown:   make(map[ActionType]int),
		ProcessedAt:       time.Now().UTC(),
	}

	for _, result := range results {
		summary.SeverityBreakdown[result.Severity]++
		summary.ActionBreakdown[result.ActionTaken]++

		for _, anomaly := range result.Anomalies {
			summary.AnomalyBreakdown[anomaly.Type]++
			summary.TotalAnomalies++
		}

		summary.TotalChecks += len(result.ChecksPassed)
	}

	// Calculate quality score (0.0 to 1.0)
	if summary.TotalResults > 0 {
		errorCount := summary.SeverityBreakdown[SeverityError] + summary.SeverityBreakdown[SeverityCritical]
		summary.QualityScore = 1.0 - float64(errorCount)/float64(summary.TotalResults)
	}

	return summary
}

// ValidationResultSummary provides aggregated validation metrics
type ValidationResultSummary struct {
	TotalResults      int                   `json:"total_results"`
	TotalAnomalies    int                   `json:"total_anomalies"`
	TotalChecks       int                   `json:"total_checks"`
	QualityScore      float64               `json:"quality_score"`
	SeverityBreakdown map[SeverityLevel]int `json:"severity_breakdown"`
	AnomalyBreakdown  map[AnomalyType]int   `json:"anomaly_breakdown"`
	ActionBreakdown   map[ActionType]int    `json:"action_breakdown"`
	ProcessedAt       time.Time             `json:"processed_at"`
}

// Validate validates the ValidationResult structure
func (vr *ValidationResult) Validate() error {
	if vr.ID == "" {
		return fmt.Errorf("validation result ID is required")
	}
	if vr.Pair == "" {
		return fmt.Errorf("trading pair is required")
	}
	if vr.CandleTimestamp.IsZero() {
		return fmt.Errorf("candle timestamp is required")
	}
	if vr.ValidationTime.IsZero() {
		return fmt.Errorf("validation time is required")
	}

	// Validate severity level
	validSeverities := map[SeverityLevel]bool{
		SeverityInfo:     true,
		SeverityWarning:  true,
		SeverityError:    true,
		SeverityCritical: true,
	}
	if !validSeverities[vr.Severity] {
		return fmt.Errorf("invalid severity level: %s", vr.Severity)
	}

	// Validate action type
	validActions := map[ActionType]bool{
		ActionNone:        true,
		ActionLogged:      true,
		ActionAlerted:     true,
		ActionQuarantined: true,
		ActionRejected:    true,
	}
	if !validActions[vr.ActionTaken] {
		return fmt.Errorf("invalid action type: %s", vr.ActionTaken)
	}

	// Validate anomalies
	for i, anomaly := range vr.Anomalies {
		if err := anomaly.Validate(); err != nil {
			return fmt.Errorf("anomaly %d is invalid: %w", i, err)
		}
	}

	return nil
}

// Validate validates the Anomaly structure
func (a *Anomaly) Validate() error {
	// Validate anomaly type
	validTypes := map[AnomalyType]bool{
		AnomalyTypePriceSpike:  true,
		AnomalyTypeVolumeSurge: true,
		AnomalyTypeLogicError:  true,
		AnomalyTypeSequenceGap: true,
	}
	if !validTypes[a.Type] {
		return fmt.Errorf("invalid anomaly type: %s", a.Type)
	}

	if a.Description == "" {
		return fmt.Errorf("anomaly description is required")
	}

	if a.Confidence < 0.0 || a.Confidence > 1.0 {
		return fmt.Errorf("confidence must be between 0.0 and 1.0, got: %f", a.Confidence)
	}

	// Validate severity level
	validSeverities := map[SeverityLevel]bool{
		SeverityInfo:     true,
		SeverityWarning:  true,
		SeverityError:    true,
		SeverityCritical: true,
	}
	if !validSeverities[a.Severity] {
		return fmt.Errorf("invalid severity level: %s", a.Severity)
	}

	return nil
}

// HasSeverity checks if the validation result has a specific severity level or higher
func (vr *ValidationResult) HasSeverity(level SeverityLevel) bool {
	severityLevels := map[SeverityLevel]int{
		SeverityInfo:     1,
		SeverityWarning:  2,
		SeverityError:    3,
		SeverityCritical: 4,
	}

	return severityLevels[vr.Severity] >= severityLevels[level]
}

// GetAnomaliesByType returns anomalies filtered by type
func (vr *ValidationResult) GetAnomaliesByType(anomalyType AnomalyType) []Anomaly {
	var filtered []Anomaly
	for _, anomaly := range vr.Anomalies {
		if anomaly.Type == anomalyType {
			filtered = append(filtered, anomaly)
		}
	}
	return filtered
}

// GetHighConfidenceAnomalies returns anomalies with confidence above threshold
func (vr *ValidationResult) GetHighConfidenceAnomalies(minConfidence float64) []Anomaly {
	var filtered []Anomaly
	for _, anomaly := range vr.Anomalies {
		if anomaly.Confidence >= minConfidence {
			filtered = append(filtered, anomaly)
		}
	}
	return filtered
}

// RequiresEscalation determines if the validation result requires escalation
func (vr *ValidationResult) RequiresEscalation() bool {
	return vr.HasSeverity(SeverityError) || len(vr.GetHighConfidenceAnomalies(0.8)) > 0
}

// GetActionRecommendation recommends an action based on validation results
func (vr *ValidationResult) GetActionRecommendation() ActionType {
	if vr.HasSeverity(SeverityCritical) {
		return ActionRejected
	} else if vr.HasSeverity(SeverityError) {
		return ActionQuarantined
	} else if vr.HasSeverity(SeverityWarning) {
		return ActionAlerted
	} else if len(vr.Anomalies) > 0 {
		return ActionLogged
	}
	return ActionNone
}

// IsAcceptable determines if the validation result indicates acceptable data quality
func (vr *ValidationResult) IsAcceptable() bool {
	return !vr.HasSeverity(SeverityError) && len(vr.GetHighConfidenceAnomalies(0.9)) == 0
}
