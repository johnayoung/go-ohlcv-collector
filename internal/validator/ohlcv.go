// Package validator provides OHLCV data validation and anomaly detection implementation.
//
// This package implements the core validation interfaces for OHLCV data quality assessment:
// - OHLCVValidator: Main validator combining all validation functionality
// - DataValidator: Logical consistency checks for OHLCV relationships
// - AnomalyDetector: Price spikes, volume surges, and sequence gap detection
// - ValidationPipeline: Orchestrates complete validation workflows
//
// The implementation follows research findings with 500% price spike and 10x volume surge
// thresholds, comprehensive error handling, and financial precision using decimal arithmetic.
package validator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// OHLCVValidator provides comprehensive OHLCV data validation and anomaly detection.
// It implements all three core interfaces: DataValidator, AnomalyDetector, and ValidationPipeline.
type OHLCVValidator struct {
	config      *ValidationConfig
	thresholds  map[models.AnomalyType]decimal.Decimal
	status      *ValidationPipelineStatus
	logger      *slog.Logger
	mu          sync.RWMutex
}

// NewOHLCVValidator creates a new OHLCV validator with default configuration.
func NewOHLCVValidator(logger *slog.Logger) *OHLCVValidator {
	if logger == nil {
		logger = slog.Default()
	}

	validator := &OHLCVValidator{
		config: NewValidationConfig(),
		thresholds: map[models.AnomalyType]decimal.Decimal{
			models.AnomalyTypePriceSpike:  decimal.NewFromFloat(5.0),  // 500% from research.md
			models.AnomalyTypeVolumeSurge: decimal.NewFromFloat(10.0), // 10x from research.md
		},
		status: &ValidationPipelineStatus{
			IsProcessing: false,
		},
		logger: logger.With("component", "ohlcv_validator"),
	}

	return validator
}

// NewOHLCVValidatorWithConfig creates a new OHLCV validator with custom configuration.
func NewOHLCVValidatorWithConfig(config *ValidationConfig, logger *slog.Logger) *OHLCVValidator {
	validator := NewOHLCVValidator(logger)
	if config != nil {
		validator.config = config
		// Update thresholds based on config
		validator.thresholds[models.AnomalyTypePriceSpike] = decimal.NewFromFloat(config.PriceSpikeThreshold)
		validator.thresholds[models.AnomalyTypeVolumeSurge] = decimal.NewFromFloat(config.VolumeSurgeThreshold)
	}
	return validator
}

// DataValidator interface implementation

// ValidateCandles validates a slice of candle data and returns comprehensive validation results.
func (v *OHLCVValidator) ValidateCandles(ctx context.Context, candles []contracts.Candle) (*ValidationResults, error) {
	if len(candles) == 0 {
		return &ValidationResults{
			ValidationErrors:  []ValidationError{},
			AnomalyDetections: []AnomalyDetection{},
			QualityMetrics:    &DataQualityMetrics{},
			ProcessingTime:    0,
			ProcessedAt:       time.Now().UTC(),
		}, nil
	}

	startTime := time.Now()
	v.logger.Debug("Starting candle validation", "count", len(candles))

	var allErrors []ValidationError
	var allAnomalies []AnomalyDetection
	var validationResults []*models.ValidationResult

	// Validate each candle individually
	for i, candle := range candles {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		result, err := v.ValidateCandle(ctx, candle)
		if err != nil {
			v.logger.Error("Failed to validate candle", "index", i, "error", err)
			allErrors = append(allErrors, ValidationError{
				CandleIndex: i,
				ErrorType:   "validation_failure",
				Message:     fmt.Sprintf("Validation failed: %v", err),
				Severity:    "error",
				Field:       "candle",
				Value:       fmt.Sprintf("candle_%d", i),
			})
			continue
		}

		validationResults = append(validationResults, result)

		// Convert model anomalies to validator anomaly detections
		for _, anomaly := range result.Anomalies {
			allAnomalies = append(allAnomalies, AnomalyDetection{
				CandleIndex:  i,
				AnomalyType:  string(anomaly.Type),
				Severity:     string(anomaly.Severity),
				Confidence:   anomaly.Confidence,
				Description:  anomaly.Description,
				Value:        anomaly.Value,
				Threshold:    anomaly.Threshold,
				DetectedAt:   time.Now().UTC(),
			})
		}
	}

	// Perform cross-candle anomaly detection if enabled
	if v.config.EnableAnomalyDetection && len(candles) > 1 {
		crossCandleAnomalies, err := v.detectCrossCandleAnomalies(ctx, candles)
		if err != nil {
			v.logger.Error("Cross-candle anomaly detection failed", "error", err)
		} else {
			allAnomalies = append(allAnomalies, crossCandleAnomalies...)
		}
	}

	// Validate timestamp sequence if enabled
	if v.config.EnableTimestampChecks && len(candles) > 1 {
		timestamps := make([]time.Time, len(candles))
		for i, candle := range candles {
			timestamps[i] = candle.Timestamp
		}

		// Determine expected interval from candles
		expectedInterval := v.estimateInterval(timestamps)
		timestampAnomalies, err := v.ValidateTimestampSequence(ctx, timestamps, expectedInterval, v.config.TimestampDriftTolerance)
		if err != nil {
			v.logger.Error("Timestamp sequence validation failed", "error", err)
		} else {
			// Convert to AnomalyDetection format
			for i, anomaly := range timestampAnomalies {
				allAnomalies = append(allAnomalies, AnomalyDetection{
					CandleIndex:  i,
					AnomalyType:  string(anomaly.Type),
					Severity:     string(anomaly.Severity),
					Confidence:   anomaly.Confidence,
					Description:  anomaly.Description,
					Value:        anomaly.Value,
					Threshold:    anomaly.Threshold,
					DetectedAt:   time.Now().UTC(),
				})
			}
		}
	}

	// Calculate quality metrics
	qualityMetrics, err := v.GetQualityMetrics(ctx, validationResults)
	if err != nil {
		v.logger.Error("Failed to calculate quality metrics", "error", err)
		qualityMetrics = &DataQualityMetrics{}
	}

	processingTime := time.Since(startTime)
	v.logger.Debug("Completed candle validation",
		"count", len(candles),
		"errors", len(allErrors),
		"anomalies", len(allAnomalies),
		"processing_time", processingTime)

	return &ValidationResults{
		ValidationErrors:  allErrors,
		AnomalyDetections: allAnomalies,
		QualityMetrics:    qualityMetrics,
		ProcessingTime:    processingTime,
		ProcessedAt:       time.Now().UTC(),
	}, nil
}

// ValidateCandle validates a single candle for logical consistency.
func (v *OHLCVValidator) ValidateCandle(ctx context.Context, candle contracts.Candle) (*models.ValidationResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Parse decimal values with proper error handling
	open, err := decimal.NewFromString(candle.Open)
	if err != nil {
		return nil, fmt.Errorf("invalid open price %q: %w", candle.Open, err)
	}

	high, err := decimal.NewFromString(candle.High)
	if err != nil {
		return nil, fmt.Errorf("invalid high price %q: %w", candle.High, err)
	}

	low, err := decimal.NewFromString(candle.Low)
	if err != nil {
		return nil, fmt.Errorf("invalid low price %q: %w", candle.Low, err)
	}

	close, err := decimal.NewFromString(candle.Close)
	if err != nil {
		return nil, fmt.Errorf("invalid close price %q: %w", candle.Close, err)
	}

	volume, err := decimal.NewFromString(candle.Volume)
	if err != nil {
		return nil, fmt.Errorf("invalid volume %q: %w", candle.Volume, err)
	}

	// Create validation result
	result := models.NewValidationResult(
		fmt.Sprintf("%s_%d", candle.Pair, candle.Timestamp.Unix()),
		candle.Pair,
		candle.Timestamp,
	)

	// Perform logical consistency validation if enabled
	if v.config.EnableLogicalChecks {
		anomalies := models.ValidateOHLCVLogic(open, high, low, close, volume)
		for _, anomaly := range anomalies {
			result.Anomalies = append(result.Anomalies, anomaly)
		}

		if len(anomalies) == 0 {
			result.ChecksPassed = append(result.ChecksPassed, "ohlcv_logic")
		}
	}

	// Additional validation checks
	if candle.Timestamp.IsZero() {
		anomaly := models.CreateLogicErrorAnomaly("timestamp_validation",
			"Candle timestamp is zero or invalid", decimal.Zero)
		result.Anomalies = append(result.Anomalies, *anomaly)
	} else {
		result.ChecksPassed = append(result.ChecksPassed, "timestamp_valid")
	}

	if candle.Pair == "" {
		anomaly := models.CreateLogicErrorAnomaly("pair_validation",
			"Trading pair is empty", decimal.Zero)
		result.Anomalies = append(result.Anomalies, *anomaly)
	} else {
		result.ChecksPassed = append(result.ChecksPassed, "pair_valid")
	}

	// Update severity based on anomalies
	for _, anomaly := range result.Anomalies {
		if ShouldEscalateSeverity(result.Severity, anomaly.Severity) {
			result.Severity = anomaly.Severity
		}
	}

	return result, nil
}

// ValidateTimestampSequence validates timestamp ordering and drift tolerance.
func (v *OHLCVValidator) ValidateTimestampSequence(ctx context.Context, timestamps []time.Time, expectedInterval time.Duration, driftTolerance time.Duration) ([]models.Anomaly, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(timestamps) < 2 {
		return []models.Anomaly{}, nil
	}

	return models.ValidateTimestampSequence(timestamps, expectedInterval, driftTolerance), nil
}

// GetConfig returns the current validation configuration.
func (v *OHLCVValidator) GetConfig() *ValidationConfig {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	// Return a copy to prevent external modification
	configCopy := *v.config
	return &configCopy
}

// UpdateConfig updates the validation configuration with new settings.
func (v *OHLCVValidator) UpdateConfig(ctx context.Context, config *ValidationConfig) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.config = config
	
	// Update thresholds based on new config
	v.thresholds[models.AnomalyTypePriceSpike] = decimal.NewFromFloat(config.PriceSpikeThreshold)
	v.thresholds[models.AnomalyTypeVolumeSurge] = decimal.NewFromFloat(config.VolumeSurgeThreshold)

	v.logger.Info("Updated validation configuration",
		"price_spike_threshold", config.PriceSpikeThreshold,
		"volume_surge_threshold", config.VolumeSurgeThreshold,
		"timestamp_drift_tolerance", config.TimestampDriftTolerance)

	return nil
}

// AnomalyDetector interface implementation

// DetectPriceSpikes analyzes candle data for significant price movements.
func (v *OHLCVValidator) DetectPriceSpikes(ctx context.Context, candles []contracts.Candle, threshold decimal.Decimal) ([]models.Anomaly, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(candles) < 2 {
		return []models.Anomaly{}, nil
	}

	var anomalies []models.Anomaly

	for i := 1; i < len(candles); i++ {
		currentHigh, err := decimal.NewFromString(candles[i].High)
		if err != nil {
			v.logger.Error("Invalid high price for spike detection", "candle", i, "error", err)
			continue
		}

		previousHigh, err := decimal.NewFromString(candles[i-1].High)
		if err != nil {
			v.logger.Error("Invalid previous high price for spike detection", "candle", i-1, "error", err)
			continue
		}

		if models.DetectPriceSpike(currentHigh, previousHigh, threshold) {
			anomaly := models.CreatePriceSpikeAnomaly(currentHigh, previousHigh)
			anomalies = append(anomalies, *anomaly)
		}
	}

	return anomalies, nil
}

// DetectVolumeSurges analyzes candle data for unusual volume patterns.
func (v *OHLCVValidator) DetectVolumeSurges(ctx context.Context, candles []contracts.Candle, threshold decimal.Decimal) ([]models.Anomaly, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(candles) < 2 {
		return []models.Anomaly{}, nil
	}

	var anomalies []models.Anomaly

	for i := 1; i < len(candles); i++ {
		currentVolume, err := decimal.NewFromString(candles[i].Volume)
		if err != nil {
			v.logger.Error("Invalid volume for surge detection", "candle", i, "error", err)
			continue
		}

		previousVolume, err := decimal.NewFromString(candles[i-1].Volume)
		if err != nil {
			v.logger.Error("Invalid previous volume for surge detection", "candle", i-1, "error", err)
			continue
		}

		if models.DetectVolumeSurge(currentVolume, previousVolume, threshold) {
			anomaly := models.CreateVolumeSurgeAnomaly(currentVolume, previousVolume)
			anomalies = append(anomalies, *anomaly)
		}
	}

	return anomalies, nil
}

// DetectSequenceGaps analyzes timestamp sequences for gaps or irregularities.
func (v *OHLCVValidator) DetectSequenceGaps(ctx context.Context, timestamps []time.Time, expectedInterval time.Duration, tolerance time.Duration) ([]models.Anomaly, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return models.ValidateTimestampSequence(timestamps, expectedInterval, tolerance), nil
}

// DetectLogicalAnomalies analyzes OHLCV data for logical inconsistencies.
func (v *OHLCVValidator) DetectLogicalAnomalies(ctx context.Context, candle contracts.Candle) ([]models.Anomaly, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Parse decimal values
	open, err := decimal.NewFromString(candle.Open)
	if err != nil {
		return nil, fmt.Errorf("invalid open price: %w", err)
	}

	high, err := decimal.NewFromString(candle.High)
	if err != nil {
		return nil, fmt.Errorf("invalid high price: %w", err)
	}

	low, err := decimal.NewFromString(candle.Low)
	if err != nil {
		return nil, fmt.Errorf("invalid low price: %w", err)
	}

	close, err := decimal.NewFromString(candle.Close)
	if err != nil {
		return nil, fmt.Errorf("invalid close price: %w", err)
	}

	volume, err := decimal.NewFromString(candle.Volume)
	if err != nil {
		return nil, fmt.Errorf("invalid volume: %w", err)
	}

	return models.ValidateOHLCVLogic(open, high, low, close, volume), nil
}

// SetThresholds configures detection thresholds for different anomaly types.
func (v *OHLCVValidator) SetThresholds(ctx context.Context, thresholds map[models.AnomalyType]decimal.Decimal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	// Validate thresholds
	for anomalyType, threshold := range thresholds {
		if threshold.LessThanOrEqual(decimal.Zero) {
			return fmt.Errorf("threshold for %s must be positive, got: %s", anomalyType, threshold.String())
		}
	}

	// Update thresholds
	for anomalyType, threshold := range thresholds {
		v.thresholds[anomalyType] = threshold
	}

	// Update config if applicable
	if priceThreshold, exists := thresholds[models.AnomalyTypePriceSpike]; exists {
		v.config.PriceSpikeThreshold = priceThreshold.InexactFloat64()
	}
	if volumeThreshold, exists := thresholds[models.AnomalyTypeVolumeSurge]; exists {
		v.config.VolumeSurgeThreshold = volumeThreshold.InexactFloat64()
	}

	v.logger.Info("Updated anomaly detection thresholds", "thresholds", thresholds)
	return nil
}

// GetThresholds returns the current detection thresholds.
func (v *OHLCVValidator) GetThresholds() map[models.AnomalyType]decimal.Decimal {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// Return a copy to prevent external modification
	thresholdsCopy := make(map[models.AnomalyType]decimal.Decimal)
	for k, v := range v.thresholds {
		thresholdsCopy[k] = v
	}
	return thresholdsCopy
}

// ValidationPipeline interface implementation

// ProcessCandles executes the complete validation pipeline on a batch of candles.
func (v *OHLCVValidator) ProcessCandles(ctx context.Context, candles []contracts.Candle) (*ValidationPipelineResults, error) {
	startTime := time.Now()
	v.updateStatus(true, 0, 1, 0, int64(len(candles)), 0, 0, startTime, nil)

	defer v.updateStatus(false, 0, 0, 0, 0, 0, 0, time.Time{}, nil)

	results, err := v.ValidateCandles(ctx, candles)
	if err != nil {
		v.updateStatus(false, 0, 0, 0, 0, 0, 0, time.Time{}, err)
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Calculate pipeline results
	validCandles := int64(len(candles)) - int64(len(results.ValidationErrors))
	invalidCandles := int64(len(results.ValidationErrors))

	endTime := time.Now()
	processingTime := endTime.Sub(startTime)

	status := PipelineStatusSuccess
	if len(results.ValidationErrors) > 0 {
		if validCandles == 0 {
			status = PipelineStatusFailure
		} else {
			status = PipelineStatusPartialFailure
		}
	}

	pipelineResults := &ValidationPipelineResults{
		ProcessedCandles: int64(len(candles)),
		ValidCandles:     validCandles,
		InvalidCandles:   invalidCandles,
		QualityMetrics:   results.QualityMetrics,
		BatchResults:     []*ValidationResults{results},
		ProcessingTime:   processingTime,
		StartTime:        startTime,
		EndTime:          endTime,
		PipelineStatus:   status,
	}

	v.logger.Info("Pipeline processing completed",
		"processed", pipelineResults.ProcessedCandles,
		"valid", pipelineResults.ValidCandles,
		"invalid", pipelineResults.InvalidCandles,
		"status", status,
		"processing_time", processingTime)

	return pipelineResults, nil
}

// ProcessBatch processes candles in configurable batch sizes for memory efficiency.
func (v *OHLCVValidator) ProcessBatch(ctx context.Context, candles []contracts.Candle, batchSize int, progressCallback func(processed, total int)) (*ValidationPipelineResults, error) {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	startTime := time.Now()
	totalCandles := len(candles)
	totalBatches := int(math.Ceil(float64(totalCandles) / float64(batchSize)))

	v.updateStatus(true, 0, totalBatches, 0, int64(totalCandles), 0, 0, startTime, nil)
	defer v.updateStatus(false, 0, 0, 0, 0, 0, 0, time.Time{}, nil)

	var allBatchResults []*ValidationResults
	var aggregatedErrors []ValidationError
	var aggregatedAnomalies []AnomalyDetection
	var totalValid, totalInvalid int64

	for batchIndex := 0; batchIndex < totalBatches; batchIndex++ {
		select {
		case <-ctx.Done():
			v.updateStatus(false, 0, 0, 0, 0, 0, 0, time.Time{}, ctx.Err())
			return nil, ctx.Err()
		default:
		}

		start := batchIndex * batchSize
		end := start + batchSize
		if end > totalCandles {
			end = totalCandles
		}

		batch := candles[start:end]
		v.updateStatus(true, batchIndex+1, totalBatches, int64(start), int64(totalCandles), 0, 0, startTime, nil)

		batchResults, err := v.ValidateCandles(ctx, batch)
		if err != nil {
			v.logger.Error("Batch validation failed", "batch", batchIndex, "error", err)
			v.updateStatus(false, 0, 0, 0, 0, 0, 0, time.Time{}, err)
			return nil, fmt.Errorf("batch %d validation failed: %w", batchIndex, err)
		}

		allBatchResults = append(allBatchResults, batchResults)

		// Update aggregated results with batch offset
		for _, valErr := range batchResults.ValidationErrors {
			valErr.CandleIndex += start // Adjust index to global position
			aggregatedErrors = append(aggregatedErrors, valErr)
		}

		for _, anomaly := range batchResults.AnomalyDetections {
			anomaly.CandleIndex += start // Adjust index to global position
			aggregatedAnomalies = append(aggregatedAnomalies, anomaly)
		}

		batchValid := int64(len(batch)) - int64(len(batchResults.ValidationErrors))
		totalValid += batchValid
		totalInvalid += int64(len(batchResults.ValidationErrors))

		// Call progress callback if provided
		if progressCallback != nil {
			progressCallback(end, totalCandles)
		}

		v.logger.Debug("Completed batch validation",
			"batch", batchIndex+1,
			"total_batches", totalBatches,
			"batch_size", len(batch),
			"batch_valid", batchValid,
			"batch_errors", len(batchResults.ValidationErrors))
	}

	endTime := time.Now()
	processingTime := endTime.Sub(startTime)

	// Calculate overall quality metrics
	var validationResults []*models.ValidationResult
	// Convert from batch results - this is a simplified approach
	// In a real implementation, you might want to preserve the actual ValidationResult objects

	qualityMetrics, err := v.GetQualityMetrics(ctx, validationResults)
	if err != nil {
		v.logger.Error("Failed to calculate quality metrics", "error", err)
		qualityMetrics = &DataQualityMetrics{
			TotalCandles:     int64(totalCandles),
			ValidCandles:     totalValid,
			ValidationErrors: totalInvalid,
			ProcessedAt:      endTime,
			ProcessingTimeMs: processingTime.Milliseconds(),
		}
	}

	// Determine overall status
	status := PipelineStatusSuccess
	if totalInvalid > 0 {
		if totalValid == 0 {
			status = PipelineStatusFailure
		} else {
			status = PipelineStatusPartialFailure
		}
	}

	pipelineResults := &ValidationPipelineResults{
		ProcessedCandles: int64(totalCandles),
		ValidCandles:     totalValid,
		InvalidCandles:   totalInvalid,
		QualityMetrics:   qualityMetrics,
		BatchResults:     allBatchResults,
		ProcessingTime:   processingTime,
		StartTime:        startTime,
		EndTime:          endTime,
		PipelineStatus:   status,
	}

	v.logger.Info("Batch processing completed",
		"total_batches", totalBatches,
		"processed", pipelineResults.ProcessedCandles,
		"valid", pipelineResults.ValidCandles,
		"invalid", pipelineResults.InvalidCandles,
		"status", status,
		"processing_time", processingTime)

	return pipelineResults, nil
}

// GetQualityMetrics calculates and returns data quality metrics from validation results.
func (v *OHLCVValidator) GetQualityMetrics(ctx context.Context, results []*models.ValidationResult) (*DataQualityMetrics, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(results) == 0 {
		return &DataQualityMetrics{
			ProcessedAt: time.Now().UTC(),
		}, nil
	}

	metrics := &DataQualityMetrics{
		TotalCandles:     int64(len(results)),
		ErrorsByType:     make(map[string]int64),
		AnomaliesByType:  make(map[string]int64),
		ProcessedAt:      time.Now().UTC(),
	}

	var validCount int64
	var criticalCount int64
	var recommendations []string

	for _, result := range results {
		// Count anomalies by type
		for _, anomaly := range result.Anomalies {
			metrics.AnomaliesByType[string(anomaly.Type)]++
			metrics.AnomaliesDetected++

			if anomaly.Severity == models.SeverityCritical {
				criticalCount++
			}
		}

		// Count as valid if no critical issues
		if !result.HasSeverity(models.SeverityError) {
			validCount++
		}

		// Count validation errors (using severity as proxy)
		if result.HasSeverity(models.SeverityError) {
			metrics.ValidationErrors++
			metrics.ErrorsByType[string(result.Severity)]++
		}
	}

	metrics.ValidCandles = validCount
	metrics.CriticalIssues = criticalCount

	// Calculate rates
	if metrics.TotalCandles > 0 {
		metrics.SuccessRate = float64(metrics.ValidCandles) / float64(metrics.TotalCandles)
		metrics.AnomalyRate = float64(metrics.AnomaliesDetected) / float64(metrics.TotalCandles)
	}

	// Calculate quality score (0.0 to 1.0)
	if metrics.TotalCandles > 0 {
		errorWeight := 0.7
		anomalyWeight := 0.3
		
		errorScore := 1.0 - (float64(metrics.ValidationErrors) / float64(metrics.TotalCandles))
		anomalyScore := 1.0 - (float64(metrics.AnomaliesDetected) / float64(metrics.TotalCandles))
		
		metrics.QualityScore = (errorScore * errorWeight) + (anomalyScore * anomalyWeight)
		if metrics.QualityScore < 0 {
			metrics.QualityScore = 0
		}
	}

	// Generate recommendations
	if metrics.QualityScore < 0.5 {
		recommendations = append(recommendations, "Data quality is critically low - investigate data sources")
	} else if metrics.QualityScore < 0.8 {
		recommendations = append(recommendations, "Data quality needs improvement - review validation failures")
	}

	if metrics.AnomalyRate > 0.1 {
		recommendations = append(recommendations, "High anomaly rate detected - check for data feed issues")
	}

	if criticalCount > 0 {
		recommendations = append(recommendations, "Critical issues found - immediate attention required")
	}

	metrics.RecommendedActions = recommendations

	return metrics, nil
}

// Configure updates pipeline configuration including validation rules.
func (v *OHLCVValidator) Configure(ctx context.Context, config *ValidationPipelineConfig) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if config == nil {
		return fmt.Errorf("pipeline configuration cannot be nil")
	}

	// Update validator configuration if provided
	if config.Validator != nil {
		if err := v.UpdateConfig(ctx, config.Validator); err != nil {
			return fmt.Errorf("failed to update validator config: %w", err)
		}
	}

	v.logger.Info("Pipeline configuration updated",
		"batch_size", config.BatchSize,
		"parallel_processing", config.EnableParallelProcessing,
		"max_concurrency", config.MaxConcurrency,
		"quality_threshold", config.QualityThreshold)

	return nil
}

// GetStatus returns the current pipeline status and processing statistics.
func (v *OHLCVValidator) GetStatus() *ValidationPipelineStatus {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// Return a copy to prevent external modification
	statusCopy := *v.status
	return &statusCopy
}

// Helper methods

// detectCrossCandleAnomalies performs anomaly detection across multiple candles
func (v *OHLCVValidator) detectCrossCandleAnomalies(ctx context.Context, candles []contracts.Candle) ([]AnomalyDetection, error) {
	var allAnomalies []AnomalyDetection

	// Price spike detection
	priceAnomalies, err := v.DetectPriceSpikes(ctx, candles, v.thresholds[models.AnomalyTypePriceSpike])
	if err != nil {
		return nil, fmt.Errorf("price spike detection failed: %w", err)
	}

	for i, anomaly := range priceAnomalies {
		allAnomalies = append(allAnomalies, AnomalyDetection{
			CandleIndex:  i + 1, // Price spikes are detected on the second candle
			AnomalyType:  string(anomaly.Type),
			Severity:     string(anomaly.Severity),
			Confidence:   anomaly.Confidence,
			Description:  anomaly.Description,
			Value:        anomaly.Value,
			Threshold:    anomaly.Threshold,
			DetectedAt:   time.Now().UTC(),
		})
	}

	// Volume surge detection
	volumeAnomalies, err := v.DetectVolumeSurges(ctx, candles, v.thresholds[models.AnomalyTypeVolumeSurge])
	if err != nil {
		return nil, fmt.Errorf("volume surge detection failed: %w", err)
	}

	for i, anomaly := range volumeAnomalies {
		allAnomalies = append(allAnomalies, AnomalyDetection{
			CandleIndex:  i + 1, // Volume surges are detected on the second candle
			AnomalyType:  string(anomaly.Type),
			Severity:     string(anomaly.Severity),
			Confidence:   anomaly.Confidence,
			Description:  anomaly.Description,
			Value:        anomaly.Value,
			Threshold:    anomaly.Threshold,
			DetectedAt:   time.Now().UTC(),
		})
	}

	return allAnomalies, nil
}

// estimateInterval estimates the expected interval between candles
func (v *OHLCVValidator) estimateInterval(timestamps []time.Time) time.Duration {
	if len(timestamps) < 2 {
		return time.Hour // Default to 1 hour
	}

	// Sort timestamps to ensure proper ordering
	sortedTimestamps := make([]time.Time, len(timestamps))
	copy(sortedTimestamps, timestamps)
	sort.Slice(sortedTimestamps, func(i, j int) bool {
		return sortedTimestamps[i].Before(sortedTimestamps[j])
	})

	// Calculate intervals and find the most common one
	intervals := make(map[time.Duration]int)
	
	for i := 1; i < len(sortedTimestamps); i++ {
		interval := sortedTimestamps[i].Sub(sortedTimestamps[i-1])
		// Round to nearest minute to handle minor variations
		roundedInterval := interval.Round(time.Minute)
		intervals[roundedInterval]++
	}

	// Find the most common interval
	var mostCommonInterval time.Duration
	maxCount := 0
	
	for interval, count := range intervals {
		if count > maxCount {
			maxCount = count
			mostCommonInterval = interval
		}
	}

	// Default to 1 hour if no clear pattern
	if mostCommonInterval == 0 {
		mostCommonInterval = time.Hour
	}

	return mostCommonInterval
}

// updateStatus updates the internal pipeline status
func (v *OHLCVValidator) updateStatus(isProcessing bool, currentBatch, totalBatches int, processedCount, totalCount, errorCount, anomalyCount int64, startTime time.Time, lastError error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.status.IsProcessing = isProcessing
	v.status.CurrentBatch = currentBatch
	v.status.TotalBatches = totalBatches
	v.status.ProcessedCount = processedCount
	v.status.TotalCount = totalCount
	v.status.ErrorCount = errorCount
	v.status.AnomalyCount = anomalyCount
	v.status.StartTime = startTime
	v.status.LastError = lastError

	// Estimate completion time if processing
	if isProcessing && processedCount > 0 && totalCount > 0 {
		elapsed := time.Since(startTime)
		rate := float64(processedCount) / elapsed.Seconds()
		remaining := float64(totalCount - processedCount)
		if rate > 0 {
			estimatedCompletion := time.Now().Add(time.Duration(remaining/rate) * time.Second)
			v.status.EstimatedCompletionTime = &estimatedCompletion
		}
	} else {
		v.status.EstimatedCompletionTime = nil
	}
}

// ShouldEscalateSeverity helper function to determine if severity should be escalated
// This function is referenced in the models package but may not be exported
func ShouldEscalateSeverity(current, proposed models.SeverityLevel) bool {
	severityLevels := map[models.SeverityLevel]int{
		models.SeverityInfo:     1,
		models.SeverityWarning:  2,
		models.SeverityError:    3,
		models.SeverityCritical: 4,
	}
	
	return severityLevels[proposed] > severityLevels[current]
}