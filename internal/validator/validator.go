// Package validator provides interfaces for OHLCV data validation and anomaly detection.
//
// This package defines the core interfaces for data quality assessment including:
// - DataValidator: Main validation interface for logical consistency checks
// - AnomalyDetector: Interface for detecting price spikes, volume surges, and other anomalies
// - ValidationPipeline: Interface for orchestrating validation workflows
//
// The interfaces support configurable thresholds, context handling, and comprehensive
// validation using the ValidationResult and Anomaly models from internal/models.
package validator

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// DataValidator defines the main interface for validating OHLCV data quality and logical consistency.
// It provides methods for comprehensive validation including OHLCV rules, timestamp sequences,
// and configurable validation rules based on ValidationConfig.
type DataValidator interface {
	// ValidateCandles validates a slice of candle data and returns comprehensive validation results.
	// It performs logical consistency checks, anomaly detection, and timestamp validation
	// based on the validator's configuration.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candles: Slice of candle data to validate
	//
	// Returns:
	//   - ValidationResults containing errors, anomalies, and quality metrics
	//   - Error if validation process fails (not data quality issues)
	ValidateCandles(ctx context.Context, candles []contracts.Candle) (*ValidationResults, error)

	// ValidateCandle validates a single candle for logical consistency.
	// Checks OHLCV relationships: High >= max(Open, Close), Low <= min(Open, Close),
	// all prices > 0, and volume >= 0.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candle: Individual candle to validate
	//
	// Returns:
	//   - ValidationResult containing any detected logical inconsistencies
	//   - Error if validation process fails
	ValidateCandle(ctx context.Context, candle contracts.Candle) (*models.ValidationResult, error)

	// ValidateTimestampSequence validates timestamp ordering and drift tolerance.
	// Ensures timestamps are in chronological order and within acceptable drift tolerance
	// for the expected interval.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - timestamps: Slice of timestamps to validate
	//   - expectedInterval: Expected time interval between candles (e.g., 1h, 5m)
	//   - driftTolerance: Maximum acceptable drift from expected interval
	//
	// Returns:
	//   - Slice of Anomaly objects for any timestamp sequence violations
	//   - Error if validation process fails
	ValidateTimestampSequence(ctx context.Context, timestamps []time.Time, expectedInterval time.Duration, driftTolerance time.Duration) ([]models.Anomaly, error)

	// GetConfig returns the current validation configuration.
	// Allows inspection of thresholds, enabled checks, and other configuration settings.
	//
	// Returns:
	//   - ValidationConfig containing current settings
	GetConfig() *ValidationConfig

	// UpdateConfig updates the validation configuration with new settings.
	// Allows runtime modification of validation thresholds and enabled checks.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - config: New validation configuration
	//
	// Returns:
	//   - Error if configuration update fails
	UpdateConfig(ctx context.Context, config *ValidationConfig) error
}

// AnomalyDetector defines the interface for detecting various types of data anomalies.
// Implements detection algorithms for price spikes, volume surges, and other unusual patterns
// with configurable thresholds and confidence scoring.
type AnomalyDetector interface {
	// DetectPriceSpikes analyzes candle data for significant price movements.
	// Uses configurable threshold (default 500% from research.md) and calculates
	// confidence scores based on the magnitude of the spike.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candles: Slice of candle data to analyze (needs at least 2 candles for comparison)
	//   - threshold: Price spike threshold as multiplier (e.g., 5.0 for 500%)
	//
	// Returns:
	//   - Slice of Anomaly objects for detected price spikes
	//   - Error if detection process fails
	DetectPriceSpikes(ctx context.Context, candles []contracts.Candle, threshold decimal.Decimal) ([]models.Anomaly, error)

	// DetectVolumeSurges analyzes candle data for unusual volume patterns.
	// Uses configurable threshold (default 10x from research.md) and calculates
	// confidence scores based on the magnitude of the surge.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candles: Slice of candle data to analyze (needs at least 2 candles for comparison)
	//   - threshold: Volume surge threshold as multiplier (e.g., 10.0 for 10x)
	//
	// Returns:
	//   - Slice of Anomaly objects for detected volume surges
	//   - Error if detection process fails
	DetectVolumeSurges(ctx context.Context, candles []contracts.Candle, threshold decimal.Decimal) ([]models.Anomaly, error)

	// DetectSequenceGaps analyzes timestamp sequences for gaps or irregularities.
	// Identifies missing candles, irregular intervals, and timestamp drift issues.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - timestamps: Slice of timestamps to analyze
	//   - expectedInterval: Expected time interval between candles
	//   - tolerance: Maximum acceptable drift from expected interval
	//
	// Returns:
	//   - Slice of Anomaly objects for detected sequence gaps
	//   - Error if detection process fails
	DetectSequenceGaps(ctx context.Context, timestamps []time.Time, expectedInterval time.Duration, tolerance time.Duration) ([]models.Anomaly, error)

	// DetectLogicalAnomalies analyzes OHLCV data for logical inconsistencies.
	// Detects violations of basic OHLCV relationships and mathematical constraints.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candle: Individual candle to analyze for logical inconsistencies
	//
	// Returns:
	//   - Slice of Anomaly objects for detected logical violations
	//   - Error if detection process fails
	DetectLogicalAnomalies(ctx context.Context, candle contracts.Candle) ([]models.Anomaly, error)

	// SetThresholds configures detection thresholds for different anomaly types.
	// Allows runtime adjustment of sensitivity for price spikes, volume surges, etc.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - thresholds: Map of anomaly types to their detection thresholds
	//
	// Returns:
	//   - Error if threshold configuration fails
	SetThresholds(ctx context.Context, thresholds map[models.AnomalyType]decimal.Decimal) error

	// GetThresholds returns the current detection thresholds.
	// Provides visibility into current anomaly detection sensitivity settings.
	//
	// Returns:
	//   - Map of anomaly types to their current thresholds
	GetThresholds() map[models.AnomalyType]decimal.Decimal
}

// ValidationPipeline defines the interface for orchestrating complete validation workflows.
// Combines data validation, anomaly detection, and result aggregation into a cohesive pipeline
// suitable for batch processing and quality assessment workflows.
type ValidationPipeline interface {
	// ProcessCandles executes the complete validation pipeline on a batch of candles.
	// Performs all enabled validation checks, anomaly detection, and generates
	// comprehensive quality metrics and reports.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candles: Slice of candle data to process through the pipeline
	//
	// Returns:
	//   - ValidationPipelineResults containing comprehensive validation outcomes
	//   - Error if pipeline execution fails
	ProcessCandles(ctx context.Context, candles []contracts.Candle) (*ValidationPipelineResults, error)

	// ProcessBatch processes candles in configurable batch sizes for memory efficiency.
	// Useful for processing large datasets while maintaining memory usage controls
	// and providing progress feedback.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - candles: Slice of candle data to process in batches
	//   - batchSize: Number of candles to process per batch
	//   - progressCallback: Optional callback for batch processing progress updates
	//
	// Returns:
	//   - ValidationPipelineResults containing aggregated results from all batches
	//   - Error if batch processing fails
	ProcessBatch(ctx context.Context, candles []contracts.Candle, batchSize int, progressCallback func(processed, total int)) (*ValidationPipelineResults, error)

	// GetQualityMetrics calculates and returns data quality metrics from validation results.
	// Provides comprehensive quality scoring, error categorization, and trend analysis.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - results: Slice of ValidationResult objects to analyze
	//
	// Returns:
	//   - DataQualityMetrics containing quality scores and categorized metrics
	//   - Error if metrics calculation fails
	GetQualityMetrics(ctx context.Context, results []*models.ValidationResult) (*DataQualityMetrics, error)

	// Configure updates pipeline configuration including validation rules,
	// anomaly detection settings, and processing options.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - config: New pipeline configuration
	//
	// Returns:
	//   - Error if configuration update fails
	Configure(ctx context.Context, config *ValidationPipelineConfig) error

	// GetStatus returns the current pipeline status and processing statistics.
	// Provides visibility into pipeline health, performance metrics, and processing history.
	//
	// Returns:
	//   - ValidationPipelineStatus containing current status and statistics
	GetStatus() *ValidationPipelineStatus
}

// Configuration types

// ValidationConfig configures data validator behavior including thresholds,
// enabled checks, and tolerance settings.
type ValidationConfig struct {
	// PriceSpikeThreshold defines the multiplier threshold for price spike detection
	// (e.g., 5.0 for 500% increase threshold from research.md)
	PriceSpikeThreshold float64

	// VolumeSurgeThreshold defines the multiplier threshold for volume surge detection
	// (e.g., 10.0 for 10x increase threshold from research.md)
	VolumeSurgeThreshold float64

	// TimestampDriftTolerance defines maximum acceptable timestamp drift
	TimestampDriftTolerance time.Duration

	// EnableLogicalChecks controls whether OHLCV logical consistency checks are performed
	EnableLogicalChecks bool

	// EnableAnomalyDetection controls whether anomaly detection is performed
	EnableAnomalyDetection bool

	// EnableTimestampChecks controls whether timestamp sequence validation is performed
	EnableTimestampChecks bool

	// MinConfidenceThreshold defines minimum confidence score for reporting anomalies
	MinConfidenceThreshold float64

	// MaxToleratedErrors defines maximum number of errors before escalation
	MaxToleratedErrors int

	// ValidationTimeout defines maximum time allowed for validation operations
	ValidationTimeout time.Duration
}

// ValidationPipelineConfig configures the validation pipeline behavior
type ValidationPipelineConfig struct {
	// Validator configures the data validator component
	Validator *ValidationConfig

	// BatchSize defines the default batch size for processing large datasets
	BatchSize int

	// EnableParallelProcessing controls whether batches are processed in parallel
	EnableParallelProcessing bool

	// MaxConcurrency defines maximum number of concurrent validation workers
	MaxConcurrency int

	// EnableProgressReporting controls whether progress callbacks are invoked
	EnableProgressReporting bool

	// QualityThreshold defines minimum acceptable quality score (0.0 to 1.0)
	QualityThreshold float64
}

// Result types

// ValidationResults represents the comprehensive results of data validation
type ValidationResults struct {
	// ValidationErrors contains logical consistency and data quality errors
	ValidationErrors []ValidationError

	// AnomalyDetections contains detected price spikes, volume surges, and other anomalies
	AnomalyDetections []AnomalyDetection

	// QualityMetrics provides aggregated quality assessment metrics
	QualityMetrics *DataQualityMetrics

	// ProcessingTime tracks how long validation took
	ProcessingTime time.Duration

	// ProcessedAt timestamp when validation was completed
	ProcessedAt time.Time
}

// ValidationError represents a data validation error with context
type ValidationError struct {
	// CandleIndex identifies which candle in the batch contained the error
	CandleIndex int

	// ErrorType categorizes the type of validation error
	ErrorType string

	// Message provides human-readable description of the error
	Message string

	// Severity indicates the severity level of the error
	Severity string

	// Field identifies the specific field that failed validation (if applicable)
	Field string

	// Value contains the actual value that failed validation
	Value string

	// Expected contains the expected value or range (if applicable)
	Expected string
}

// AnomalyDetection represents a detected data anomaly with confidence scoring
type AnomalyDetection struct {
	// CandleIndex identifies which candle in the batch contained the anomaly
	CandleIndex int

	// AnomalyType categorizes the type of anomaly detected
	AnomalyType string

	// Severity indicates the severity level of the anomaly
	Severity string

	// Confidence score from 0.0 to 1.0 indicating detection confidence
	Confidence float64

	// Description provides human-readable description of the anomaly
	Description string

	// Value contains the actual value that triggered the anomaly detection
	Value decimal.Decimal

	// Threshold contains the threshold that was exceeded
	Threshold decimal.Decimal

	// DetectedAt timestamp when the anomaly was detected
	DetectedAt time.Time
}

// DataQualityMetrics provides comprehensive data quality assessment
type DataQualityMetrics struct {
	// TotalCandles is the total number of candles processed
	TotalCandles int64

	// ValidCandles is the number of candles that passed all validation checks
	ValidCandles int64

	// ValidationErrors is the total number of validation errors detected
	ValidationErrors int64

	// AnomaliesDetected is the total number of anomalies detected
	AnomaliesDetected int64

	// QualityScore is an overall quality score from 0.0 (poor) to 1.0 (excellent)
	QualityScore float64

	// ErrorsByType categorizes validation errors by type
	ErrorsByType map[string]int64

	// AnomaliesByType categorizes anomalies by type
	AnomaliesByType map[string]int64

	// ProcessingTimeMs tracks total processing time in milliseconds
	ProcessingTimeMs int64

	// ProcessedAt timestamp when metrics were calculated
	ProcessedAt time.Time

	// SuccessRate is the percentage of candles that passed validation
	SuccessRate float64

	// AnomalyRate is the percentage of candles with detected anomalies
	AnomalyRate float64

	// CriticalIssues is the number of critical severity issues detected
	CriticalIssues int64

	// RecommendedActions contains suggested actions based on quality assessment
	RecommendedActions []string
}

// ValidationPipelineResults contains comprehensive results from pipeline processing
type ValidationPipelineResults struct {
	// ProcessedCandles is the total number of candles processed
	ProcessedCandles int64

	// ValidCandles is the number of candles that passed all validation
	ValidCandles int64

	// InvalidCandles is the number of candles with validation errors
	InvalidCandles int64

	// QualityMetrics provides detailed quality assessment
	QualityMetrics *DataQualityMetrics

	// BatchResults contains results from each processing batch (if batch processing was used)
	BatchResults []*ValidationResults

	// ProcessingTime tracks total pipeline execution time
	ProcessingTime time.Duration

	// StartTime when pipeline processing began
	StartTime time.Time

	// EndTime when pipeline processing completed
	EndTime time.Time

	// PipelineStatus indicates overall pipeline execution status
	PipelineStatus PipelineStatus
}

// ValidationPipelineStatus provides current pipeline status and statistics
type ValidationPipelineStatus struct {
	// IsProcessing indicates if pipeline is currently processing data
	IsProcessing bool

	// CurrentBatch indicates which batch is currently being processed (if applicable)
	CurrentBatch int

	// TotalBatches indicates total number of batches to process (if applicable)
	TotalBatches int

	// ProcessedCount tracks number of candles processed so far
	ProcessedCount int64

	// TotalCount tracks total number of candles to process
	TotalCount int64

	// ErrorCount tracks number of errors encountered during processing
	ErrorCount int64

	// AnomalyCount tracks number of anomalies detected during processing
	AnomalyCount int64

	// StartTime when current processing session began
	StartTime time.Time

	// EstimatedCompletionTime provides estimated completion time (if available)
	EstimatedCompletionTime *time.Time

	// LastError contains the most recent error encountered (if any)
	LastError error
}

// PipelineStatus represents the overall status of pipeline execution
type PipelineStatus string

const (
	// PipelineStatusSuccess indicates successful completion
	PipelineStatusSuccess PipelineStatus = "success"

	// PipelineStatusPartialFailure indicates partial success with some errors
	PipelineStatusPartialFailure PipelineStatus = "partial_failure"

	// PipelineStatusFailure indicates complete failure
	PipelineStatusFailure PipelineStatus = "failure"

	// PipelineStatusProcessing indicates currently processing
	PipelineStatusProcessing PipelineStatus = "processing"

	// PipelineStatusCancelled indicates processing was cancelled
	PipelineStatusCancelled PipelineStatus = "cancelled"
)

// Convenience constructors and utility functions

// NewValidationConfig creates a new ValidationConfig with sensible defaults
// based on research findings (500% price spike, 10x volume surge thresholds)
func NewValidationConfig() *ValidationConfig {
	return &ValidationConfig{
		PriceSpikeThreshold:     5.0,  // 500% from research.md
		VolumeSurgeThreshold:    10.0, // 10x from research.md
		TimestampDriftTolerance: 2 * time.Minute,
		EnableLogicalChecks:     true,
		EnableAnomalyDetection:  true,
		EnableTimestampChecks:   true,
		MinConfidenceThreshold:  0.7,
		MaxToleratedErrors:      10,
		ValidationTimeout:       30 * time.Second,
	}
}

// NewValidationPipelineConfig creates a new ValidationPipelineConfig with defaults
func NewValidationPipelineConfig() *ValidationPipelineConfig {
	return &ValidationPipelineConfig{
		Validator:                NewValidationConfig(),
		BatchSize:                100,
		EnableParallelProcessing: true,
		MaxConcurrency:           4,
		EnableProgressReporting:  true,
		QualityThreshold:         0.8,
	}
}
