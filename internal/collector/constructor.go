package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/gaps"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/validator"
	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
)

// NewWithDefaults creates a new Collector with default configuration
// This is the main constructor used by integration tests
func NewWithDefaults(
	exchange contracts.ExchangeAdapter,
	storage contracts.FullStorage,
) Collector {
	return NewWithConfig(exchange, storage, nil, nil, nil)
}

// NewWithConfig creates a new Collector with custom configuration
func NewWithConfig(
	exchange contracts.ExchangeAdapter,
	storage contracts.FullStorage,
	validator validator.ValidationPipeline,
	gapDetector gaps.GapDetector,
	config *Config,
) Collector {
	if config == nil {
		config = DefaultConfig()
	}

	// Validate configuration
	if err := ValidateConfig(config); err != nil {
		config.Logger.Error("Invalid collector configuration, using defaults", "error", err)
		config = DefaultConfig()
	}

	// Create validator if not provided
	if validator == nil {
		// Create a mock/stub validator for now
		// In a real implementation, this would create a proper validator
		validator = &stubValidator{}
	}

	// Create gap detector if not provided
	if gapDetector == nil {
		// Create a mock/stub gap detector for now
		// In a real implementation, this would create a proper gap detector
		gapDetector = &stubGapDetector{}
	}

	return New(exchange, storage, validator, gapDetector, config)
}

// Builder pattern for more flexible construction

// CollectorBuilder provides a builder pattern for creating collectors
type CollectorBuilder struct {
	exchange    contracts.ExchangeAdapter
	storage     contracts.FullStorage
	validator   validator.ValidationPipeline
	gapDetector gaps.GapDetector
	config      *Config
	logger      *slog.Logger
}

// NewBuilder creates a new collector builder
func NewBuilder() *CollectorBuilder {
	return &CollectorBuilder{
		config: DefaultConfig(),
		logger: slog.Default(),
	}
}

// WithExchange sets the exchange adapter
func (b *CollectorBuilder) WithExchange(exchange contracts.ExchangeAdapter) *CollectorBuilder {
	b.exchange = exchange
	return b
}

// WithStorage sets the storage adapter
func (b *CollectorBuilder) WithStorage(storage contracts.FullStorage) *CollectorBuilder {
	b.storage = storage
	return b
}

// WithValidator sets the validator
func (b *CollectorBuilder) WithValidator(validator validator.ValidationPipeline) *CollectorBuilder {
	b.validator = validator
	return b
}

// WithGapDetector sets the gap detector
func (b *CollectorBuilder) WithGapDetector(gapDetector gaps.GapDetector) *CollectorBuilder {
	b.gapDetector = gapDetector
	return b
}

// WithConfig sets the configuration
func (b *CollectorBuilder) WithConfig(config *Config) *CollectorBuilder {
	if config != nil {
		b.config = config
	}
	return b
}

// WithLogger sets the logger
func (b *CollectorBuilder) WithLogger(logger *slog.Logger) *CollectorBuilder {
	b.logger = logger
	if b.config != nil {
		b.config.Logger = logger
	}
	return b
}

// WithWorkerCount sets the worker count
func (b *CollectorBuilder) WithWorkerCount(count int) *CollectorBuilder {
	b.config.WorkerCount = count
	return b
}

// WithBatchSize sets the batch size
func (b *CollectorBuilder) WithBatchSize(size int) *CollectorBuilder {
	b.config.BatchSize = size
	return b
}

// WithRateLimit sets the rate limit
func (b *CollectorBuilder) WithRateLimit(limit int) *CollectorBuilder {
	b.config.RateLimit = limit
	return b
}

// WithMemoryLimit sets the memory limit in MB
func (b *CollectorBuilder) WithMemoryLimit(limitMB int) *CollectorBuilder {
	b.config.MemoryLimitMB = limitMB
	return b
}

// WithValidation enables or disables validation
func (b *CollectorBuilder) WithValidation(enabled bool) *CollectorBuilder {
	b.config.ValidationEnabled = enabled
	return b
}

// WithGapDetection enables or disables gap detection
func (b *CollectorBuilder) WithGapDetection(enabled bool) *CollectorBuilder {
	b.config.GapDetectionEnabled = enabled
	return b
}

// Build creates the collector with the configured options
func (b *CollectorBuilder) Build() (Collector, error) {
	// Validate required dependencies
	if b.exchange == nil {
		return nil, fmt.Errorf("exchange adapter is required")
	}

	if b.storage == nil {
		return nil, fmt.Errorf("storage adapter is required")
	}

	// Validate configuration
	if err := ValidateConfig(b.config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set logger in config
	if b.logger != nil && b.config != nil {
		b.config.Logger = b.logger
	}

	// Create with provided or default components
	return NewWithConfig(b.exchange, b.storage, b.validator, b.gapDetector, b.config), nil
}

// Stub implementations for testing (these would be replaced with real implementations)

// stubValidator provides a minimal validator implementation for testing
type stubValidator struct{}

func (s *stubValidator) ProcessCandles(ctx context.Context, candles []contracts.Candle) (*validator.ValidationPipelineResults, error) {
	// Return basic validation results
	return &validator.ValidationPipelineResults{
		ProcessedCandles: int64(len(candles)),
		ValidCandles:     int64(len(candles)),
		InvalidCandles:   0,
		QualityMetrics: &validator.DataQualityMetrics{
			TotalCandles:      int64(len(candles)),
			ValidCandles:      int64(len(candles)),
			ValidationErrors:  0,
			AnomaliesDetected: 0,
			QualityScore:      1.0,
			SuccessRate:       1.0,
		},
		PipelineStatus: validator.PipelineStatusSuccess,
	}, nil
}

func (s *stubValidator) ProcessBatch(ctx context.Context, candles []contracts.Candle, batchSize int, progressCallback func(processed, total int)) (*validator.ValidationPipelineResults, error) {
	return s.ProcessCandles(ctx, candles)
}

func (s *stubValidator) GetQualityMetrics(ctx context.Context, results []*models.ValidationResult) (*validator.DataQualityMetrics, error) {
	return &validator.DataQualityMetrics{
		QualityScore: 1.0,
		SuccessRate:  1.0,
	}, nil
}

func (s *stubValidator) Configure(ctx context.Context, config *validator.ValidationPipelineConfig) error {
	return nil
}

func (s *stubValidator) GetStatus() *validator.ValidationPipelineStatus {
	return &validator.ValidationPipelineStatus{
		IsProcessing: false,
		ErrorCount:   0,
		AnomalyCount: 0,
	}
}

// stubGapDetector provides a minimal gap detector implementation for testing
type stubGapDetector struct{}

func (s *stubGapDetector) DetectGaps(ctx context.Context, pair, interval string, startTime, endTime time.Time) ([]models.Gap, error) {
	// Return empty gaps for testing
	return []models.Gap{}, nil
}

func (s *stubGapDetector) DetectGapsInSequence(ctx context.Context, candles []contracts.Candle, expectedInterval string) ([]models.Gap, error) {
	return []models.Gap{}, nil
}

func (s *stubGapDetector) DetectRecentGaps(ctx context.Context, pair, interval string, lookbackPeriod time.Duration) ([]models.Gap, error) {
	return []models.Gap{}, nil
}

func (s *stubGapDetector) ValidateGapPeriod(ctx context.Context, pair string, startTime, endTime time.Time, interval string) (bool, string, error) {
	return true, "", nil
}
