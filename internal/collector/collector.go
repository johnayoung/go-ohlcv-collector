// Package collector provides the main orchestration component for OHLCV data collection
// with worker pools, memory management, and comprehensive error handling.
//
// This package implements the core Collector interface from integration tests with:
// - Worker pool pattern with golang.org/x/time/rate for API rate limiting
// - Memory management using sync.Pool and streaming processing <100MB
// - Integration with exchange adapters, storage, validators, and gap detectors
// - Comprehensive logging, metrics collection, and error handling with retry classification
// - Support for historical collection, scheduled updates, and gap backfilling
// - Context propagation and graceful cancellation following Go best practices
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/time/rate"

	"github.com/johnayoung/go-ohlcv-collector/internal/gaps"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/validator"
	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
)

// Configuration constants from research findings
const (
	// MaxMemoryUsageMB defines the maximum memory usage in MB (from research.md)
	MaxMemoryUsageMB = 100

	// DefaultWorkerCount defines default number of workers
	DefaultWorkerCount = 4

	// DefaultBatchSize defines the batch size for memory management (from research.md)
	DefaultBatchSize = 10000

	// DefaultRateLimit from Coinbase API research (10 req/sec)
	DefaultRateLimit = 10

	// InitialBackoffInterval from research findings (500ms)
	InitialBackoffInterval = 500 * time.Millisecond

	// MaxBackoffInterval from research findings (30s)
	MaxBackoffInterval = 30 * time.Second

	// MaxRetries for collection operations
	MaxRetries = 5

	// MemoryCheckInterval for periodic memory monitoring
	MemoryCheckInterval = 30 * time.Second
)

// Collector provides the main interface for OHLCV data collection orchestration
type Collector interface {
	// CollectHistorical performs historical data collection for a specific time range
	CollectHistorical(ctx context.Context, req HistoricalRequest) error

	// CollectScheduled performs scheduled data collection for multiple pairs
	CollectScheduled(ctx context.Context, pairs []string, interval string) error

	// GetMetrics returns current collection metrics and statistics
	GetMetrics(ctx context.Context) (*CollectionMetrics, error)

	// Start initializes the collector and background services
	Start(ctx context.Context) error

	// Stop gracefully shuts down the collector
	Stop(ctx context.Context) error

	// Health returns the current health status
	Health(ctx context.Context) error
}

// HistoricalRequest defines parameters for historical data collection
type HistoricalRequest struct {
	Pair     string
	Interval string
	Start    time.Time
	End      time.Time
}

// CollectionMetrics provides comprehensive collection statistics
type CollectionMetrics struct {
	CandlesCollected  int64
	ErrorCount        int64
	SuccessRate       float64
	AvgResponseTime   time.Duration
	RateLimitHits     int64
	MemoryUsageMB     int64
	ActiveConnections int
	WorkerPoolStats   *WorkerPoolStats
	ValidationStats   *ValidationStats
}

// WorkerPoolStats provides worker pool performance metrics
type WorkerPoolStats struct {
	ActiveWorkers  int
	QueuedJobs     int
	CompletedJobs  int64
	FailedJobs     int64
	AvgJobDuration time.Duration
}

// ValidationStats provides validation performance metrics
type ValidationStats struct {
	TotalValidated    int64
	ValidationErrors  int64
	AnomaliesDetected int64
	QualityScore      float64
}

// Config configures the collector behavior
type Config struct {
	WorkerCount         int
	BatchSize           int
	RateLimit           int
	MemoryLimitMB       int
	RetryAttempts       int
	ValidationEnabled   bool
	GapDetectionEnabled bool
	Logger              *slog.Logger
}

// DefaultConfig returns a configuration with sensible defaults based on research findings
func DefaultConfig() *Config {
	return &Config{
		WorkerCount:         DefaultWorkerCount,
		BatchSize:           DefaultBatchSize,
		RateLimit:           DefaultRateLimit,
		MemoryLimitMB:       MaxMemoryUsageMB,
		RetryAttempts:       MaxRetries,
		ValidationEnabled:   true,
		GapDetectionEnabled: true,
		Logger:              slog.Default(),
	}
}

// collectorImpl implements the Collector interface
type collectorImpl struct {
	config *Config

	// Dependencies injected via constructor
	exchange  contracts.ExchangeAdapter
	storage   contracts.FullStorage
	validator validator.ValidationPipeline
	gaps      gaps.GapDetector

	// Rate limiting and concurrency control
	rateLimiter *rate.Limiter
	workerPool  *WorkerPool

	// Metrics and monitoring
	metrics    *metricsCollector
	memoryMgmt *memoryManager

	// Object pools for memory management
	candlePool   *sync.Pool
	requestPool  *sync.Pool
	responsePool *sync.Pool

	// State management
	isRunning  int32
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex

	logger *slog.Logger
}

// New creates a new Collector instance with the provided dependencies
func New(
	exchange contracts.ExchangeAdapter,
	storage contracts.FullStorage,
	validator validator.ValidationPipeline,
	gapDetector gaps.GapDetector,
	config *Config,
) Collector {
	if config == nil {
		config = DefaultConfig()
	}

	c := &collectorImpl{
		config:    config,
		exchange:  exchange,
		storage:   storage,
		validator: validator,
		gaps:      gapDetector,
		logger:    config.Logger,

		// Initialize rate limiter (10 req/sec from research)
		rateLimiter: rate.NewLimiter(rate.Limit(config.RateLimit), 1),

		// Initialize shutdown channel
		shutdownCh: make(chan struct{}),

		// Initialize metrics collector
		metrics: newMetricsCollector(),

		// Initialize memory manager
		memoryMgmt: newMemoryManager(config.MemoryLimitMB),
	}

	// Initialize object pools for memory efficiency
	c.initializePools()

	// Initialize worker pool
	c.workerPool = NewWorkerPool(config.WorkerCount, c.rateLimiter, c.logger)

	return c
}

// initializePools sets up sync.Pool instances for object reuse
func (c *collectorImpl) initializePools() {
	c.candlePool = &sync.Pool{
		New: func() interface{} {
			return make([]contracts.Candle, 0, c.config.BatchSize)
		},
	}

	c.requestPool = &sync.Pool{
		New: func() interface{} {
			return &contracts.FetchRequest{}
		},
	}

	c.responsePool = &sync.Pool{
		New: func() interface{} {
			return &contracts.FetchResponse{}
		},
	}
}

// Start initializes the collector and starts background services
func (c *collectorImpl) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		return fmt.Errorf("collector is already running")
	}

	c.logger.Info("Starting OHLCV data collector",
		"worker_count", c.config.WorkerCount,
		"batch_size", c.config.BatchSize,
		"rate_limit", c.config.RateLimit,
		"memory_limit_mb", c.config.MemoryLimitMB,
	)

	// Start worker pool
	if err := c.workerPool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker pool: %w", err)
	}

	// Start memory monitoring
	c.startMemoryMonitoring(ctx)

	// Start metrics collection
	c.startMetricsCollection(ctx)

	c.logger.Info("Collector started successfully")
	return nil
}

// Stop gracefully shuts down the collector
func (c *collectorImpl) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 1, 0) {
		return fmt.Errorf("collector is not running")
	}

	c.logger.Info("Stopping OHLCV data collector")

	// Signal shutdown
	close(c.shutdownCh)

	// Stop worker pool
	if err := c.workerPool.Stop(ctx); err != nil {
		c.logger.Error("Error stopping worker pool", "error", err)
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("Collector stopped successfully")
		return nil
	case <-ctx.Done():
		c.logger.Warn("Collector stop timed out", "error", ctx.Err())
		return ctx.Err()
	}
}

// Health returns the current health status of the collector
func (c *collectorImpl) Health(ctx context.Context) error {
	if atomic.LoadInt32(&c.isRunning) == 0 {
		return fmt.Errorf("collector is not running")
	}

	// Check exchange health
	if err := c.exchange.HealthCheck(ctx); err != nil {
		return fmt.Errorf("exchange health check failed: %w", err)
	}

	// Check storage health
	if err := c.storage.HealthCheck(ctx); err != nil {
		return fmt.Errorf("storage health check failed: %w", err)
	}

	// Check memory usage
	if c.memoryMgmt.IsOverLimit() {
		return fmt.Errorf("memory usage exceeds limit: %dMB", c.memoryMgmt.GetUsageMB())
	}

	return nil
}

// CollectHistorical performs historical data collection with comprehensive error handling
func (c *collectorImpl) CollectHistorical(ctx context.Context, req HistoricalRequest) error {
	startTime := time.Now()

	c.logger.Info("Starting historical data collection",
		"pair", req.Pair,
		"interval", req.Interval,
		"start", req.Start,
		"end", req.End,
	)

	// Validate request
	if err := c.validateHistoricalRequest(req); err != nil {
		c.metrics.recordError("validation", err)
		return fmt.Errorf("invalid request: %w", err)
	}

	// Create and track job
	job := models.NewJob(
		fmt.Sprintf("historical_%s_%s_%d", req.Pair, req.Interval, startTime.Unix()),
		models.JobTypeInitialSync,
		req.Pair,
		req.Start,
		req.End,
		req.Interval,
	)

	if err := job.Start(); err != nil {
		return fmt.Errorf("failed to start job: %w", err)
	}

	// Process collection with retries
	err := c.processHistoricalWithRetry(ctx, req, job)

	// Update job status based on result
	if err != nil {
		job.Fail(err.Error())
		c.metrics.recordError("collection", err)
		c.logger.Error("Historical collection failed",
			"pair", req.Pair,
			"interval", req.Interval,
			"error", err,
			"duration", time.Since(startTime),
		)
		return err
	}

	job.Complete()
	duration := time.Since(startTime)
	c.metrics.recordSuccess(duration)

	c.logger.Info("Historical collection completed successfully",
		"pair", req.Pair,
		"interval", req.Interval,
		"records", job.RecordsCollected,
		"duration", duration,
	)

	return nil
}

// processHistoricalWithRetry handles the actual collection with exponential backoff
func (c *collectorImpl) processHistoricalWithRetry(ctx context.Context, req HistoricalRequest, job *models.Job) error {
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = InitialBackoffInterval
	backoffConfig.MaxInterval = MaxBackoffInterval
	backoffConfig.MaxElapsedTime = 10 * time.Minute
	backoffConfig.Multiplier = 2.0
	backoffConfig.RandomizationFactor = 0.5 // 50% jitter from research

	return backoff.RetryNotify(
		func() error {
			return c.processHistoricalData(ctx, req, job)
		},
		backoff.WithContext(backoffConfig, ctx),
		func(err error, duration time.Duration) {
			c.logger.Warn("Historical collection attempt failed, retrying",
				"pair", req.Pair,
				"error", err,
				"retry_delay", duration,
			)
		},
	)
}

// processHistoricalData performs the actual data collection and storage
func (c *collectorImpl) processHistoricalData(ctx context.Context, req HistoricalRequest, job *models.Job) error {
	// Calculate time chunks for batch processing
	chunks := c.calculateTimeChunks(req.Start, req.End, req.Interval)
	totalChunks := len(chunks)

	c.logger.Debug("Processing historical data in chunks",
		"total_chunks", totalChunks,
		"chunk_duration", chunks[0].duration,
	)

	var allCandles []contracts.Candle
	processed := 0

	for i, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Rate limiting
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiting failed: %w", err)
		}

		// Fetch data for this chunk
		candles, err := c.fetchChunkData(ctx, req.Pair, req.Interval, chunk)
		if err != nil {
			return fmt.Errorf("failed to fetch chunk %d/%d: %w", i+1, totalChunks, err)
		}

		// Validate data if enabled
		if c.config.ValidationEnabled {
			validationResults, err := c.validator.ProcessCandles(ctx, candles)
			if err != nil {
				c.logger.Warn("Validation failed for chunk", "chunk", i+1, "error", err)
				// Continue processing despite validation errors
			} else {
				c.metrics.recordValidation(validationResults)
			}
		}

		// Accumulate candles
		allCandles = append(allCandles, candles...)
		processed++

		// Update job progress
		progress := int((processed * 100) / totalChunks)
		job.UpdateProgress(progress, len(allCandles))

		// Memory management - process in batches if too large
		if len(allCandles) >= c.config.BatchSize {
			if err := c.storeBatch(ctx, allCandles); err != nil {
				return fmt.Errorf("failed to store batch: %w", err)
			}

			// Return candles to pool to reduce GC pressure
			c.returnCandlesToPool(allCandles)
			allCandles = allCandles[:0] // Reset slice but keep capacity
		}

		// Check memory usage
		if c.memoryMgmt.IsOverLimit() {
			c.logger.Warn("Memory usage approaching limit, forcing batch storage")
			if len(allCandles) > 0 {
				if err := c.storeBatch(ctx, allCandles); err != nil {
					return fmt.Errorf("failed to store batch during memory management: %w", err)
				}
				c.returnCandlesToPool(allCandles)
				allCandles = allCandles[:0]
			}

			// Force GC to free memory
			runtime.GC()
		}
	}

	// Store remaining candles
	if len(allCandles) > 0 {
		if err := c.storeBatch(ctx, allCandles); err != nil {
			return fmt.Errorf("failed to store final batch: %w", err)
		}
		c.returnCandlesToPool(allCandles)
	}

	// Detect gaps if enabled
	if c.config.GapDetectionEnabled {
		if err := c.detectAndStoreGaps(ctx, req); err != nil {
			c.logger.Warn("Gap detection failed", "error", err)
			// Don't fail the entire operation for gap detection errors
		}
	}

	return nil
}

// CollectScheduled performs scheduled data collection for multiple pairs
func (c *collectorImpl) CollectScheduled(ctx context.Context, pairs []string, interval string) error {
	c.logger.Info("Starting scheduled collection",
		"pairs", pairs,
		"interval", interval,
	)

	// Process pairs concurrently using direct collection
	errCh := make(chan error, len(pairs))
	var wg sync.WaitGroup

	for _, pair := range pairs {
		wg.Add(1)

		go func(p string) {
			defer wg.Done()

			// For scheduled collection, get recent data (last 2 days to ensure overlap)
			req := HistoricalRequest{
				Pair:     p,
				Interval: interval,
				Start:    time.Now().AddDate(0, 0, -2), // Last 2 days
				End:      time.Now(),
			}

			if err := c.CollectHistorical(ctx, req); err != nil {
				errCh <- fmt.Errorf("failed to collect %s: %w", p, err)
			}
		}(pair)
	}

	// Wait for all jobs to complete
	wg.Wait()
	close(errCh)

	// Collect any errors
	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("scheduled collection had %d errors: %v", len(errors), errors)
	}

	c.logger.Info("Scheduled collection completed successfully",
		"pairs_processed", len(pairs),
	)

	return nil
}

// GetMetrics returns current collection metrics
func (c *collectorImpl) GetMetrics(ctx context.Context) (*CollectionMetrics, error) {
	metrics := c.metrics.getMetrics()

	// Add current memory usage
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	metrics.MemoryUsageMB = int64(memStats.Alloc / 1024 / 1024)

	// Add worker pool stats
	metrics.WorkerPoolStats = c.workerPool.GetStats()

	// Add validation stats if validator is available
	if c.validator != nil {
		status := c.validator.GetStatus()
		metrics.ValidationStats = &ValidationStats{
			TotalValidated:    status.ProcessedCount,
			ValidationErrors:  status.ErrorCount,
			AnomaliesDetected: status.AnomalyCount,
		}
	}

	return metrics, nil
}

// Helper methods for data processing

// timeChunk represents a time range for batch processing
type timeChunk struct {
	start    time.Time
	end      time.Time
	duration time.Duration
}

// calculateTimeChunks breaks down a time range into manageable chunks
func (c *collectorImpl) calculateTimeChunks(start, end time.Time, interval string) []timeChunk {
	// Maximum 300 candles per request from Coinbase API research
	maxCandlesPerChunk := 300

	intervalDuration := c.parseInterval(interval)
	maxChunkDuration := time.Duration(maxCandlesPerChunk) * intervalDuration

	var chunks []timeChunk
	current := start

	for current.Before(end) {
		chunkEnd := current.Add(maxChunkDuration)
		if chunkEnd.After(end) {
			chunkEnd = end
		}

		chunks = append(chunks, timeChunk{
			start:    current,
			end:      chunkEnd,
			duration: chunkEnd.Sub(current),
		})

		current = chunkEnd
	}

	return chunks
}

// parseInterval converts string interval to time.Duration
func (c *collectorImpl) parseInterval(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return time.Hour // fallback
	}
}

// fetchChunkData retrieves data for a specific time chunk
func (c *collectorImpl) fetchChunkData(ctx context.Context, pair, interval string, chunk timeChunk) ([]contracts.Candle, error) {
	// Get request from pool
	req := c.requestPool.Get().(*contracts.FetchRequest)
	defer c.requestPool.Put(req)

	// Configure request
	req.Pair = pair
	req.Start = chunk.start
	req.End = chunk.end
	req.Interval = interval
	req.Limit = 300 // Max from Coinbase API

	// Record request start time for metrics
	startTime := time.Now()

	// Fetch data from exchange
	resp, err := c.exchange.FetchCandles(ctx, *req)
	if err != nil {
		c.metrics.recordError("fetch", err)
		return nil, fmt.Errorf("exchange fetch failed: %w", err)
	}

	// Record response time
	c.metrics.recordResponseTime(time.Since(startTime))

	// Handle rate limiting
	if resp.RateLimit.RetryAfter > 0 {
		c.metrics.recordRateLimitHit()
		c.logger.Debug("Rate limit hit, waiting",
			"retry_after", resp.RateLimit.RetryAfter,
		)

		select {
		case <-time.After(resp.RateLimit.RetryAfter):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return resp.Candles, nil
}

// storeBatch stores a batch of candles with error handling
func (c *collectorImpl) storeBatch(ctx context.Context, candles []contracts.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	startTime := time.Now()

	// Use batch storage for efficiency
	if err := c.storage.StoreBatch(ctx, candles); err != nil {
		return fmt.Errorf("batch storage failed: %w", err)
	}

	// Record storage metrics
	c.metrics.recordCandlesStored(len(candles))
	c.metrics.recordCandlesCollected(len(candles))
	c.logger.Debug("Stored batch of candles",
		"count", len(candles),
		"duration", time.Since(startTime),
	)

	return nil
}

// detectAndStoreGaps performs gap detection and storage
func (c *collectorImpl) detectAndStoreGaps(ctx context.Context, req HistoricalRequest) error {
	gaps, err := c.gaps.DetectGaps(ctx, req.Pair, req.Interval, req.Start, req.End)
	if err != nil {
		return fmt.Errorf("gap detection failed: %w", err)
	}

	// Store detected gaps
	for _, gap := range gaps {
		gapContract := contracts.Gap{
			ID:        gap.ID,
			Pair:      gap.Pair,
			Interval:  gap.Interval,
			StartTime: gap.StartTime,
			EndTime:   gap.EndTime,
			Status:    string(gap.Status),
			CreatedAt: gap.CreatedAt,
		}

		if err := c.storage.StoreGap(ctx, gapContract); err != nil {
			c.logger.Warn("Failed to store gap", "gap_id", gap.ID, "error", err)
			// Continue processing other gaps
		}
	}

	c.logger.Info("Gap detection completed",
		"pair", req.Pair,
		"gaps_detected", len(gaps),
	)

	return nil
}

// validateHistoricalRequest validates the historical collection request
func (c *collectorImpl) validateHistoricalRequest(req HistoricalRequest) error {
	if req.Pair == "" {
		return fmt.Errorf("pair is required")
	}

	if req.Interval == "" {
		return fmt.Errorf("interval is required")
	}

	if req.Start.IsZero() {
		return fmt.Errorf("start time is required")
	}

	if req.End.IsZero() {
		return fmt.Errorf("end time is required")
	}

	if req.Start.After(req.End) {
		return fmt.Errorf("start time must be before end time")
	}

	// Validate interval format
	if c.parseInterval(req.Interval) == 0 {
		return fmt.Errorf("invalid interval format: %s", req.Interval)
	}

	return nil
}

// returnCandlesToPool returns candles slice to the pool for reuse
func (c *collectorImpl) returnCandlesToPool(candles []contracts.Candle) {
	// Clear the slice but keep capacity
	for i := range candles {
		candles[i] = contracts.Candle{} // Zero out for GC
	}

	candleSlice := candles[:0]
	c.candlePool.Put(candleSlice)
}

// startMemoryMonitoring starts background memory usage monitoring
func (c *collectorImpl) startMemoryMonitoring(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(MemoryCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.memoryMgmt.CheckAndReport()

				if c.memoryMgmt.IsOverLimit() {
					c.logger.Warn("Memory usage over limit, forcing GC",
						"usage_mb", c.memoryMgmt.GetUsageMB(),
						"limit_mb", c.config.MemoryLimitMB,
					)
					runtime.GC()
				}

			case <-c.shutdownCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// startMetricsCollection starts background metrics collection
func (c *collectorImpl) startMetricsCollection(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				metrics := c.metrics.getMetrics()
				c.logger.Debug("Collection metrics",
					"candles_collected", metrics.CandlesCollected,
					"success_rate", metrics.SuccessRate,
					"avg_response_time", metrics.AvgResponseTime,
					"error_count", metrics.ErrorCount,
				)

			case <-c.shutdownCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}
