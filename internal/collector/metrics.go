package collector

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/validator"
)

// metricsCollector tracks collection performance and statistics
type metricsCollector struct {
	// Atomic counters for thread-safe updates
	candlesCollected   int64
	errorCount         int64
	successCount       int64
	rateLimitHits      int64
	candlesStored      int64

	// Response time tracking
	totalResponseTime int64 // nanoseconds
	responseCount     int64

	// Validation metrics
	validationResults []*validator.ValidationPipelineResults
	validationMutex   sync.RWMutex

	// Start time for calculating rates
	startTime time.Time
	mutex     sync.RWMutex
}

// newMetricsCollector creates a new metrics collector
func newMetricsCollector() *metricsCollector {
	return &metricsCollector{
		startTime: time.Now(),
	}
}

// recordSuccess records a successful operation
func (m *metricsCollector) recordSuccess(duration time.Duration) {
	atomic.AddInt64(&m.successCount, 1)
	atomic.AddInt64(&m.totalResponseTime, duration.Nanoseconds())
	atomic.AddInt64(&m.responseCount, 1)
}

// recordError records an error occurrence
func (m *metricsCollector) recordError(category string, err error) {
	atomic.AddInt64(&m.errorCount, 1)
}

// recordCandlesCollected records the number of candles collected
func (m *metricsCollector) recordCandlesCollected(count int) {
	atomic.AddInt64(&m.candlesCollected, int64(count))
}

// recordCandlesStored records the number of candles stored
func (m *metricsCollector) recordCandlesStored(count int) {
	atomic.AddInt64(&m.candlesStored, int64(count))
}

// recordResponseTime records API response time
func (m *metricsCollector) recordResponseTime(duration time.Duration) {
	atomic.AddInt64(&m.totalResponseTime, duration.Nanoseconds())
	atomic.AddInt64(&m.responseCount, 1)
}

// recordRateLimitHit records when rate limiting is encountered
func (m *metricsCollector) recordRateLimitHit() {
	atomic.AddInt64(&m.rateLimitHits, 1)
}

// recordValidation records validation results
func (m *metricsCollector) recordValidation(results *validator.ValidationPipelineResults) {
	m.validationMutex.Lock()
	defer m.validationMutex.Unlock()

	// Keep only recent validation results (last 100)
	if len(m.validationResults) >= 100 {
		// Remove oldest result
		m.validationResults = m.validationResults[1:]
	}

	m.validationResults = append(m.validationResults, results)
}

// getMetrics returns current metrics snapshot
func (m *metricsCollector) getMetrics() *CollectionMetrics {
	candlesCollected := atomic.LoadInt64(&m.candlesCollected)
	errorCount := atomic.LoadInt64(&m.errorCount)
	successCount := atomic.LoadInt64(&m.successCount)
	rateLimitHits := atomic.LoadInt64(&m.rateLimitHits)
	totalResponseTime := atomic.LoadInt64(&m.totalResponseTime)
	responseCount := atomic.LoadInt64(&m.responseCount)

	// Calculate success rate
	totalOperations := successCount + errorCount
	var successRate float64
	if totalOperations > 0 {
		successRate = float64(successCount) / float64(totalOperations)
	}

	// Calculate average response time
	var avgResponseTime time.Duration
	if responseCount > 0 {
		avgResponseTime = time.Duration(totalResponseTime / responseCount)
	}

	return &CollectionMetrics{
		CandlesCollected:  candlesCollected,
		ErrorCount:        errorCount,
		SuccessRate:       successRate,
		AvgResponseTime:   avgResponseTime,
		RateLimitHits:     rateLimitHits,
		ActiveConnections: 1, // Placeholder - would be tracked separately
	}
}

// getValidationStats calculates aggregate validation statistics
func (m *metricsCollector) getValidationStats() *ValidationStats {
	m.validationMutex.RLock()
	defer m.validationMutex.RUnlock()

	if len(m.validationResults) == 0 {
		return &ValidationStats{}
	}

	var totalValidated, validationErrors, anomaliesDetected int64
	var qualityScores []float64

	for _, result := range m.validationResults {
		totalValidated += result.ProcessedCandles
		validationErrors += result.InvalidCandles
		
		if result.QualityMetrics != nil {
			anomaliesDetected += result.QualityMetrics.AnomaliesDetected
			qualityScores = append(qualityScores, result.QualityMetrics.QualityScore)
		}
	}

	// Calculate average quality score
	var avgQualityScore float64
	if len(qualityScores) > 0 {
		var sum float64
		for _, score := range qualityScores {
			sum += score
		}
		avgQualityScore = sum / float64(len(qualityScores))
	}

	return &ValidationStats{
		TotalValidated:    totalValidated,
		ValidationErrors:  validationErrors,
		AnomaliesDetected: anomaliesDetected,
		QualityScore:      avgQualityScore,
	}
}

// reset resets all metrics (useful for testing)
func (m *metricsCollector) reset() {
	atomic.StoreInt64(&m.candlesCollected, 0)
	atomic.StoreInt64(&m.errorCount, 0)
	atomic.StoreInt64(&m.successCount, 0)
	atomic.StoreInt64(&m.rateLimitHits, 0)
	atomic.StoreInt64(&m.candlesStored, 0)
	atomic.StoreInt64(&m.totalResponseTime, 0)
	atomic.StoreInt64(&m.responseCount, 0)

	m.validationMutex.Lock()
	m.validationResults = nil
	m.validationMutex.Unlock()

	m.mutex.Lock()
	m.startTime = time.Now()
	m.mutex.Unlock()
}