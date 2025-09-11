// Package metrics provides comprehensive metrics collection and health monitoring
// for the OHLCV data collector. This module implements Prometheus-style metrics
// with configurable collection intervals and health check endpoints.
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/config"
	"github.com/johnayoung/go-ohlcv-collector/internal/logger"
)

// MetricsCollector manages application metrics and health monitoring
type MetricsCollector struct {
	config      config.MetricsConfig
	logger      *logger.ComponentLogger
	server      *http.Server
	mu          sync.RWMutex
	metrics     map[string]Metric
	healthCheck HealthChecker
	startTime   time.Time
	collectors  []MetricCollector
	
	// Performance counters
	requestCount    int64
	errorCount      int64
	lastUpdateTime  time.Time
	updateTicker    *time.Ticker
	stopChan        chan struct{}
}

// Metric represents a single metric with metadata
type Metric struct {
	Name        string                 `json:"name"`
	Type        MetricType             `json:"type"`
	Value       float64                `json:"value"`
	Labels      map[string]string      `json:"labels,omitempty"`
	Description string                 `json:"description"`
	UpdatedAt   time.Time              `json:"updated_at"`
	History     []MetricDataPoint      `json:"history,omitempty"`
}

// MetricType represents different types of metrics
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// MetricDataPoint represents a time-series data point
type MetricDataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// MetricCollector interface for components that provide metrics
type MetricCollector interface {
	CollectMetrics(ctx context.Context) ([]Metric, error)
	GetMetricNames() []string
}

// HealthChecker interface for components that provide health status
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
	GetHealthStatus() HealthStatus
}

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Status      string            `json:"status"`
	Timestamp   time.Time         `json:"timestamp"`
	Duration    time.Duration     `json:"duration"`
	Details     map[string]string `json:"details,omitempty"`
	Dependencies []string         `json:"dependencies,omitempty"`
}

// MetricsSnapshot represents a snapshot of all metrics at a point in time
type MetricsSnapshot struct {
	Timestamp      time.Time         `json:"timestamp"`
	Uptime         time.Duration     `json:"uptime"`
	Metrics        map[string]Metric `json:"metrics"`
	SystemMetrics  SystemMetrics     `json:"system_metrics"`
	HealthStatus   map[string]HealthStatus `json:"health_status"`
	RequestCount   int64             `json:"request_count"`
	ErrorCount     int64             `json:"error_count"`
	ErrorRate      float64           `json:"error_rate"`
}

// SystemMetrics represents system-level metrics
type SystemMetrics struct {
	CPUPercent      float64           `json:"cpu_percent"`
	MemoryUsed      int64             `json:"memory_used"`
	MemoryPercent   float64           `json:"memory_percent"`
	GoroutineCount  int               `json:"goroutine_count"`
	GCPauseNs       uint64            `json:"gc_pause_ns"`
	NumGC           uint32            `json:"num_gc"`
	HeapAlloc       uint64            `json:"heap_alloc"`
	HeapSys         uint64            `json:"heap_sys"`
	HeapIdle        uint64            `json:"heap_idle"`
	HeapInuse       uint64            `json:"heap_inuse"`
	StackInuse      uint64            `json:"stack_inuse"`
	StackSys        uint64            `json:"stack_sys"`
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(cfg config.MetricsConfig, loggerMgr *logger.LoggerManager) *MetricsCollector {
	componentLogger := loggerMgr.GetComponentLogger("metrics")
	
	mc := &MetricsCollector{
		config:      cfg,
		logger:      componentLogger,
		metrics:     make(map[string]Metric),
		startTime:   time.Now(),
		collectors:  make([]MetricCollector, 0),
		stopChan:    make(chan struct{}),
	}

	return mc
}

// Start initializes and starts the metrics collection system
func (mc *MetricsCollector) Start(ctx context.Context) error {
	if !mc.config.Enabled {
		mc.logger.Info("metrics collection disabled")
		return nil
	}

	mc.logger.Info("starting metrics collector",
		"port", mc.config.Port,
		"path", mc.config.Path,
		"update_interval", mc.config.UpdateInterval)

	// Parse update interval
	updateInterval, err := time.ParseDuration(mc.config.UpdateInterval)
	if err != nil {
		return fmt.Errorf("invalid update interval: %w", err)
	}

	// Start periodic metrics collection
	mc.updateTicker = time.NewTicker(updateInterval)
	go mc.collectMetricsLoop(ctx)

	// Start HTTP server if port is configured
	if mc.config.Port > 0 {
		if err := mc.startHTTPServer(); err != nil {
			return fmt.Errorf("failed to start metrics HTTP server: %w", err)
		}
	}

	return nil
}

// Stop gracefully stops the metrics collection system
func (mc *MetricsCollector) Stop(ctx context.Context) error {
	mc.logger.Info("stopping metrics collector")

	// Signal stop to background goroutines
	close(mc.stopChan)

	// Stop update ticker
	if mc.updateTicker != nil {
		mc.updateTicker.Stop()
	}

	// Shutdown HTTP server
	if mc.server != nil {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := mc.server.Shutdown(ctx); err != nil {
			mc.logger.ErrorWithContext(ctx, "error shutting down metrics server", err)
			return err
		}
	}

	mc.logger.Info("metrics collector stopped")
	return nil
}

// RegisterCollector registers a metric collector component
func (mc *MetricsCollector) RegisterCollector(collector MetricCollector) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.collectors = append(mc.collectors, collector)
	mc.logger.Debug("registered metric collector", 
		"metric_names", collector.GetMetricNames())
}

// RegisterHealthChecker registers a health checker component
func (mc *MetricsCollector) RegisterHealthChecker(checker HealthChecker) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.healthCheck = checker
	mc.logger.Debug("registered health checker")
}

// RecordCounter increments a counter metric
func (mc *MetricsCollector) RecordCounter(name, description string, labels map[string]string) {
	mc.recordMetric(name, MetricTypeCounter, 1, description, labels)
	atomic.AddInt64(&mc.requestCount, 1)
}

// RecordGauge sets a gauge metric value
func (mc *MetricsCollector) RecordGauge(name string, value float64, description string, labels map[string]string) {
	mc.recordMetric(name, MetricTypeGauge, value, description, labels)
}

// RecordError records an error metric
func (mc *MetricsCollector) RecordError(name, description string, labels map[string]string) {
	mc.recordMetric(name, MetricTypeCounter, 1, description, labels)
	atomic.AddInt64(&mc.errorCount, 1)
}

// RecordDuration records a duration metric in milliseconds
func (mc *MetricsCollector) RecordDuration(name string, duration time.Duration, description string, labels map[string]string) {
	ms := float64(duration.Nanoseconds()) / float64(time.Millisecond)
	mc.recordMetric(name, MetricTypeHistogram, ms, description, labels)
}

// recordMetric is the internal method for recording metrics
func (mc *MetricsCollector) recordMetric(name string, metricType MetricType, value float64, description string, labels map[string]string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	now := time.Now()
	
	existing, exists := mc.metrics[name]
	if exists {
		// Update existing metric
		if metricType == MetricTypeCounter {
			existing.Value += value
		} else {
			existing.Value = value
		}
		existing.UpdatedAt = now
		
		// Add to history (keep last 100 points)
		existing.History = append(existing.History, MetricDataPoint{
			Timestamp: now,
			Value:     existing.Value,
		})
		if len(existing.History) > 100 {
			existing.History = existing.History[1:]
		}
		
		mc.metrics[name] = existing
	} else {
		// Create new metric
		metric := Metric{
			Name:        name,
			Type:        metricType,
			Value:       value,
			Labels:      labels,
			Description: description,
			UpdatedAt:   now,
			History: []MetricDataPoint{
				{Timestamp: now, Value: value},
			},
		}
		mc.metrics[name] = metric
	}
}

// collectMetricsLoop runs the periodic metrics collection
func (mc *MetricsCollector) collectMetricsLoop(ctx context.Context) {
	for {
		select {
		case <-mc.updateTicker.C:
			if err := mc.collectAllMetrics(ctx); err != nil {
				mc.logger.ErrorWithContext(ctx, "failed to collect metrics", err)
			}
		case <-mc.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// collectAllMetrics collects metrics from all registered collectors
func (mc *MetricsCollector) collectAllMetrics(ctx context.Context) error {
	mc.mu.RLock()
	collectors := make([]MetricCollector, len(mc.collectors))
	copy(collectors, mc.collectors)
	mc.mu.RUnlock()

	// Collect system metrics
	mc.collectSystemMetrics()

	// Collect metrics from registered collectors
	for _, collector := range collectors {
		metrics, err := collector.CollectMetrics(ctx)
		if err != nil {
			mc.logger.ErrorWithContext(ctx, "collector failed to provide metrics", err)
			continue
		}

		// Store collected metrics
		mc.mu.Lock()
		for _, metric := range metrics {
			mc.metrics[metric.Name] = metric
		}
		mc.mu.Unlock()
	}

	mc.lastUpdateTime = time.Now()
	return nil
}

// collectSystemMetrics collects system-level metrics
func (mc *MetricsCollector) collectSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Record system metrics
	mc.RecordGauge("system_goroutines", float64(runtime.NumGoroutine()), "Number of goroutines", nil)
	mc.RecordGauge("system_memory_heap_alloc", float64(m.HeapAlloc), "Heap allocation in bytes", nil)
	mc.RecordGauge("system_memory_heap_sys", float64(m.HeapSys), "Heap system memory in bytes", nil)
	mc.RecordGauge("system_memory_heap_idle", float64(m.HeapIdle), "Heap idle memory in bytes", nil)
	mc.RecordGauge("system_memory_heap_inuse", float64(m.HeapInuse), "Heap in-use memory in bytes", nil)
	mc.RecordGauge("system_memory_stack_inuse", float64(m.StackInuse), "Stack in-use memory in bytes", nil)
	mc.RecordGauge("system_memory_stack_sys", float64(m.StackSys), "Stack system memory in bytes", nil)
	mc.RecordCounter("system_gc_runs", "Total number of GC runs", nil)
	mc.RecordGauge("system_gc_pause_ns", float64(m.PauseTotalNs), "Total GC pause time in nanoseconds", nil)
}

// GetSnapshot returns a snapshot of all current metrics
func (mc *MetricsCollector) GetSnapshot() MetricsSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Copy metrics
	metricsCopy := make(map[string]Metric)
	for k, v := range mc.metrics {
		metricsCopy[k] = v
	}

	// Calculate error rate
	requestCount := atomic.LoadInt64(&mc.requestCount)
	errorCount := atomic.LoadInt64(&mc.errorCount)
	var errorRate float64
	if requestCount > 0 {
		errorRate = float64(errorCount) / float64(requestCount) * 100
	}

	// Get system metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	systemMetrics := SystemMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		NumGC:         m.NumGC,
		GCPauseNs:     m.PauseTotalNs,
		HeapAlloc:     m.HeapAlloc,
		HeapSys:       m.HeapSys,
		HeapIdle:      m.HeapIdle,
		HeapInuse:     m.HeapInuse,
		StackInuse:    m.StackInuse,
		StackSys:      m.StackSys,
	}

	// Get health status
	healthStatus := make(map[string]HealthStatus)
	if mc.healthCheck != nil {
		healthStatus["application"] = mc.healthCheck.GetHealthStatus()
	}

	return MetricsSnapshot{
		Timestamp:     time.Now(),
		Uptime:        time.Since(mc.startTime),
		Metrics:       metricsCopy,
		SystemMetrics: systemMetrics,
		HealthStatus:  healthStatus,
		RequestCount:  requestCount,
		ErrorCount:    errorCount,
		ErrorRate:     errorRate,
	}
}

// startHTTPServer starts the metrics HTTP server
func (mc *MetricsCollector) startHTTPServer() error {
	mux := http.NewServeMux()
	
	// Metrics endpoint
	mux.HandleFunc(mc.config.Path, mc.handleMetrics)
	
	// Health check endpoint
	mux.HandleFunc("/health", mc.handleHealth)
	mux.HandleFunc("/ready", mc.handleReadiness)
	
	// Debug endpoints
	mux.HandleFunc("/debug/metrics", mc.handleDebugMetrics)
	
	mc.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", mc.config.Port),
		Handler: mux,
	}

	go func() {
		mc.logger.Info("metrics HTTP server starting", "addr", mc.server.Addr)
		if err := mc.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			mc.logger.Error("metrics HTTP server failed", "error", err)
		}
	}()

	return nil
}

// handleMetrics handles the metrics endpoint
func (mc *MetricsCollector) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := mc.GetSnapshot()
	
	w.Header().Set("Content-Type", "application/json")
	
	// Simple format compatible with Prometheus
	output := make(map[string]interface{})
	for name, metric := range snapshot.Metrics {
		output[name] = map[string]interface{}{
			"value":       metric.Value,
			"type":        metric.Type,
			"description": metric.Description,
			"labels":      metric.Labels,
			"updated_at":  metric.UpdatedAt,
		}
	}
	
	json.NewEncoder(w).Encode(output)
}

// handleHealth handles the health check endpoint
func (mc *MetricsCollector) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	
	status := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    time.Since(mc.startTime),
	}

	// Check health checker if available
	if mc.healthCheck != nil {
		if err := mc.healthCheck.HealthCheck(ctx); err != nil {
			status["status"] = "unhealthy"
			status["error"] = err.Error()
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleReadiness handles the readiness probe endpoint
func (mc *MetricsCollector) handleReadiness(w http.ResponseWriter, r *http.Request) {
	// Check if metrics collection is working
	if time.Since(mc.lastUpdateTime) > 5*time.Minute {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"reason": "metrics collection stalled",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ready",
		"timestamp":  time.Now(),
		"last_update": mc.lastUpdateTime,
	})
}

// handleDebugMetrics provides detailed metrics for debugging
func (mc *MetricsCollector) handleDebugMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := mc.GetSnapshot()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshot)
}

// CollectorMetrics implements MetricCollector for self-monitoring
type CollectorMetrics struct {
	collector *MetricsCollector
}

// NewCollectorMetrics creates a self-monitoring collector
func NewCollectorMetrics(collector *MetricsCollector) *CollectorMetrics {
	return &CollectorMetrics{collector: collector}
}

// CollectMetrics collects metrics about the metrics collector itself
func (cm *CollectorMetrics) CollectMetrics(ctx context.Context) ([]Metric, error) {
	cm.collector.mu.RLock()
	defer cm.collector.mu.RUnlock()

	now := time.Now()
	metrics := []Metric{
		{
			Name:        "metrics_collector_uptime",
			Type:        MetricTypeGauge,
			Value:       time.Since(cm.collector.startTime).Seconds(),
			Description: "Metrics collector uptime in seconds",
			UpdatedAt:   now,
		},
		{
			Name:        "metrics_collector_metrics_count",
			Type:        MetricTypeGauge,
			Value:       float64(len(cm.collector.metrics)),
			Description: "Number of metrics being tracked",
			UpdatedAt:   now,
		},
		{
			Name:        "metrics_collector_collectors_count",
			Type:        MetricTypeGauge,
			Value:       float64(len(cm.collector.collectors)),
			Description: "Number of registered metric collectors",
			UpdatedAt:   now,
		},
	}

	return metrics, nil
}

// GetMetricNames returns the metric names this collector provides
func (cm *CollectorMetrics) GetMetricNames() []string {
	return []string{
		"metrics_collector_uptime",
		"metrics_collector_metrics_count", 
		"metrics_collector_collectors_count",
	}
}

// Simple health checker implementation
type SimpleHealthChecker struct {
	name         string
	logger       *logger.ComponentLogger
	dependencies []string
	lastCheck    time.Time
	lastResult   error
	mu           sync.RWMutex
}

// NewSimpleHealthChecker creates a basic health checker
func NewSimpleHealthChecker(name string, logger *logger.ComponentLogger) *SimpleHealthChecker {
	return &SimpleHealthChecker{
		name:         name,
		logger:       logger,
		dependencies: make([]string, 0),
	}
}

// AddDependency adds a dependency to check
func (shc *SimpleHealthChecker) AddDependency(name string) {
	shc.mu.Lock()
	defer shc.mu.Unlock()
	shc.dependencies = append(shc.dependencies, name)
}

// HealthCheck performs a health check
func (shc *SimpleHealthChecker) HealthCheck(ctx context.Context) error {
	shc.mu.Lock()
	defer shc.mu.Unlock()

	start := time.Now()
	defer func() {
		shc.lastCheck = time.Now()
	}()

	// Perform basic checks
	if len(shc.dependencies) > 0 {
		for _, dep := range shc.dependencies {
			// In a real implementation, you would check each dependency
			shc.logger.DebugWithContext(ctx, "checking dependency", "dependency", dep)
		}
	}

	// Check if context is canceled
	if ctx.Err() != nil {
		shc.lastResult = ctx.Err()
		return ctx.Err()
	}

	// Simulate some health check logic
	if time.Since(start) > time.Second {
		shc.lastResult = fmt.Errorf("health check took too long")
		return shc.lastResult
	}

	shc.lastResult = nil
	return nil
}

// GetHealthStatus returns the current health status
func (shc *SimpleHealthChecker) GetHealthStatus() HealthStatus {
	shc.mu.RLock()
	defer shc.mu.RUnlock()

	status := "healthy"
	if shc.lastResult != nil {
		status = "unhealthy"
	}

	details := make(map[string]string)
	if shc.lastResult != nil {
		details["error"] = shc.lastResult.Error()
	}
	details["last_check"] = shc.lastCheck.Format(time.RFC3339)

	return HealthStatus{
		Status:       status,
		Timestamp:    time.Now(),
		Duration:     time.Since(shc.lastCheck),
		Details:      details,
		Dependencies: shc.dependencies,
	}
}