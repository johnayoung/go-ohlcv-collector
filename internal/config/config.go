// Package config provides centralized configuration management for all OHLCV collector components.
// This module handles configuration loading from multiple sources (files, environment variables),
// validation, and provides typed configuration structures for different adapters and services.
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"log/slog"
)

// AppConfig represents the complete application configuration
type AppConfig struct {
	// Application metadata
	AppName    string `json:"app_name" env:"APP_NAME"`
	Version    string `json:"version" env:"VERSION"`
	ConfigPath string `json:"-" env:"CONFIG_PATH"`

	// Storage configuration
	Storage StorageConfig `json:"storage"`

	// Exchange configuration
	Exchange ExchangeConfig `json:"exchange"`

	// Collector configuration
	Collector CollectorConfig `json:"collector"`

	// Scheduler configuration
	Scheduler SchedulerConfig `json:"scheduler"`

	// Validator configuration
	Validator ValidatorConfig `json:"validator"`

	// Logging configuration
	Logging LoggingConfig `json:"logging"`

	// Metrics configuration
	Metrics MetricsConfig `json:"metrics"`

	// Error handling configuration
	ErrorHandling ErrorHandlingConfig `json:"error_handling"`
}

// StorageConfig configures the storage backend
type StorageConfig struct {
	Type         string `json:"type" env:"STORAGE_TYPE"`           // "duckdb", "memory", "postgresql"
	DatabaseURL  string `json:"database_url" env:"DATABASE_URL"`   // Database connection string
	BatchSize    int    `json:"batch_size" env:"BATCH_SIZE"`       // Batch size for bulk operations
	MaxConns     int    `json:"max_conns" env:"MAX_CONNS"`         // Maximum database connections
	IdleTimeout  string `json:"idle_timeout" env:"IDLE_TIMEOUT"`   // Connection idle timeout
	QueryTimeout string `json:"query_timeout" env:"QUERY_TIMEOUT"` // Query execution timeout
}

// ExchangeConfig configures exchange adapters
type ExchangeConfig struct {
	Type         string                 `json:"type" env:"EXCHANGE_TYPE"`           // "coinbase", "mock"
	APIKey       string                 `json:"api_key" env:"API_KEY"`              // API key for authenticated requests
	APISecret    string                 `json:"api_secret" env:"API_SECRET"`        // API secret for authenticated requests
	Sandbox      bool                   `json:"sandbox" env:"SANDBOX"`              // Use sandbox/test environment
	RateLimit    int                    `json:"rate_limit" env:"RATE_LIMIT"`        // Requests per minute
	Timeout      string                 `json:"timeout" env:"HTTP_TIMEOUT"`         // HTTP request timeout
	RetryPolicy  RetryPolicyConfig      `json:"retry_policy"`                       // Retry configuration
	Exchanges    map[string]interface{} `json:"exchanges"`                          // Exchange-specific configurations
}

// CollectorConfig configures the data collection system
type CollectorConfig struct {
	WorkerCount     int    `json:"worker_count" env:"WORKER_COUNT"`         // Number of worker goroutines
	BatchSize       int    `json:"batch_size" env:"BATCH_SIZE"`             // Batch size for data collection
	ChannelSize     int    `json:"channel_size" env:"CHANNEL_SIZE"`         // Internal channel buffer size
	MaxMemoryMB     int    `json:"max_memory_mb" env:"MAX_MEMORY_MB"`       // Maximum memory usage in MB
	GracefulTimeout string `json:"graceful_timeout" env:"GRACEFUL_TIMEOUT"` // Graceful shutdown timeout
	RetryAttempts   int    `json:"retry_attempts" env:"RETRY_ATTEMPTS"`     // Maximum retry attempts
}

// SchedulerConfig configures the data collection scheduler
type SchedulerConfig struct {
	Enabled             bool     `json:"enabled" env:"SCHEDULER_ENABLED"`                     // Enable scheduled collection
	Interval            string   `json:"interval" env:"SCHEDULER_INTERVAL"`                   // Collection interval
	MaxConcurrentJobs   int      `json:"max_concurrent_jobs" env:"MAX_CONCURRENT_JOBS"`       // Maximum concurrent jobs
	JobTimeout          string   `json:"job_timeout" env:"JOB_TIMEOUT"`                       // Job execution timeout
	EnableHourAlignment bool     `json:"enable_hour_alignment" env:"ENABLE_HOUR_ALIGNMENT"`   // Align to hour boundaries
	TimezoneLocation    string   `json:"timezone_location" env:"TIMEZONE_LOCATION"`           // Timezone for scheduling
	DefaultPairs        []string `json:"default_pairs" env:"DEFAULT_PAIRS"`                   // Default trading pairs to collect
	DefaultIntervals    []string `json:"default_intervals" env:"DEFAULT_INTERVALS"`           // Default time intervals
}

// ValidatorConfig configures data validation
type ValidatorConfig struct {
	Enabled                 bool              `json:"enabled" env:"VALIDATOR_ENABLED"`                       // Enable validation
	PriceSpikeThreshold     float64           `json:"price_spike_threshold" env:"PRICE_SPIKE_THRESHOLD"`     // Price spike detection threshold
	VolumeSurgeThreshold    float64           `json:"volume_surge_threshold" env:"VOLUME_SURGE_THRESHOLD"`   // Volume surge detection threshold
	MinConfidenceScore      float64           `json:"min_confidence_score" env:"MIN_CONFIDENCE_SCORE"`       // Minimum confidence score
	MaxLookbackPeriods      int               `json:"max_lookback_periods" env:"MAX_LOOKBACK_PERIODS"`       // Historical data periods for validation
	EnabledChecks           []string          `json:"enabled_checks" env:"ENABLED_CHECKS"`                   // List of enabled validation checks
	SeverityActions         map[string]string `json:"severity_actions"`                                       // Actions for different severity levels
	BatchSize               int               `json:"batch_size" env:"VALIDATION_BATCH_SIZE"`                // Validation batch size
	ProcessingTimeout       string            `json:"processing_timeout" env:"VALIDATION_PROCESSING_TIMEOUT"` // Validation processing timeout
}

// LoggingConfig configures structured logging
type LoggingConfig struct {
	Level         string            `json:"level" env:"LOG_LEVEL"`           // Log level: debug, info, warn, error
	Format        string            `json:"format" env:"LOG_FORMAT"`         // Log format: json, text
	Output        string            `json:"output" env:"LOG_OUTPUT"`         // Output: stdout, stderr, file
	FilePath      string            `json:"file_path" env:"LOG_FILE_PATH"`   // Log file path
	MaxSize       int               `json:"max_size" env:"LOG_MAX_SIZE"`     // Maximum log file size in MB
	MaxBackups    int               `json:"max_backups" env:"LOG_MAX_BACKUPS"` // Maximum log file backups
	MaxAge        int               `json:"max_age" env:"LOG_MAX_AGE"`       // Maximum log file age in days
	Compress      bool              `json:"compress" env:"LOG_COMPRESS"`     // Compress old log files
	ContextFields map[string]string `json:"context_fields"`                  // Additional context fields
}

// MetricsConfig configures metrics collection
type MetricsConfig struct {
	Enabled         bool     `json:"enabled" env:"METRICS_ENABLED"`             // Enable metrics collection
	Port            int      `json:"port" env:"METRICS_PORT"`                   // Metrics server port
	Path            string   `json:"path" env:"METRICS_PATH"`                   // Metrics endpoint path
	UpdateInterval  string   `json:"update_interval" env:"METRICS_UPDATE_INTERVAL"` // Metrics update interval
	HistoryDuration string   `json:"history_duration" env:"METRICS_HISTORY_DURATION"` // How long to keep metrics history
	EnabledMetrics  []string `json:"enabled_metrics" env:"ENABLED_METRICS"`     // List of enabled metrics
}

// ErrorHandlingConfig configures error handling and retry policies
type ErrorHandlingConfig struct {
	GlobalRetryPolicy RetryPolicyConfig       `json:"global_retry_policy"`          // Global retry policy
	ComponentPolicies map[string]RetryPolicyConfig `json:"component_policies"`    // Component-specific retry policies
	FallbackBehavior  string                  `json:"fallback_behavior" env:"FALLBACK_BEHAVIOR"` // Behavior when all retries fail
	EnableCircuitBreaker bool                 `json:"enable_circuit_breaker" env:"ENABLE_CIRCUIT_BREAKER"` // Enable circuit breaker pattern
	CircuitBreakerConfig CircuitBreakerConfig `json:"circuit_breaker_config"`       // Circuit breaker configuration
}

// RetryPolicyConfig configures retry behavior
type RetryPolicyConfig struct {
	MaxAttempts     int      `json:"max_attempts"`     // Maximum retry attempts
	InitialDelay    string   `json:"initial_delay"`    // Initial delay between retries
	MaxDelay        string   `json:"max_delay"`        // Maximum delay between retries
	BackoffStrategy string   `json:"backoff_strategy"` // Backoff strategy: fixed, exponential, linear
	RetryableErrors []string `json:"retryable_errors"` // List of retryable error types
	Jitter          bool     `json:"jitter"`           // Add randomness to delays
}

// CircuitBreakerConfig configures circuit breaker behavior
type CircuitBreakerConfig struct {
	FailureThreshold int    `json:"failure_threshold"`  // Number of failures to open circuit
	RecoveryTimeout  string `json:"recovery_timeout"`   // Time before attempting recovery
	HalfOpenRequests int    `json:"half_open_requests"` // Number of test requests in half-open state
}

// ConfigManager handles configuration loading, validation, and hot-reloading
type ConfigManager struct {
	config      *AppConfig
	configPath  string
	logger      *slog.Logger
	watchers    []ConfigWatcher
	reloadChan  chan struct{}
}

// ConfigWatcher defines an interface for components that need to be notified of config changes
type ConfigWatcher interface {
	OnConfigUpdate(ctx context.Context, config *AppConfig) error
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(configPath string, logger *slog.Logger) *ConfigManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &ConfigManager{
		configPath: configPath,
		logger:     logger,
		reloadChan: make(chan struct{}, 1),
	}
}

// LoadConfig loads configuration from multiple sources with priority order:
// 1. Environment variables (highest priority)
// 2. Configuration file
// 3. Default values (lowest priority)
func (cm *ConfigManager) LoadConfig(ctx context.Context) (*AppConfig, error) {
	config := DefaultConfig()

	// Load from configuration file if it exists
	if cm.configPath != "" {
		if err := cm.loadFromFile(config); err != nil {
			return nil, fmt.Errorf("failed to load config from file: %w", err)
		}
	}

	// Override with environment variables
	if err := cm.loadFromEnv(config); err != nil {
		return nil, fmt.Errorf("failed to load config from environment: %w", err)
	}

	// Validate the final configuration
	if err := cm.validateConfig(config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	cm.config = config
	cm.logger.Info("configuration loaded successfully",
		"config_path", cm.configPath,
		"storage_type", config.Storage.Type,
		"exchange_type", config.Exchange.Type,
		"log_level", config.Logging.Level)

	return config, nil
}

// loadFromFile loads configuration from a JSON file
func (cm *ConfigManager) loadFromFile(config *AppConfig) error {
	if _, err := os.Stat(cm.configPath); os.IsNotExist(err) {
		cm.logger.Debug("config file does not exist, using defaults", "path", cm.configPath)
		return nil
	}

	data, err := os.ReadFile(cm.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", cm.configPath, err)
	}

	if err := json.Unmarshal(data, config); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", cm.configPath, err)
	}

	cm.logger.Debug("loaded configuration from file", "path", cm.configPath)
	return nil
}

// loadFromEnv loads configuration from environment variables
func (cm *ConfigManager) loadFromEnv(config *AppConfig) error {
	// Load main application config
	if val := os.Getenv("APP_NAME"); val != "" {
		config.AppName = val
	}
	if val := os.Getenv("VERSION"); val != "" {
		config.Version = val
	}

	// Load storage config
	if val := os.Getenv("STORAGE_TYPE"); val != "" {
		config.Storage.Type = val
	}
	if val := os.Getenv("DATABASE_URL"); val != "" {
		config.Storage.DatabaseURL = val
	}
	if val := os.Getenv("BATCH_SIZE"); val != "" {
		if batchSize, err := strconv.Atoi(val); err == nil {
			config.Storage.BatchSize = batchSize
			config.Collector.BatchSize = batchSize // Also update collector batch size
		}
	}
	if val := os.Getenv("MAX_CONNS"); val != "" {
		if maxConns, err := strconv.Atoi(val); err == nil {
			config.Storage.MaxConns = maxConns
		}
	}

	// Load exchange config
	if val := os.Getenv("EXCHANGE_TYPE"); val != "" {
		config.Exchange.Type = val
	}
	if val := os.Getenv("API_KEY"); val != "" {
		config.Exchange.APIKey = val
	}
	if val := os.Getenv("API_SECRET"); val != "" {
		config.Exchange.APISecret = val
	}
	if val := os.Getenv("SANDBOX"); val != "" {
		config.Exchange.Sandbox = val == "true"
	}
	if val := os.Getenv("RATE_LIMIT"); val != "" {
		if rateLimit, err := strconv.Atoi(val); err == nil {
			config.Exchange.RateLimit = rateLimit
		}
	}

	// Load collector config
	if val := os.Getenv("WORKER_COUNT"); val != "" {
		if workerCount, err := strconv.Atoi(val); err == nil {
			config.Collector.WorkerCount = workerCount
		}
	}
	if val := os.Getenv("RETRY_ATTEMPTS"); val != "" {
		if retryAttempts, err := strconv.Atoi(val); err == nil {
			config.Collector.RetryAttempts = retryAttempts
		}
	}

	// Load scheduler config
	if val := os.Getenv("SCHEDULER_ENABLED"); val != "" {
		config.Scheduler.Enabled = val == "true"
	}
	if val := os.Getenv("SCHEDULER_INTERVAL"); val != "" {
		config.Scheduler.Interval = val
	}
	if val := os.Getenv("MAX_CONCURRENT_JOBS"); val != "" {
		if maxJobs, err := strconv.Atoi(val); err == nil {
			config.Scheduler.MaxConcurrentJobs = maxJobs
		}
	}
	if val := os.Getenv("DEFAULT_PAIRS"); val != "" {
		config.Scheduler.DefaultPairs = strings.Split(val, ",")
	}
	if val := os.Getenv("DEFAULT_INTERVALS"); val != "" {
		config.Scheduler.DefaultIntervals = strings.Split(val, ",")
	}

	// Load validator config
	if val := os.Getenv("VALIDATOR_ENABLED"); val != "" {
		config.Validator.Enabled = val == "true"
	}
	if val := os.Getenv("PRICE_SPIKE_THRESHOLD"); val != "" {
		if threshold, err := strconv.ParseFloat(val, 64); err == nil {
			config.Validator.PriceSpikeThreshold = threshold
		}
	}
	if val := os.Getenv("VOLUME_SURGE_THRESHOLD"); val != "" {
		if threshold, err := strconv.ParseFloat(val, 64); err == nil {
			config.Validator.VolumeSurgeThreshold = threshold
		}
	}

	// Load logging config
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		config.Logging.Level = val
	}
	if val := os.Getenv("LOG_FORMAT"); val != "" {
		config.Logging.Format = val
	}
	if val := os.Getenv("LOG_OUTPUT"); val != "" {
		config.Logging.Output = val
	}
	if val := os.Getenv("LOG_FILE_PATH"); val != "" {
		config.Logging.FilePath = val
	}

	// Load metrics config
	if val := os.Getenv("METRICS_ENABLED"); val != "" {
		config.Metrics.Enabled = val == "true"
	}
	if val := os.Getenv("METRICS_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			config.Metrics.Port = port
		}
	}

	cm.logger.Debug("loaded configuration from environment variables")
	return nil
}

// validateConfig validates the configuration for consistency and required fields
func (cm *ConfigManager) validateConfig(config *AppConfig) error {
	var errors []string

	// Validate storage configuration
	if config.Storage.Type == "" {
		errors = append(errors, "storage.type is required")
	}
	if config.Storage.Type == "duckdb" && config.Storage.DatabaseURL == "" {
		errors = append(errors, "storage.database_url is required for DuckDB storage")
	}
	if config.Storage.BatchSize <= 0 {
		errors = append(errors, "storage.batch_size must be greater than 0")
	}

	// Validate exchange configuration
	if config.Exchange.Type == "" {
		errors = append(errors, "exchange.type is required")
	}
	if config.Exchange.RateLimit <= 0 {
		errors = append(errors, "exchange.rate_limit must be greater than 0")
	}

	// Validate collector configuration
	if config.Collector.WorkerCount <= 0 {
		errors = append(errors, "collector.worker_count must be greater than 0")
	}
	if config.Collector.BatchSize <= 0 {
		errors = append(errors, "collector.batch_size must be greater than 0")
	}

	// Validate scheduler configuration
	if config.Scheduler.Enabled {
		if config.Scheduler.Interval == "" {
			errors = append(errors, "scheduler.interval is required when scheduler is enabled")
		}
		if _, err := time.ParseDuration(config.Scheduler.Interval); err != nil {
			errors = append(errors, fmt.Sprintf("scheduler.interval is not a valid duration: %v", err))
		}
		if config.Scheduler.MaxConcurrentJobs <= 0 {
			errors = append(errors, "scheduler.max_concurrent_jobs must be greater than 0")
		}
	}

	// Validate logging configuration
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[config.Logging.Level] {
		errors = append(errors, "logging.level must be one of: debug, info, warn, error")
	}

	validLogFormats := map[string]bool{"json": true, "text": true}
	if !validLogFormats[config.Logging.Format] {
		errors = append(errors, "logging.format must be one of: json, text")
	}

	// Validate metrics configuration
	if config.Metrics.Enabled {
		if config.Metrics.Port <= 0 || config.Metrics.Port > 65535 {
			errors = append(errors, "metrics.port must be between 1 and 65535")
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("configuration validation errors:\n- %s", strings.Join(errors, "\n- "))
	}

	return nil
}

// GetConfig returns the current configuration
func (cm *ConfigManager) GetConfig() *AppConfig {
	return cm.config
}

// SaveConfig saves the current configuration to the config file
func (cm *ConfigManager) SaveConfig(ctx context.Context) error {
	if cm.configPath == "" {
		return fmt.Errorf("no config path specified")
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(cm.configPath), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal configuration to JSON
	data, err := json.MarshalIndent(cm.config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}

	// Write to file
	if err := os.WriteFile(cm.configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	cm.logger.Info("configuration saved", "path", cm.configPath)
	return nil
}

// RegisterWatcher registers a component to be notified of configuration changes
func (cm *ConfigManager) RegisterWatcher(watcher ConfigWatcher) {
	cm.watchers = append(cm.watchers, watcher)
}

// NotifyWatchers notifies all registered watchers of configuration changes
func (cm *ConfigManager) NotifyWatchers(ctx context.Context) error {
	for _, watcher := range cm.watchers {
		if err := watcher.OnConfigUpdate(ctx, cm.config); err != nil {
			cm.logger.Error("watcher failed to handle config update", "error", err)
			return fmt.Errorf("config update notification failed: %w", err)
		}
	}
	return nil
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *AppConfig {
	return &AppConfig{
		AppName: "ohlcv-collector",
		Version: "1.0.0",
		Storage: StorageConfig{
			Type:         "duckdb",
			DatabaseURL:  "./data/ohlcv.db",
			BatchSize:    1000,
			MaxConns:     1,
			IdleTimeout:  "30m",
			QueryTimeout: "30s",
		},
		Exchange: ExchangeConfig{
			Type:      "coinbase",
			Sandbox:   false,
			RateLimit: 10,
			Timeout:   "30s",
			RetryPolicy: RetryPolicyConfig{
				MaxAttempts:     3,
				InitialDelay:    "1s",
				MaxDelay:        "30s",
				BackoffStrategy: "exponential",
				RetryableErrors: []string{"timeout", "rate_limit", "server_error"},
				Jitter:          true,
			},
			Exchanges: make(map[string]interface{}),
		},
		Collector: CollectorConfig{
			WorkerCount:     4,
			BatchSize:       1000,
			ChannelSize:     10000,
			MaxMemoryMB:     512,
			GracefulTimeout: "30s",
			RetryAttempts:   3,
		},
		Scheduler: SchedulerConfig{
			Enabled:             false,
			Interval:            "1h",
			MaxConcurrentJobs:   2,
			JobTimeout:          "10m",
			EnableHourAlignment: true,
			TimezoneLocation:    "UTC",
			DefaultPairs:        []string{"BTC-USD", "ETH-USD"},
			DefaultIntervals:    []string{"1h", "1d"},
		},
		Validator: ValidatorConfig{
			Enabled:                 true,
			PriceSpikeThreshold:     5.0,  // 500% price change threshold
			VolumeSurgeThreshold:    10.0, // 10x volume surge threshold
			MinConfidenceScore:      0.7,  // 70% minimum confidence
			MaxLookbackPeriods:      24,   // 24 periods for context
			EnabledChecks:           []string{"price_spike", "volume_surge", "ohlc_logic", "sequence_gap"},
			SeverityActions:         map[string]string{"critical": "block", "error": "log", "warning": "log", "info": "log"},
			BatchSize:               100,
			ProcessingTimeout:       "5s",
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "json",
			Output:     "stdout",
			FilePath:   "",
			MaxSize:    100, // 100MB
			MaxBackups: 5,
			MaxAge:     30, // 30 days
			Compress:   true,
			ContextFields: map[string]string{
				"service": "ohlcv-collector",
				"version": "1.0.0",
			},
		},
		Metrics: MetricsConfig{
			Enabled:         true,
			Port:            9090,
			Path:            "/metrics",
			UpdateInterval:  "30s",
			HistoryDuration: "24h",
			EnabledMetrics:  []string{"collection_rate", "storage_operations", "validation_results", "error_counts"},
		},
		ErrorHandling: ErrorHandlingConfig{
			GlobalRetryPolicy: RetryPolicyConfig{
				MaxAttempts:     3,
				InitialDelay:    "1s",
				MaxDelay:        "60s",
				BackoffStrategy: "exponential",
				RetryableErrors: []string{"timeout", "connection_error", "temporary_failure"},
				Jitter:          true,
			},
			ComponentPolicies:    make(map[string]RetryPolicyConfig),
			FallbackBehavior:     "log_and_continue",
			EnableCircuitBreaker: true,
			CircuitBreakerConfig: CircuitBreakerConfig{
				FailureThreshold: 5,
				RecoveryTimeout:  "30s",
				HalfOpenRequests: 3,
			},
		},
	}
}

// GetStorageConfig returns storage-specific configuration
func (c *AppConfig) GetStorageConfig() StorageConfig {
	return c.Storage
}

// GetExchangeConfig returns exchange-specific configuration
func (c *AppConfig) GetExchangeConfig() ExchangeConfig {
	return c.Exchange
}

// GetCollectorConfig returns collector-specific configuration
func (c *AppConfig) GetCollectorConfig() CollectorConfig {
	return c.Collector
}

// GetSchedulerConfig returns scheduler-specific configuration
func (c *AppConfig) GetSchedulerConfig() SchedulerConfig {
	return c.Scheduler
}

// GetValidatorConfig returns validator-specific configuration
func (c *AppConfig) GetValidatorConfig() ValidatorConfig {
	return c.Validator
}

// GetLoggingConfig returns logging-specific configuration
func (c *AppConfig) GetLoggingConfig() LoggingConfig {
	return c.Logging
}

// GetMetricsConfig returns metrics-specific configuration
func (c *AppConfig) GetMetricsConfig() MetricsConfig {
	return c.Metrics
}

// GetErrorHandlingConfig returns error handling configuration
func (c *AppConfig) GetErrorHandlingConfig() ErrorHandlingConfig {
	return c.ErrorHandling
}

// String returns a string representation of the configuration (excluding sensitive data)
func (c *AppConfig) String() string {
	// Create a copy without sensitive data
	sanitized := *c
	sanitized.Exchange.APIKey = "[REDACTED]"
	sanitized.Exchange.APISecret = "[REDACTED]"
	
	data, _ := json.MarshalIndent(&sanitized, "", "  ")
	return string(data)
}