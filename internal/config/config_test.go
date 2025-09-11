package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"log/slog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	
	assert.Equal(t, "ohlcv-collector", config.AppName)
	assert.Equal(t, "1.0.0", config.Version)
	assert.Equal(t, "duckdb", config.Storage.Type)
	assert.Equal(t, "./data/ohlcv.db", config.Storage.DatabaseURL)
	assert.Equal(t, 1000, config.Storage.BatchSize)
	assert.Equal(t, "coinbase", config.Exchange.Type)
	assert.Equal(t, 10, config.Exchange.RateLimit)
	assert.Equal(t, 4, config.Collector.WorkerCount)
	assert.Equal(t, "info", config.Logging.Level)
	assert.True(t, config.Metrics.Enabled)
	assert.True(t, config.ErrorHandling.EnableCircuitBreaker)
}

func TestConfigValidation(t *testing.T) {
	logger := slog.Default()
	cm := NewConfigManager("", logger)
	
	t.Run("valid config passes validation", func(t *testing.T) {
		config := DefaultConfig()
		err := cm.validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("missing storage type fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Storage.Type = ""
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage.type is required")
	})

	t.Run("invalid batch size fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Storage.BatchSize = 0
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage.batch_size must be greater than 0")
	})

	t.Run("missing exchange type fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Exchange.Type = ""
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exchange.type is required")
	})

	t.Run("invalid rate limit fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Exchange.RateLimit = 0
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exchange.rate_limit must be greater than 0")
	})

	t.Run("invalid worker count fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Collector.WorkerCount = 0
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "collector.worker_count must be greater than 0")
	})

	t.Run("scheduler validation when enabled", func(t *testing.T) {
		config := DefaultConfig()
		config.Scheduler.Enabled = true
		config.Scheduler.Interval = ""
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scheduler.interval is required")
		
		config.Scheduler.Interval = "invalid-duration"
		err = cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scheduler.interval is not a valid duration")
		
		config.Scheduler.Interval = "1h"
		config.Scheduler.MaxConcurrentJobs = 0
		err = cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scheduler.max_concurrent_jobs must be greater than 0")
	})

	t.Run("invalid log level fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Logging.Level = "invalid"
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "logging.level must be one of")
	})

	t.Run("invalid log format fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Logging.Format = "invalid"
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "logging.format must be one of")
	})

	t.Run("invalid metrics port fails", func(t *testing.T) {
		config := DefaultConfig()
		config.Metrics.Enabled = true
		config.Metrics.Port = 0
		err := cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "metrics.port must be between 1 and 65535")
		
		config.Metrics.Port = 70000
		err = cm.validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "metrics.port must be between 1 and 65535")
	})
}

func TestLoadConfigFromFile(t *testing.T) {
	// Create temporary directory for test files
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.json")

	testConfig := &AppConfig{
		AppName: "test-app",
		Version: "2.0.0",
		Storage: StorageConfig{
			Type:        "memory",
			DatabaseURL: ":memory:",
			BatchSize:   500,
		},
		Exchange: ExchangeConfig{
			Type:      "mock",
			RateLimit: 20,
		},
		Collector: CollectorConfig{
			WorkerCount: 8,
			BatchSize:   500,
		},
		Logging: LoggingConfig{
			Level:  "debug",
			Format: "text",
		},
	}

	// Write test config to file
	configData, err := json.MarshalIndent(testConfig, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, configData, 0644))

	logger := slog.Default()
	cm := NewConfigManager(configPath, logger)

	t.Run("loads config from file", func(t *testing.T) {
		ctx := context.Background()
		loadedConfig, err := cm.LoadConfig(ctx)
		require.NoError(t, err)
		
		assert.Equal(t, "test-app", loadedConfig.AppName)
		assert.Equal(t, "2.0.0", loadedConfig.Version)
		assert.Equal(t, "memory", loadedConfig.Storage.Type)
		assert.Equal(t, ":memory:", loadedConfig.Storage.DatabaseURL)
		assert.Equal(t, 500, loadedConfig.Storage.BatchSize)
		assert.Equal(t, "mock", loadedConfig.Exchange.Type)
		assert.Equal(t, 20, loadedConfig.Exchange.RateLimit)
		assert.Equal(t, 8, loadedConfig.Collector.WorkerCount)
		assert.Equal(t, "debug", loadedConfig.Logging.Level)
		assert.Equal(t, "text", loadedConfig.Logging.Format)
	})

	t.Run("handles invalid json file", func(t *testing.T) {
		invalidPath := filepath.Join(tempDir, "invalid.json")
		require.NoError(t, os.WriteFile(invalidPath, []byte("invalid json"), 0644))
		
		cm := NewConfigManager(invalidPath, logger)
		ctx := context.Background()
		_, err := cm.LoadConfig(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse config file")
	})

	t.Run("handles non-existent file gracefully", func(t *testing.T) {
		nonExistentPath := filepath.Join(tempDir, "does_not_exist.json")
		cm := NewConfigManager(nonExistentPath, logger)
		
		ctx := context.Background()
		config, err := cm.LoadConfig(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		// Should load default config when file doesn't exist
		assert.Equal(t, "ohlcv-collector", config.AppName)
	})
}

func TestLoadConfigFromEnvironment(t *testing.T) {
	logger := slog.Default()
	cm := NewConfigManager("", logger)

	// Set environment variables
	envVars := map[string]string{
		"APP_NAME":              "env-test-app",
		"VERSION":               "3.0.0",
		"STORAGE_TYPE":          "postgresql",
		"DATABASE_URL":          "postgres://localhost/test",
		"BATCH_SIZE":            "2000",
		"EXCHANGE_TYPE":         "binance",
		"API_KEY":               "test-key",
		"API_SECRET":            "test-secret",
		"RATE_LIMIT":            "50",
		"WORKER_COUNT":          "10",
		"RETRY_ATTEMPTS":        "5",
		"SCHEDULER_ENABLED":     "true",
		"SCHEDULER_INTERVAL":    "30m",
		"MAX_CONCURRENT_JOBS":   "5",
		"DEFAULT_PAIRS":         "BTC-USD,ETH-USD,LTC-USD",
		"DEFAULT_INTERVALS":     "1m,5m,1h",
		"VALIDATOR_ENABLED":     "false",
		"PRICE_SPIKE_THRESHOLD": "10.0",
		"LOG_LEVEL":             "error",
		"LOG_FORMAT":            "json",
		"METRICS_ENABLED":       "false",
		"METRICS_PORT":          "8080",
	}

	// Set environment variables
	for key, value := range envVars {
		t.Setenv(key, value)
	}

	t.Run("loads config from environment", func(t *testing.T) {
		config := DefaultConfig()
		err := cm.loadFromEnv(config)
		require.NoError(t, err)
		
		assert.Equal(t, "env-test-app", config.AppName)
		assert.Equal(t, "3.0.0", config.Version)
		assert.Equal(t, "postgresql", config.Storage.Type)
		assert.Equal(t, "postgres://localhost/test", config.Storage.DatabaseURL)
		assert.Equal(t, 2000, config.Storage.BatchSize)
		assert.Equal(t, "binance", config.Exchange.Type)
		assert.Equal(t, "test-key", config.Exchange.APIKey)
		assert.Equal(t, "test-secret", config.Exchange.APISecret)
		assert.Equal(t, 50, config.Exchange.RateLimit)
		assert.Equal(t, 10, config.Collector.WorkerCount)
		assert.Equal(t, 5, config.Collector.RetryAttempts)
		assert.True(t, config.Scheduler.Enabled)
		assert.Equal(t, "30m", config.Scheduler.Interval)
		assert.Equal(t, 5, config.Scheduler.MaxConcurrentJobs)
		assert.Equal(t, []string{"BTC-USD", "ETH-USD", "LTC-USD"}, config.Scheduler.DefaultPairs)
		assert.Equal(t, []string{"1m", "5m", "1h"}, config.Scheduler.DefaultIntervals)
		assert.False(t, config.Validator.Enabled)
		assert.Equal(t, 10.0, config.Validator.PriceSpikeThreshold)
		assert.Equal(t, "error", config.Logging.Level)
		assert.Equal(t, "json", config.Logging.Format)
		assert.False(t, config.Metrics.Enabled)
		assert.Equal(t, 8080, config.Metrics.Port)
	})

	t.Run("handles invalid numeric values", func(t *testing.T) {
		t.Setenv("BATCH_SIZE", "not-a-number")
		
		config := DefaultConfig()
		originalBatchSize := config.Storage.BatchSize
		
		err := cm.loadFromEnv(config)
		assert.NoError(t, err) // Should not error, but should keep original value
		assert.Equal(t, originalBatchSize, config.Storage.BatchSize)
	})
}

func TestSaveConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "save_test.json")

	logger := slog.Default()
	cm := NewConfigManager(configPath, logger)
	cm.config = DefaultConfig()
	cm.config.AppName = "saved-config-test"
	cm.config.Version = "4.0.0"

	t.Run("saves config to file", func(t *testing.T) {
		ctx := context.Background()
		err := cm.SaveConfig(ctx)
		require.NoError(t, err)

		// Verify file was created and contains expected data
		data, err := os.ReadFile(configPath)
		require.NoError(t, err)
		
		var savedConfig AppConfig
		err = json.Unmarshal(data, &savedConfig)
		require.NoError(t, err)
		
		assert.Equal(t, "saved-config-test", savedConfig.AppName)
		assert.Equal(t, "4.0.0", savedConfig.Version)
	})

	t.Run("creates directory if not exists", func(t *testing.T) {
		nestedPath := filepath.Join(tempDir, "nested", "dir", "config.json")
		cm := NewConfigManager(nestedPath, logger)
		cm.config = DefaultConfig()
		
		ctx := context.Background()
		err := cm.SaveConfig(ctx)
		assert.NoError(t, err)
		
		// Verify directory was created
		assert.FileExists(t, nestedPath)
	})

	t.Run("fails when no config path specified", func(t *testing.T) {
		cm := NewConfigManager("", logger)
		cm.config = DefaultConfig()
		
		ctx := context.Background()
		err := cm.SaveConfig(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no config path specified")
	})
}

func TestConfigAccessors(t *testing.T) {
	config := DefaultConfig()

	t.Run("storage config accessor", func(t *testing.T) {
		storageConfig := config.GetStorageConfig()
		assert.Equal(t, config.Storage, storageConfig)
	})

	t.Run("exchange config accessor", func(t *testing.T) {
		exchangeConfig := config.GetExchangeConfig()
		assert.Equal(t, config.Exchange, exchangeConfig)
	})

	t.Run("collector config accessor", func(t *testing.T) {
		collectorConfig := config.GetCollectorConfig()
		assert.Equal(t, config.Collector, collectorConfig)
	})

	t.Run("scheduler config accessor", func(t *testing.T) {
		schedulerConfig := config.GetSchedulerConfig()
		assert.Equal(t, config.Scheduler, schedulerConfig)
	})

	t.Run("validator config accessor", func(t *testing.T) {
		validatorConfig := config.GetValidatorConfig()
		assert.Equal(t, config.Validator, validatorConfig)
	})

	t.Run("logging config accessor", func(t *testing.T) {
		loggingConfig := config.GetLoggingConfig()
		assert.Equal(t, config.Logging, loggingConfig)
	})

	t.Run("metrics config accessor", func(t *testing.T) {
		metricsConfig := config.GetMetricsConfig()
		assert.Equal(t, config.Metrics, metricsConfig)
	})

	t.Run("error handling config accessor", func(t *testing.T) {
		errorConfig := config.GetErrorHandlingConfig()
		assert.Equal(t, config.ErrorHandling, errorConfig)
	})
}

func TestConfigString(t *testing.T) {
	config := DefaultConfig()
	config.Exchange.APIKey = "secret-key"
	config.Exchange.APISecret = "secret-value"

	configStr := config.String()
	
	// Should contain most config values
	assert.Contains(t, configStr, "ohlcv-collector")
	assert.Contains(t, configStr, "duckdb")
	assert.Contains(t, configStr, "coinbase")
	
	// Should redact sensitive information
	assert.Contains(t, configStr, "[REDACTED]")
	assert.NotContains(t, configStr, "secret-key")
	assert.NotContains(t, configStr, "secret-value")
}

// Mock config watcher for testing
type mockConfigWatcher struct {
	updateCount int
	lastConfig  *AppConfig
	shouldError bool
}

func (m *mockConfigWatcher) OnConfigUpdate(ctx context.Context, config *AppConfig) error {
	m.updateCount++
	m.lastConfig = config
	if m.shouldError {
		return assert.AnError
	}
	return nil
}

func TestConfigWatchers(t *testing.T) {
	logger := slog.Default()
	cm := NewConfigManager("", logger)
	cm.config = DefaultConfig()

	t.Run("register and notify watchers", func(t *testing.T) {
		watcher1 := &mockConfigWatcher{}
		watcher2 := &mockConfigWatcher{}

		cm.RegisterWatcher(watcher1)
		cm.RegisterWatcher(watcher2)

		ctx := context.Background()
		err := cm.NotifyWatchers(ctx)
		assert.NoError(t, err)

		assert.Equal(t, 1, watcher1.updateCount)
		assert.Equal(t, 1, watcher2.updateCount)
		assert.Equal(t, cm.config, watcher1.lastConfig)
		assert.Equal(t, cm.config, watcher2.lastConfig)
	})

	t.Run("handles watcher errors", func(t *testing.T) {
		cm := NewConfigManager("", logger)
		cm.config = DefaultConfig()
		
		errorWatcher := &mockConfigWatcher{shouldError: true}
		cm.RegisterWatcher(errorWatcher)

		ctx := context.Background()
		err := cm.NotifyWatchers(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "config update notification failed")
	})
}

func TestCompleteConfigFlow(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "complete_test.json")

	// Create initial config file
	initialConfig := &AppConfig{
		AppName: "flow-test",
		Storage: StorageConfig{
			Type:      "memory",
			BatchSize: 100,
		},
		Exchange: ExchangeConfig{
			Type:      "mock",
			RateLimit: 5,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
		},
	}

	configData, err := json.MarshalIndent(initialConfig, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, configData, 0644))

	// Set some environment variables that should override file values
	t.Setenv("STORAGE_TYPE", "duckdb")
	t.Setenv("DATABASE_URL", "./test.db")
	t.Setenv("BATCH_SIZE", "2000")
	t.Setenv("WORKER_COUNT", "8")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "json")

	logger := slog.Default()
	cm := NewConfigManager(configPath, logger)

	t.Run("complete load flow with precedence", func(t *testing.T) {
		ctx := context.Background()
		config, err := cm.LoadConfig(ctx)
		require.NoError(t, err)

		// Values from file
		assert.Equal(t, "flow-test", config.AppName)
		assert.Equal(t, "mock", config.Exchange.Type)
		assert.Equal(t, 5, config.Exchange.RateLimit)

		// Values overridden by environment
		assert.Equal(t, "duckdb", config.Storage.Type) // overridden from "memory"
		assert.Equal(t, "./test.db", config.Storage.DatabaseURL) // overridden
		assert.Equal(t, 2000, config.Storage.BatchSize) // overridden from 100
		assert.Equal(t, 8, config.Collector.WorkerCount) // overridden
		assert.Equal(t, "debug", config.Logging.Level) // overridden from default "info"
		assert.Equal(t, "json", config.Logging.Format) // overridden

		// Default values for unspecified fields
		assert.True(t, config.Metrics.Enabled) // default value
	})
}

func TestConfigManagerState(t *testing.T) {
	logger := slog.Default()
	cm := NewConfigManager("test.json", logger)

	t.Run("initially no config", func(t *testing.T) {
		assert.Nil(t, cm.GetConfig())
	})

	t.Run("returns config after load", func(t *testing.T) {
		ctx := context.Background()
		loadedConfig, err := cm.LoadConfig(ctx)
		require.NoError(t, err)

		retrievedConfig := cm.GetConfig()
		assert.Equal(t, loadedConfig, retrievedConfig)
		assert.NotNil(t, retrievedConfig)
	})
}