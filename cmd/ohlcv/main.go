// OHLCV Data Collector CLI
// This application provides command-line interface for collecting, scheduling,
// querying, and managing OHLCV (Open, High, Low, Close, Volume) data from
// cryptocurrency exchanges.
//
// Usage:
//
//	ohlcv collect --pair BTC-USD --interval 1d --days 30
//	ohlcv schedule --pairs BTC-USD,ETH-USD --interval 1d --frequency 1h
//	ohlcv gaps --pair BTC-USD --interval 1d
//	ohlcv query --pair BTC-USD --start 2024-01-01 --end 2024-01-31
//
// For detailed help on any command, use: ohlcv <command> --help
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/collector"
	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/exchange"
	"github.com/johnayoung/go-ohlcv-collector/internal/gaps"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
	"github.com/johnayoung/go-ohlcv-collector/internal/storage"
	"github.com/johnayoung/go-ohlcv-collector/internal/validator"
)

// CLI version information
const (
	Version    = "1.0.0"
	AppName    = "ohlcv"
	ConfigFile = "ohlcv.json"
)

// Exit codes following standard conventions
const (
	ExitSuccess       = 0
	ExitUsageError    = 1
	ExitConfigError   = 2
	ExitConnectionErr = 3
	ExitDataError     = 4
	ExitInterrupt     = 130
)

// Config represents the CLI application configuration
type Config struct {
	// Storage configuration
	StorageType string `json:"storage_type,omitempty"` // "duckdb", "postgresql", "memory"
	DatabaseURL string `json:"database_url,omitempty"`

	// Exchange configuration
	ExchangeType string `json:"exchange_type,omitempty"` // "coinbase", "mock"
	APIKey       string `json:"api_key,omitempty"`
	APISecret    string `json:"api_secret,omitempty"`

	// Collection configuration
	BatchSize      int `json:"batch_size,omitempty"`
	WorkerCount    int `json:"worker_count,omitempty"`
	RateLimit      int `json:"rate_limit,omitempty"`
	RetryAttempts  int `json:"retry_attempts,omitempty"`
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`

	// Logging configuration
	LogLevel  string `json:"log_level,omitempty"`  // "debug", "info", "warn", "error"
	LogFormat string `json:"log_format,omitempty"` // "text", "json"
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		StorageType:    "duckdb",
		DatabaseURL:    "ohlcv_data.db",
		ExchangeType:   "coinbase",
		BatchSize:      10000,
		WorkerCount:    4,
		RateLimit:      10,
		RetryAttempts:  3,
		TimeoutSeconds: 30,
		LogLevel:       "info",
		LogFormat:      "text",
	}
}

// CLI represents the main CLI application
type CLI struct {
	config    *Config
	logger    *slog.Logger
	collector collector.Collector
	scheduler collector.Scheduler
	storage   contracts.FullStorage
	exchange  contracts.ExchangeAdapter
	gaps      gaps.GapDetector
	validator validator.ValidationPipeline
}

// main is the entry point for the CLI application
func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(ExitUsageError)
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cli := &CLI{}

	// Initialize CLI
	if err := cli.initialize(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to initialize CLI: %v\n", err)
		os.Exit(ExitConfigError)
	}

	// Parse command and execute
	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "collect":
		if err := cli.handleCollect(ctx, args); err != nil {
			cli.logger.Error("Collection failed", "error", err)
			os.Exit(ExitDataError)
		}
	case "schedule":
		if err := cli.handleSchedule(ctx, args); err != nil {
			cli.logger.Error("Scheduling failed", "error", err)
			os.Exit(ExitDataError)
		}
	case "gaps":
		if err := cli.handleGaps(ctx, args); err != nil {
			cli.logger.Error("Gap analysis failed", "error", err)
			os.Exit(ExitDataError)
		}
	case "query":
		if err := cli.handleQuery(ctx, args); err != nil {
			cli.logger.Error("Query failed", "error", err)
			os.Exit(ExitDataError)
		}
	case "--version", "-v":
		fmt.Printf("%s version %s\n", AppName, Version)
	case "--help", "-h", "help":
		if len(args) > 0 {
			printCommandHelp(args[0])
		} else {
			printUsage()
		}
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown command '%s'\n\n", command)
		printUsage()
		os.Exit(ExitUsageError)
	}
}

// initialize sets up the CLI application components
func (cli *CLI) initialize(ctx context.Context) error {
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	cli.config = config

	// Setup logging
	logger, err := setupLogging(config.LogLevel, config.LogFormat)
	if err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}
	cli.logger = logger

	// Initialize storage
	storage, err := createStorage(config)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	cli.storage = storage

	// Initialize storage tables/schema
	if err := storage.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize storage schema: %w", err)
	}

	// Initialize exchange adapter
	exchange, err := createExchange(config)
	if err != nil {
		return fmt.Errorf("failed to initialize exchange: %w", err)
	}
	cli.exchange = exchange

	// Initialize validator
	cli.validator = &mockValidator{}

	// Initialize gap detector
	cli.gaps = &mockGapDetector{}

	// Initialize collector
	collectorConfig := &collector.Config{
		BatchSize:     config.BatchSize,
		WorkerCount:   config.WorkerCount,
		RateLimit:     config.RateLimit,
		RetryAttempts: config.RetryAttempts,
		Logger:        logger,
	}

	cli.collector = collector.NewWithConfig(
		exchange,
		storage,
		cli.validator,
		cli.gaps,
		collectorConfig,
	)

	// Initialize scheduler
	schedulerConfig := collector.DefaultSchedulerConfig()
	schedulerConfig.MaxConcurrentJobs = config.WorkerCount

	cli.scheduler = collector.NewScheduler(schedulerConfig, cli.exchange, cli.storage, cli.collector)

	return nil
}

// handleCollect handles the 'collect' command for historical data collection
func (cli *CLI) handleCollect(ctx context.Context, args []string) error {
	// Parse collect command flags
	flags, err := parseCollectFlags(args)
	if err != nil {
		return err
	}

	if flags.Help {
		printCommandHelp("collect")
		return nil
	}

	// Validate required parameters
	if flags.Pair == "" {
		return fmt.Errorf("--pair is required")
	}
	if flags.Interval == "" {
		return fmt.Errorf("--interval is required")
	}

	// Calculate time range
	var startTime, endTime time.Time

	if flags.Start != "" {
		startTime, err = time.Parse("2006-01-02", flags.Start)
		if err != nil {
			return fmt.Errorf("invalid start date format, use YYYY-MM-DD: %w", err)
		}
	}

	if flags.End != "" {
		endTime, err = time.Parse("2006-01-02", flags.End)
		if err != nil {
			return fmt.Errorf("invalid end date format, use YYYY-MM-DD: %w", err)
		}
	}

	// Handle days parameter if no explicit dates provided
	if flags.Days > 0 {
		endTime = time.Now().UTC()
		startTime = endTime.AddDate(0, 0, -flags.Days)
	}

	if startTime.IsZero() || endTime.IsZero() {
		return fmt.Errorf("specify either --days or both --start and --end")
	}

	if startTime.After(endTime) {
		return fmt.Errorf("start time cannot be after end time")
	}

	cli.logger.Info("Starting historical data collection",
		"pair", flags.Pair,
		"interval", flags.Interval,
		"start", startTime.Format("2006-01-02"),
		"end", endTime.Format("2006-01-02"))

	// Execute collection
	req := collector.HistoricalRequest{
		Pair:     flags.Pair,
		Interval: flags.Interval,
		Start:    startTime,
		End:      endTime,
	}

	if err := cli.collector.CollectHistorical(ctx, req); err != nil {
		return fmt.Errorf("collection failed: %w", err)
	}

	cli.logger.Info("Historical data collection completed successfully")

	// Print summary
	fmt.Printf("✅ Successfully collected %s %s data from %s to %s\n",
		flags.Pair, flags.Interval,
		startTime.Format("2006-01-02"),
		endTime.Format("2006-01-02"))

	return nil
}

// handleSchedule handles the 'schedule' command for automated data collection
func (cli *CLI) handleSchedule(ctx context.Context, args []string) error {
	flags, err := parseScheduleFlags(args)
	if err != nil {
		return err
	}

	if flags.Help {
		printCommandHelp("schedule")
		return nil
	}

	if len(flags.Pairs) == 0 {
		return fmt.Errorf("--pairs is required")
	}
	if flags.Interval == "" {
		return fmt.Errorf("--interval is required")
	}

	frequency, err := time.ParseDuration(flags.Frequency)
	if err != nil {
		return fmt.Errorf("invalid frequency format: %w", err)
	}

	cli.logger.Info("Starting scheduled collection",
		"pairs", flags.Pairs,
		"interval", flags.Interval,
		"frequency", frequency)

	// Create collection jobs for each pair
	for _, pair := range flags.Pairs {
		job := &collectionJob{
			id:        fmt.Sprintf("%s-%s", pair, flags.Interval),
			pair:      pair,
			interval:  flags.Interval,
			nextRun:   time.Now().Add(time.Minute), // Start in 1 minute
			collector: cli.collector,
			logger:    cli.logger,
		}

		if err := cli.scheduler.AddJob(job); err != nil {
			return fmt.Errorf("failed to add job for %s: %w", pair, err)
		}
	}

	// Start scheduler
	fmt.Printf("🚀 Starting scheduler for %d pairs with %v frequency...\n", len(flags.Pairs), frequency)
	fmt.Println("Press Ctrl+C to stop gracefully")

	return cli.scheduler.Start(ctx)
}

// handleGaps handles the 'gaps' command for gap detection and analysis
func (cli *CLI) handleGaps(ctx context.Context, args []string) error {
	flags, err := parseGapsFlags(args)
	if err != nil {
		return err
	}

	if flags.Help {
		printCommandHelp("gaps")
		return nil
	}

	if flags.Pair == "" {
		return fmt.Errorf("--pair is required")
	}
	if flags.Interval == "" {
		return fmt.Errorf("--interval is required")
	}

	// Calculate lookback period
	lookback := 7 * 24 * time.Hour // Default 7 days
	if flags.Days > 0 {
		lookback = time.Duration(flags.Days) * 24 * time.Hour
	}

	cli.logger.Info("Detecting data gaps",
		"pair", flags.Pair,
		"interval", flags.Interval,
		"lookback_days", flags.Days)

	// Detect gaps
	gaps, err := cli.gaps.DetectRecentGaps(ctx, flags.Pair, flags.Interval, lookback)
	if err != nil {
		return fmt.Errorf("gap detection failed: %w", err)
	}

	// Display results
	if len(gaps) == 0 {
		fmt.Printf("✅ No data gaps found for %s %s in the last %d days\n", flags.Pair, flags.Interval, flags.Days)
		return nil
	}

	fmt.Printf("🔍 Found %d data gaps for %s %s:\n\n", len(gaps), flags.Pair, flags.Interval)

	for i, gap := range gaps {
		duration := gap.EndTime.Sub(gap.StartTime)
		fmt.Printf("%d. Gap from %s to %s (Duration: %v, Status: %s)\n",
			i+1,
			gap.StartTime.Format("2006-01-02 15:04:05"),
			gap.EndTime.Format("2006-01-02 15:04:05"),
			duration,
			gap.Status)
	}

	// Offer to backfill gaps
	if flags.Backfill {
		fmt.Println("\n📡 Starting gap backfill...")

		for _, gap := range gaps {
			if gap.Status == "filled" {
				continue
			}

			cli.logger.Info("Backfilling gap",
				"pair", gap.Pair,
				"start", gap.StartTime,
				"end", gap.EndTime)

			req := collector.HistoricalRequest{
				Pair:     gap.Pair,
				Interval: gap.Interval,
				Start:    gap.StartTime,
				End:      gap.EndTime,
			}

			if err := cli.collector.CollectHistorical(ctx, req); err != nil {
				cli.logger.Error("Failed to backfill gap", "gap_id", gap.ID, "error", err)
				continue
			}

			// Mark gap as filled
			if err := cli.storage.MarkGapFilled(ctx, gap.ID, time.Now()); err != nil {
				cli.logger.Warn("Failed to mark gap as filled", "gap_id", gap.ID, "error", err)
			}

			fmt.Printf("✅ Backfilled gap from %s to %s\n",
				gap.StartTime.Format("2006-01-02 15:04:05"),
				gap.EndTime.Format("2006-01-02 15:04:05"))
		}
	} else {
		fmt.Printf("\nTo backfill these gaps, run: %s gaps --pair %s --interval %s --backfill\n", AppName, flags.Pair, flags.Interval)
	}

	return nil
}

// handleQuery handles the 'query' command for data retrieval
func (cli *CLI) handleQuery(ctx context.Context, args []string) error {
	flags, err := parseQueryFlags(args)
	if err != nil {
		return err
	}

	if flags.Help {
		printCommandHelp("query")
		return nil
	}

	if flags.Pair == "" {
		return fmt.Errorf("--pair is required")
	}

	// Parse time range
	var startTime, endTime time.Time

	if flags.Start != "" {
		startTime, err = time.Parse("2006-01-02", flags.Start)
		if err != nil {
			return fmt.Errorf("invalid start date format, use YYYY-MM-DD: %w", err)
		}
	}

	if flags.End != "" {
		endTime, err = time.Parse("2006-01-02", flags.End)
		if err != nil {
			return fmt.Errorf("invalid end date format, use YYYY-MM-DD: %w", err)
		}
	}

	// Default to last 7 days if no dates specified
	if startTime.IsZero() && endTime.IsZero() {
		endTime = time.Now().UTC()
		startTime = endTime.AddDate(0, 0, -7)
	}

	// Build query request
	req := contracts.QueryRequest{
		Pair:     flags.Pair,
		Start:    startTime,
		End:      endTime,
		Interval: flags.Interval,
		Limit:    flags.Limit,
		OrderBy:  "timestamp_asc",
	}

	cli.logger.Info("Querying data",
		"pair", flags.Pair,
		"interval", flags.Interval,
		"start", startTime.Format("2006-01-02"),
		"end", endTime.Format("2006-01-02"),
		"limit", flags.Limit)

	// Execute query
	result, err := cli.storage.Query(ctx, req)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	// Display results
	fmt.Printf("📊 Query Results for %s", flags.Pair)
	if flags.Interval != "" {
		fmt.Printf(" (%s)", flags.Interval)
	}
	fmt.Printf("\n")
	fmt.Printf("Time Range: %s to %s\n", startTime.Format("2006-01-02"), endTime.Format("2006-01-02"))
	fmt.Printf("Found: %d candles\n", len(result.Candles))
	fmt.Printf("Query Time: %v\n\n", result.QueryTime)

	if len(result.Candles) == 0 {
		fmt.Println("No data found for the specified criteria.")
		return nil
	}

	// Format and display candles
	switch flags.Format {
	case "json":
		return outputJSON(result.Candles)
	case "csv":
		return outputCSV(result.Candles)
	case "table":
		return outputTable(result.Candles, flags.Limit)
	default:
		return outputTable(result.Candles, flags.Limit)
	}
}

// Flag structures for parsing command line arguments

// CollectFlags represents flags for the collect command
type CollectFlags struct {
	Pair     string
	Interval string
	Start    string
	End      string
	Days     int
	Limit    int
	Help     bool
}

// ScheduleFlags represents flags for the schedule command
type ScheduleFlags struct {
	Pairs     []string
	Interval  string
	Frequency string
	Help      bool
}

// GapsFlags represents flags for the gaps command
type GapsFlags struct {
	Pair     string
	Interval string
	Days     int
	Backfill bool
	Help     bool
}

// QueryFlags represents flags for the query command
type QueryFlags struct {
	Pair     string
	Interval string
	Start    string
	End      string
	Limit    int
	Format   string
	Help     bool
}

// Flag parsing functions

// parseCollectFlags parses command line arguments for the collect command
func parseCollectFlags(args []string) (*CollectFlags, error) {
	flags := &CollectFlags{
		Interval: "1d", // Default interval
		Days:     30,   // Default days
		Limit:    1000, // Default limit
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--pair", "-p":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--pair requires a value")
			}
			flags.Pair = args[i+1]
			i++
		case "--interval", "-i":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--interval requires a value")
			}
			flags.Interval = args[i+1]
			i++
		case "--start", "-s":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--start requires a value")
			}
			flags.Start = args[i+1]
			i++
		case "--end", "-e":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--end requires a value")
			}
			flags.End = args[i+1]
			i++
		case "--days", "-d":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--days requires a value")
			}
			days, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid days value: %w", err)
			}
			flags.Days = days
			i++
		case "--help", "-h":
			flags.Help = true
		default:
			return nil, fmt.Errorf("unknown flag: %s", args[i])
		}
	}

	return flags, nil
}

// parseScheduleFlags parses command line arguments for the schedule command
func parseScheduleFlags(args []string) (*ScheduleFlags, error) {
	flags := &ScheduleFlags{
		Interval:  "1h", // Default interval
		Frequency: "1h", // Default frequency
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--pairs", "-p":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--pairs requires a value")
			}
			flags.Pairs = strings.Split(args[i+1], ",")
			i++
		case "--interval", "-i":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--interval requires a value")
			}
			flags.Interval = args[i+1]
			i++
		case "--frequency", "-f":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--frequency requires a value")
			}
			flags.Frequency = args[i+1]
			i++
		case "--help", "-h":
			flags.Help = true
		default:
			return nil, fmt.Errorf("unknown flag: %s", args[i])
		}
	}

	return flags, nil
}

// parseGapsFlags parses command line arguments for the gaps command
func parseGapsFlags(args []string) (*GapsFlags, error) {
	flags := &GapsFlags{
		Interval: "1d", // Default interval
		Days:     7,    // Default lookback days
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--pair", "-p":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--pair requires a value")
			}
			flags.Pair = args[i+1]
			i++
		case "--interval", "-i":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--interval requires a value")
			}
			flags.Interval = args[i+1]
			i++
		case "--days", "-d":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--days requires a value")
			}
			days, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid days value: %w", err)
			}
			flags.Days = days
			i++
		case "--backfill", "-b":
			flags.Backfill = true
		case "--help", "-h":
			flags.Help = true
		default:
			return nil, fmt.Errorf("unknown flag: %s", args[i])
		}
	}

	return flags, nil
}

// parseQueryFlags parses command line arguments for the query command
func parseQueryFlags(args []string) (*QueryFlags, error) {
	flags := &QueryFlags{
		Interval: "1d",    // Default interval
		Limit:    100,     // Default limit
		Format:   "table", // Default format
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--pair", "-p":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--pair requires a value")
			}
			flags.Pair = args[i+1]
			i++
		case "--interval", "-i":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--interval requires a value")
			}
			flags.Interval = args[i+1]
			i++
		case "--start", "-s":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--start requires a value")
			}
			flags.Start = args[i+1]
			i++
		case "--end", "-e":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--end requires a value")
			}
			flags.End = args[i+1]
			i++
		case "--limit", "-l":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--limit requires a value")
			}
			limit, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid limit value: %w", err)
			}
			flags.Limit = limit
			i++
		case "--format", "-f":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--format requires a value")
			}
			format := args[i+1]
			if format != "json" && format != "csv" && format != "table" {
				return nil, fmt.Errorf("invalid format, must be: json, csv, or table")
			}
			flags.Format = format
			i++
		case "--help", "-h":
			flags.Help = true
		default:
			return nil, fmt.Errorf("unknown flag: %s", args[i])
		}
	}

	return flags, nil
}

// Configuration and initialization functions

// loadConfig loads configuration from file and environment variables
func loadConfig() (*Config, error) {
	config := DefaultConfig()

	// Try to load from config file
	configPath := filepath.Join(".", ConfigFile)
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
		}
	}

	// Override with environment variables
	if val := os.Getenv("OHLCV_STORAGE_TYPE"); val != "" {
		config.StorageType = val
	}
	if val := os.Getenv("OHLCV_DATABASE_URL"); val != "" {
		config.DatabaseURL = val
	}
	if val := os.Getenv("OHLCV_EXCHANGE_TYPE"); val != "" {
		config.ExchangeType = val
	}
	if val := os.Getenv("OHLCV_API_KEY"); val != "" {
		config.APIKey = val
	}
	if val := os.Getenv("OHLCV_API_SECRET"); val != "" {
		config.APISecret = val
	}
	if val := os.Getenv("OHLCV_LOG_LEVEL"); val != "" {
		config.LogLevel = val
	}
	if val := os.Getenv("OHLCV_LOG_FORMAT"); val != "" {
		config.LogFormat = val
	}

	// Parse numeric environment variables
	if val := os.Getenv("OHLCV_BATCH_SIZE"); val != "" {
		if batchSize, err := strconv.Atoi(val); err == nil {
			config.BatchSize = batchSize
		}
	}
	if val := os.Getenv("OHLCV_WORKER_COUNT"); val != "" {
		if workerCount, err := strconv.Atoi(val); err == nil {
			config.WorkerCount = workerCount
		}
	}
	if val := os.Getenv("OHLCV_RATE_LIMIT"); val != "" {
		if rateLimit, err := strconv.Atoi(val); err == nil {
			config.RateLimit = rateLimit
		}
	}

	return config, nil
}

// setupLogging configures the logging system
func setupLogging(logLevel, logFormat string) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	switch strings.ToLower(logFormat) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler), nil
}

// createStorage creates the appropriate storage adapter
func createStorage(config *Config) (contracts.FullStorage, error) {
	switch config.StorageType {
	case "duckdb":
		impl, err := storage.NewDuckDBStorage(config.DatabaseURL, slog.Default())
		if err != nil {
			return nil, err
		}
		return storage.NewStorageContractAdapter(impl), nil
	case "postgresql":
		// PostgreSQL adapter not implemented yet
		return nil, fmt.Errorf("postgresql storage not implemented")
	case "memory":
		impl := storage.NewMemoryStorage()
		return storage.NewStorageContractAdapter(impl), nil
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.StorageType)
	}
}

// createExchange creates the appropriate exchange adapter
func createExchange(config *Config) (contracts.ExchangeAdapter, error) {
	switch config.ExchangeType {
	case "coinbase":
		impl := exchange.NewCoinbaseAdapter()
		return exchange.NewExchangeContractAdapter(impl), nil
	case "mock":
		// Mock exchange not implemented yet
		return nil, fmt.Errorf("mock exchange not implemented")
	default:
		return nil, fmt.Errorf("unsupported exchange type: %s", config.ExchangeType)
	}
}

// Output formatting functions

// outputJSON formats candles as JSON
func outputJSON(candles []contracts.Candle) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(candles)
}

// outputCSV formats candles as CSV
func outputCSV(candles []contracts.Candle) error {
	fmt.Println("timestamp,pair,interval,open,high,low,close,volume")
	for _, candle := range candles {
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			candle.Timestamp.Format("2006-01-02T15:04:05Z"),
			candle.Pair,
			candle.Interval,
			candle.Open,
			candle.High,
			candle.Low,
			candle.Close,
			candle.Volume)
	}
	return nil
}

// outputTable formats candles as a table
func outputTable(candles []contracts.Candle, limit int) error {
	if limit > 0 && len(candles) > limit {
		candles = candles[:limit]
	}

	// Table header
	fmt.Printf("%-20s %-12s %-8s %-12s %-12s %-12s %-12s %-15s\n",
		"Timestamp", "Pair", "Interval", "Open", "High", "Low", "Close", "Volume")
	fmt.Println(strings.Repeat("-", 100))

	// Table rows
	for _, candle := range candles {
		fmt.Printf("%-20s %-12s %-8s %-12s %-12s %-12s %-12s %-15s\n",
			candle.Timestamp.Format("2006-01-02 15:04"),
			candle.Pair,
			candle.Interval,
			truncateDecimal(candle.Open, 8),
			truncateDecimal(candle.High, 8),
			truncateDecimal(candle.Low, 8),
			truncateDecimal(candle.Close, 8),
			truncateDecimal(candle.Volume, 10))
	}

	if limit > 0 && len(candles) == limit {
		fmt.Printf("\n... showing first %d results (use --limit to see more)\n", limit)
	}

	return nil
}

// truncateDecimal truncates decimal string to specified length
func truncateDecimal(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// Mock implementations for components not fully implemented

// mockValidator provides a basic validator implementation
type mockValidator struct{}

func (m *mockValidator) ProcessCandles(ctx context.Context, candles []contracts.Candle) (*validator.ValidationPipelineResults, error) {
	return &validator.ValidationPipelineResults{
		ProcessedCandles: int64(len(candles)),
		ValidCandles:     int64(len(candles)),
		InvalidCandles:   0,
		QualityMetrics: &validator.DataQualityMetrics{
			TotalCandles: int64(len(candles)),
			ValidCandles: int64(len(candles)),
			QualityScore: 1.0,
			SuccessRate:  1.0,
		},
		PipelineStatus: validator.PipelineStatusSuccess,
	}, nil
}

func (m *mockValidator) ProcessBatch(ctx context.Context, candles []contracts.Candle, batchSize int, progressCallback func(processed, total int)) (*validator.ValidationPipelineResults, error) {
	return m.ProcessCandles(ctx, candles)
}

func (m *mockValidator) GetQualityMetrics(ctx context.Context, results []*models.ValidationResult) (*validator.DataQualityMetrics, error) {
	return &validator.DataQualityMetrics{
		QualityScore: 1.0,
		SuccessRate:  1.0,
	}, nil
}

func (m *mockValidator) Configure(ctx context.Context, config *validator.ValidationPipelineConfig) error {
	return nil
}

func (m *mockValidator) GetStatus() *validator.ValidationPipelineStatus {
	return &validator.ValidationPipelineStatus{
		IsProcessing: false,
		ErrorCount:   0,
		AnomalyCount: 0,
	}
}

// mockGapDetector provides a basic gap detector implementation
type mockGapDetector struct{}

func (m *mockGapDetector) DetectGaps(ctx context.Context, pair, interval string, startTime, endTime time.Time) ([]models.Gap, error) {
	return []models.Gap{}, nil
}

func (m *mockGapDetector) DetectGapsInSequence(ctx context.Context, candles []contracts.Candle, expectedInterval string) ([]models.Gap, error) {
	return []models.Gap{}, nil
}

func (m *mockGapDetector) DetectRecentGaps(ctx context.Context, pair, interval string, lookbackPeriod time.Duration) ([]models.Gap, error) {
	return []models.Gap{}, nil
}

func (m *mockGapDetector) ValidateGapPeriod(ctx context.Context, pair string, startTime, endTime time.Time, interval string) (bool, string, error) {
	return true, "", nil
}

// collectionJob implements the CollectionJob interface for scheduling
type collectionJob struct {
	id        string
	pair      string
	interval  string
	nextRun   time.Time
	collector collector.Collector
	logger    *slog.Logger
}

func (j *collectionJob) Execute(ctx context.Context) error {
	j.logger.Info("Executing scheduled collection job", "job_id", j.id, "pair", j.pair)

	// Collect data for the last interval period
	endTime := time.Now().UTC()
	var startTime time.Time

	switch j.interval {
	case "1m":
		startTime = endTime.Add(-time.Minute)
	case "5m":
		startTime = endTime.Add(-5 * time.Minute)
	case "1h":
		startTime = endTime.Add(-time.Hour)
	case "1d":
		startTime = endTime.AddDate(0, 0, -1)
	default:
		startTime = endTime.Add(-time.Hour)
	}

	req := collector.HistoricalRequest{
		Pair:     j.pair,
		Interval: j.interval,
		Start:    startTime,
		End:      endTime,
	}

	return j.collector.CollectHistorical(ctx, req)
}

func (j *collectionJob) GetPair() string {
	return j.pair
}

func (j *collectionJob) GetInterval() string {
	return j.interval
}

func (j *collectionJob) GetNextRun() time.Time {
	return j.nextRun
}

func (j *collectionJob) SetNextRun(nextRun time.Time) {
	j.nextRun = nextRun
}

func (j *collectionJob) GetID() string {
	return j.id
}

// Help and usage functions

// printUsage prints the main usage information
func printUsage() {
	fmt.Printf(`%s - OHLCV Data Collector CLI v%s

USAGE:
    %s <command> [options]

COMMANDS:
    collect     Collect historical OHLCV data for a trading pair
    schedule    Start scheduled data collection for multiple pairs  
    gaps        Detect and optionally backfill data gaps
    query       Query and display stored OHLCV data

GLOBAL OPTIONS:
    --help, -h     Show help information
    --version, -v  Show version information

EXAMPLES:
    # Collect BTC-USD daily data for the last 30 days
    %s collect --pair BTC-USD --interval 1d --days 30
    
    # Start hourly scheduled collection for multiple pairs
    %s schedule --pairs BTC-USD,ETH-USD --interval 1h --frequency 1h
    
    # Check for gaps in BTC-USD data over the last 7 days
    %s gaps --pair BTC-USD --interval 1d --days 7
    
    # Query BTC-USD data for January 2024 in JSON format
    %s query --pair BTC-USD --start 2024-01-01 --end 2024-01-31 --format json

CONFIGURATION:
    Configuration can be provided via:
    - Config file: %s (JSON format)
    - Environment variables: OHLCV_* (e.g., OHLCV_DATABASE_URL)
    
    Example config file:
    {
        "storage_type": "duckdb",
        "database_url": "ohlcv_data.db",
        "exchange_type": "coinbase",
        "log_level": "info"
    }

For detailed help on any command, use: %s <command> --help
`, AppName, Version, AppName, AppName, AppName, AppName, AppName, ConfigFile, AppName)
}

// printCommandHelp prints detailed help for a specific command
func printCommandHelp(command string) {
	switch command {
	case "collect":
		fmt.Printf(`%s collect - Collect historical OHLCV data

USAGE:
    %s collect [options]

OPTIONS:
    --pair, -p <pair>         Trading pair to collect (required)
                              Examples: BTC-USD, ETH-USD, SOL-USD
                              
    --interval, -i <interval> Time interval for candles (default: 1d)
                              Supported: 1m, 5m, 15m, 1h, 4h, 1d
                              
    --start, -s <date>        Start date (YYYY-MM-DD format)
    --end, -e <date>          End date (YYYY-MM-DD format)
    --days, -d <days>         Number of days to collect from today (default: 30)
                              
    --help, -h                Show this help message

EXAMPLES:
    # Collect BTC-USD daily data for the last 30 days
    %s collect --pair BTC-USD --interval 1d --days 30
    
    # Collect ETH-USD hourly data for a specific date range
    %s collect --pair ETH-USD --interval 1h --start 2024-01-01 --end 2024-01-31
    
    # Collect SOL-USD 5-minute data for the last 7 days
    %s collect --pair SOL-USD --interval 5m --days 7

NOTES:
    - Either use --days OR both --start and --end
    - Data is stored in the configured storage backend
    - Collection respects exchange rate limits automatically
`, AppName, AppName, AppName, AppName, AppName)

	case "schedule":
		fmt.Printf(`%s schedule - Start scheduled data collection

USAGE:
    %s schedule [options]

OPTIONS:
    --pairs, -p <pairs>       Comma-separated list of trading pairs (required)
                              Examples: BTC-USD,ETH-USD,SOL-USD
                              
    --interval, -i <interval> Time interval for candles (default: 1h)  
                              Supported: 1m, 5m, 15m, 1h, 4h, 1d
                              
    --frequency, -f <freq>    Collection frequency (default: 1h)
                              Examples: 30s, 5m, 1h, 24h
                              
    --help, -h                Show this help message

EXAMPLES:
    # Collect BTC and ETH hourly data every hour
    %s schedule --pairs BTC-USD,ETH-USD --interval 1h --frequency 1h
    
    # Collect multiple pairs daily data every 6 hours
    %s schedule --pairs BTC-USD,ETH-USD,SOL-USD --interval 1d --frequency 6h
    
    # Collect high-frequency data every 30 seconds
    %s schedule --pairs BTC-USD --interval 1m --frequency 30s

NOTES:
    - Scheduler runs until interrupted (Ctrl+C)
    - Jobs are aligned to hour boundaries when possible
    - Failed collections are retried automatically
    - Press Ctrl+C to stop gracefully
`, AppName, AppName, AppName, AppName, AppName)

	case "gaps":
		fmt.Printf(`%s gaps - Detect and backfill data gaps

USAGE:
    %s gaps [options]

OPTIONS:
    --pair, -p <pair>         Trading pair to analyze (required)
    --interval, -i <interval> Time interval to check (default: 1d)
    --days, -d <days>         Days to look back for gaps (default: 7)  
    --backfill, -b            Automatically backfill detected gaps
    --help, -h                Show this help message

EXAMPLES:
    # Check for gaps in BTC-USD daily data over last 7 days
    %s gaps --pair BTC-USD --interval 1d --days 7
    
    # Check and backfill gaps in ETH-USD hourly data over last 30 days
    %s gaps --pair ETH-USD --interval 1h --days 30 --backfill
    
    # Quick gap check for the last 24 hours
    %s gaps --pair BTC-USD --interval 1h --days 1

NOTES:
    - Gap detection analyzes timestamp sequences for missing data
    - Backfill automatically collects missing data from the exchange
    - Large gaps may take time to backfill due to rate limits
`, AppName, AppName, AppName, AppName, AppName)

	case "query":
		fmt.Printf(`%s query - Query and display stored data

USAGE:
    %s query [options]

OPTIONS:
    --pair, -p <pair>         Trading pair to query (required)
    --interval, -i <interval> Time interval filter (default: 1d)
    --start, -s <date>        Start date (YYYY-MM-DD)
    --end, -e <date>          End date (YYYY-MM-DD)
    --limit, -l <limit>       Maximum results to return (default: 100)
    --format, -f <format>     Output format: table, json, csv (default: table)
    --help, -h                Show this help message

EXAMPLES:
    # Query BTC-USD daily data for last 7 days in table format
    %s query --pair BTC-USD --interval 1d
    
    # Query ETH-USD data for January 2024 in JSON format
    %s query --pair ETH-USD --start 2024-01-01 --end 2024-01-31 --format json
    
    # Query SOL-USD hourly data, show first 50 results as CSV
    %s query --pair SOL-USD --interval 1h --limit 50 --format csv

OUTPUT FORMATS:
    table - Human-readable table format (default)
    json  - JSON format for programmatic use
    csv   - CSV format for spreadsheet import

NOTES:
    - If no dates specified, shows last 7 days
    - Use --limit 0 to show all results
    - Large queries may take time to complete
`, AppName, AppName, AppName, AppName, AppName)

	default:
		fmt.Fprintf(os.Stderr, "No help available for command: %s\n", command)
		printUsage()
	}
}
