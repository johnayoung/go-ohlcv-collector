// Package logger provides structured logging with context propagation for the OHLCV collector.
// This module implements context-aware logging using the standard library's slog package,
// with support for request tracing, component-specific loggers, and configurable output formats.
package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ContextKey represents keys for context values
type ContextKey string

const (
	// TraceIDKey is the context key for trace ID
	TraceIDKey ContextKey = "trace_id"
	// ComponentKey is the context key for component name
	ComponentKey ContextKey = "component"
	// OperationKey is the context key for operation name
	OperationKey ContextKey = "operation"
	// UserIDKey is the context key for user ID
	UserIDKey ContextKey = "user_id"
	// RequestIDKey is the context key for request ID
	RequestIDKey ContextKey = "request_id"
	// PairKey is the context key for trading pair
	PairKey ContextKey = "pair"
	// IntervalKey is the context key for time interval
	IntervalKey ContextKey = "interval"
	// JobIDKey is the context key for collection job ID
	JobIDKey ContextKey = "job_id"
)

// LoggerManager manages structured logging for the application
type LoggerManager struct {
	baseLogger     *slog.Logger
	config         config.LoggingConfig
	writer         io.WriteCloser
	componentCache map[string]*slog.Logger
}

// ComponentLogger represents a logger for a specific component
type ComponentLogger struct {
	*slog.Logger
	component string
}

// NewLoggerManager creates a new logger manager with the specified configuration
func NewLoggerManager(cfg config.LoggingConfig) (*LoggerManager, error) {
	// Create the appropriate writer based on configuration
	writer, err := createWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create log writer: %w", err)
	}

	// Create handler options
	opts := &slog.HandlerOptions{
		Level:     parseLogLevel(cfg.Level),
		AddSource: cfg.Level == "debug",
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Customize attribute formatting
			switch a.Key {
			case slog.TimeKey:
				// Use ISO 8601 format for timestamps
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format(time.RFC3339Nano))
				}
			case slog.LevelKey:
				// Use uppercase level names
				if level, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(strings.ToUpper(level.String()))
				}
			}
			return a
		},
	}

	// Create the appropriate handler based on format
	var handler slog.Handler
	switch cfg.Format {
	case "json":
		handler = slog.NewJSONHandler(writer, opts)
	case "text":
		handler = slog.NewTextHandler(writer, opts)
	default:
		handler = slog.NewJSONHandler(writer, opts)
	}

	// Create base logger with context fields
	baseAttrs := make([]slog.Attr, 0, len(cfg.ContextFields))
	for key, value := range cfg.ContextFields {
		baseAttrs = append(baseAttrs, slog.String(key, value))
	}

	var baseLogger *slog.Logger
	if len(baseAttrs) > 0 {
		baseLogger = slog.New(handler.WithAttrs(baseAttrs))
	} else {
		baseLogger = slog.New(handler)
	}

	return &LoggerManager{
		baseLogger:     baseLogger,
		config:         cfg,
		writer:         writer,
		componentCache: make(map[string]*slog.Logger),
	}, nil
}

// createWriter creates the appropriate writer based on configuration
func createWriter(cfg config.LoggingConfig) (io.WriteCloser, error) {
	switch cfg.Output {
	case "stdout":
		return nopWriteCloser{os.Stdout}, nil
	case "stderr":
		return nopWriteCloser{os.Stderr}, nil
	case "file":
		if cfg.FilePath == "" {
			return nil, fmt.Errorf("file path is required when output is 'file'")
		}

		// Ensure directory exists
		dir := filepath.Dir(cfg.FilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		// Create rotating file logger
		lj := &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSize, // MB
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge, // days
			Compress:   cfg.Compress,
		}
		return lj, nil
	default:
		return nopWriteCloser{os.Stdout}, nil
	}
}

// nopWriteCloser wraps an io.Writer to provide a Close method
type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

// parseLogLevel converts string log level to slog.Level
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// GetLogger returns the base logger instance
func (lm *LoggerManager) GetLogger() *slog.Logger {
	return lm.baseLogger
}

// GetComponentLogger returns a logger for the specified component
func (lm *LoggerManager) GetComponentLogger(component string) *ComponentLogger {
	if cached, exists := lm.componentCache[component]; exists {
		return &ComponentLogger{Logger: cached, component: component}
	}

	// Create component-specific logger with component attribute
	componentLogger := lm.baseLogger.With(slog.String("component", component))
	lm.componentCache[component] = componentLogger

	return &ComponentLogger{Logger: componentLogger, component: component}
}

// WithContext creates a logger that includes context values
func (lm *LoggerManager) WithContext(ctx context.Context) *slog.Logger {
	attrs := extractContextAttributes(ctx)
	if len(attrs) == 0 {
		return lm.baseLogger
	}
	return lm.baseLogger.With(attrs...)
}

// WithComponentContext creates a component logger that includes context values
func (lm *LoggerManager) WithComponentContext(ctx context.Context, component string) *ComponentLogger {
	attrs := extractContextAttributes(ctx)
	attrs = append(attrs, slog.String("component", component))

	logger := lm.baseLogger.With(attrs...)
	return &ComponentLogger{Logger: logger, component: component}
}

// extractContextAttributes extracts logging attributes from context
func extractContextAttributes(ctx context.Context) []interface{} {
	var attrs []interface{}

	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		attrs = append(attrs, slog.String("trace_id", traceID))
	}

	if requestID, ok := ctx.Value(RequestIDKey).(string); ok && requestID != "" {
		attrs = append(attrs, slog.String("request_id", requestID))
	}

	if operation, ok := ctx.Value(OperationKey).(string); ok && operation != "" {
		attrs = append(attrs, slog.String("operation", operation))
	}

	if userID, ok := ctx.Value(UserIDKey).(string); ok && userID != "" {
		attrs = append(attrs, slog.String("user_id", userID))
	}

	if pair, ok := ctx.Value(PairKey).(string); ok && pair != "" {
		attrs = append(attrs, slog.String("pair", pair))
	}

	if interval, ok := ctx.Value(IntervalKey).(string); ok && interval != "" {
		attrs = append(attrs, slog.String("interval", interval))
	}

	if jobID, ok := ctx.Value(JobIDKey).(string); ok && jobID != "" {
		attrs = append(attrs, slog.String("job_id", jobID))
	}

	return attrs
}

// Close closes the logger and any associated resources
func (lm *LoggerManager) Close() error {
	if lm.writer != nil {
		return lm.writer.Close()
	}
	return nil
}

// WithTraceID adds a trace ID to the context
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// WithComponent adds a component name to the context
func WithComponent(ctx context.Context, component string) context.Context {
	return context.WithValue(ctx, ComponentKey, component)
}

// WithOperation adds an operation name to the context
func WithOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, OperationKey, operation)
}

// WithUserID adds a user ID to the context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// WithRequestID adds a request ID to the context
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, requestID)
}

// WithPair adds a trading pair to the context
func WithPair(ctx context.Context, pair string) context.Context {
	return context.WithValue(ctx, PairKey, pair)
}

// WithInterval adds a time interval to the context
func WithInterval(ctx context.Context, interval string) context.Context {
	return context.WithValue(ctx, IntervalKey, interval)
}

// WithJobID adds a job ID to the context
func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, JobIDKey, jobID)
}

// GetTraceID extracts the trace ID from context
func GetTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
		return traceID
	}
	return ""
}

// GetComponent extracts the component name from context
func GetComponent(ctx context.Context) string {
	if component, ok := ctx.Value(ComponentKey).(string); ok {
		return component
	}
	return ""
}

// GetOperation extracts the operation name from context
func GetOperation(ctx context.Context) string {
	if operation, ok := ctx.Value(OperationKey).(string); ok {
		return operation
	}
	return ""
}

// GetPair extracts the trading pair from context
func GetPair(ctx context.Context) string {
	if pair, ok := ctx.Value(PairKey).(string); ok {
		return pair
	}
	return ""
}

// GetJobID extracts the job ID from context
func GetJobID(ctx context.Context) string {
	if jobID, ok := ctx.Value(JobIDKey).(string); ok {
		return jobID
	}
	return ""
}

// ComponentLogger methods for enhanced functionality

// WithOperation returns a logger with an operation context
func (cl *ComponentLogger) WithOperation(operation string) *slog.Logger {
	return cl.With(slog.String("operation", operation))
}

// WithPair returns a logger with a trading pair context
func (cl *ComponentLogger) WithPair(pair string) *slog.Logger {
	return cl.With(slog.String("pair", pair))
}

// WithJobID returns a logger with a job ID context
func (cl *ComponentLogger) WithJobID(jobID string) *slog.Logger {
	return cl.With(slog.String("job_id", jobID))
}

// WithDuration logs an operation with its duration
func (cl *ComponentLogger) WithDuration(operation string, duration time.Duration, level slog.Level, msg string, args ...interface{}) {
	cl.Log(context.Background(), level, msg,
		append([]interface{}{
			slog.String("operation", operation),
			slog.Duration("duration", duration),
		}, args...)...)
}

// ErrorWithContext logs an error with full context information
func (cl *ComponentLogger) ErrorWithContext(ctx context.Context, msg string, err error, args ...interface{}) {
	attrs := extractContextAttributes(ctx)
	attrs = append(attrs, slog.Any("error", err))
	attrs = append(attrs, args...)
	cl.Error(msg, attrs...)
}

// WarnWithContext logs a warning with full context information
func (cl *ComponentLogger) WarnWithContext(ctx context.Context, msg string, args ...interface{}) {
	attrs := extractContextAttributes(ctx)
	attrs = append(attrs, args...)
	cl.Warn(msg, attrs...)
}

// InfoWithContext logs info with full context information
func (cl *ComponentLogger) InfoWithContext(ctx context.Context, msg string, args ...interface{}) {
	attrs := extractContextAttributes(ctx)
	attrs = append(attrs, args...)
	cl.Info(msg, attrs...)
}

// DebugWithContext logs debug information with full context
func (cl *ComponentLogger) DebugWithContext(ctx context.Context, msg string, args ...interface{}) {
	attrs := extractContextAttributes(ctx)
	attrs = append(attrs, args...)
	cl.Debug(msg, attrs...)
}

// LogOperation logs the start and end of an operation with timing
func (cl *ComponentLogger) LogOperation(ctx context.Context, operation string, fn func() error) error {
	start := time.Now()
	cl.InfoWithContext(ctx, "operation started", slog.String("operation", operation))

	err := fn()
	duration := time.Since(start)

	if err != nil {
		cl.ErrorWithContext(ctx, "operation failed", err,
			slog.String("operation", operation),
			slog.Duration("duration", duration))
		return err
	}

	cl.InfoWithContext(ctx, "operation completed",
		slog.String("operation", operation),
		slog.Duration("duration", duration))

	return nil
}

// Utility functions for common logging patterns

// NewTraceLogger creates a new logger with a generated trace ID
func NewTraceLogger(lm *LoggerManager, component string) (*ComponentLogger, context.Context) {
	traceID := generateTraceID()
	ctx := WithTraceID(context.Background(), traceID)
	logger := lm.WithComponentContext(ctx, component)
	return logger, ctx
}

// generateTraceID generates a simple trace ID (in production, use proper UUID or similar)
func generateTraceID() string {
	return fmt.Sprintf("trace_%d", time.Now().UnixNano())
}

// LoggerMiddleware creates middleware that adds request context to loggers
type LoggerMiddleware struct {
	manager *LoggerManager
}

// NewLoggerMiddleware creates a new logger middleware
func NewLoggerMiddleware(manager *LoggerManager) *LoggerMiddleware {
	return &LoggerMiddleware{manager: manager}
}

// AddRequestContext adds request-specific context to the logger
func (lm *LoggerMiddleware) AddRequestContext(ctx context.Context, requestID, userID string) context.Context {
	ctx = WithRequestID(ctx, requestID)
	if userID != "" {
		ctx = WithUserID(ctx, userID)
	}
	return ctx
}

// Performance logging helpers

// TimedOperation logs an operation with automatic timing
func TimedOperation(logger *slog.Logger, operation string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)

	if err != nil {
		logger.Error("timed operation failed",
			slog.String("operation", operation),
			slog.Duration("duration", duration),
			slog.Any("error", err))
		return err
	}

	logger.Info("timed operation completed",
		slog.String("operation", operation),
		slog.Duration("duration", duration))

	return nil
}

// TimedOperationWithContext logs an operation with context and timing
func TimedOperationWithContext(ctx context.Context, logger *slog.Logger, operation string, fn func() error) error {
	start := time.Now()

	// Add operation to context
	ctx = WithOperation(ctx, operation)

	logger.InfoContext(ctx, "operation started")

	err := fn()
	duration := time.Since(start)

	if err != nil {
		logger.ErrorContext(ctx, "operation failed",
			slog.Duration("duration", duration),
			slog.Any("error", err))
		return err
	}

	logger.InfoContext(ctx, "operation completed",
		slog.Duration("duration", duration))

	return nil
}

// Structured error logging

// LogError logs an error with structured context
func LogError(logger *slog.Logger, err error, msg string, attrs ...interface{}) {
	allAttrs := append([]interface{}{slog.Any("error", err)}, attrs...)
	logger.Error(msg, allAttrs...)
}

// LogErrorWithContext logs an error with full context
func LogErrorWithContext(ctx context.Context, logger *slog.Logger, err error, msg string, attrs ...interface{}) {
	contextAttrs := extractContextAttributes(ctx)
	contextAttrs = append(contextAttrs, slog.Any("error", err))
	contextAttrs = append(contextAttrs, attrs...)
	logger.Error(msg, contextAttrs...)
}
