// Package errors provides comprehensive error handling with retry classification,
// circuit breakers, and structured error reporting for the OHLCV collector.
// This module implements smart retry logic based on error types and provides
// consistent error handling patterns across all components.
package errors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/johnayoung/go-ohlcv-collector/internal/config"
)

// ErrorType represents the classification of an error
type ErrorType string

const (
	// Retryable error types
	ErrorTypeNetwork        ErrorType = "network"          // Network connectivity issues
	ErrorTypeTimeout        ErrorType = "timeout"          // Request timeout
	ErrorTypeRateLimit      ErrorType = "rate_limit"       // Rate limiting from external service
	ErrorTypeServerError    ErrorType = "server_error"     // HTTP 5xx errors
	ErrorTypeTemporary      ErrorType = "temporary"        // Temporary failures
	ErrorTypeCircuitOpen    ErrorType = "circuit_open"     // Circuit breaker is open
	
	// Non-retryable error types
	ErrorTypeAuthentication ErrorType = "authentication"   // Authentication/authorization failures
	ErrorTypeBadRequest     ErrorType = "bad_request"      // HTTP 4xx errors (except rate limit)
	ErrorTypeValidation     ErrorType = "validation"       // Data validation errors
	ErrorTypeConfiguration ErrorType = "configuration"    // Configuration errors
	ErrorTypePanic          ErrorType = "panic"            // Panic recovery
	ErrorTypeInternal       ErrorType = "internal"         // Internal application errors
	
	// Special error types
	ErrorTypeUnknown        ErrorType = "unknown"          // Unclassified errors
	ErrorTypeFatal          ErrorType = "fatal"            // Fatal errors that should stop processing
)

// Severity represents the severity level of an error
type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

// String returns the string representation of the severity
func (s Severity) String() string {
	switch s {
	case SeverityLow:
		return "low"
	case SeverityMedium:
		return "medium"
	case SeverityHigh:
		return "high"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// ClassifiedError represents an error with metadata for handling decisions
type ClassifiedError struct {
	Err         error                  `json:"error"`
	Type        ErrorType              `json:"type"`
	Severity    Severity               `json:"severity"`
	Retryable   bool                   `json:"retryable"`
	Component   string                 `json:"component"`
	Operation   string                 `json:"operation"`
	Context     map[string]interface{} `json:"context"`
	Timestamp   time.Time              `json:"timestamp"`
	Attempts    int                    `json:"attempts"`
	LastAttempt time.Time              `json:"last_attempt"`
}

// Error implements the error interface
func (ce *ClassifiedError) Error() string {
	return fmt.Sprintf("[%s/%s] %s: %v", ce.Component, ce.Type, ce.Operation, ce.Err)
}

// Unwrap returns the underlying error
func (ce *ClassifiedError) Unwrap() error {
	return ce.Err
}

// Is checks if the error is of the specified type
func (ce *ClassifiedError) Is(target error) bool {
	if t, ok := target.(*ClassifiedError); ok {
		return ce.Type == t.Type
	}
	return errors.Is(ce.Err, target)
}

// ErrorClassifier handles error classification and retry logic
type ErrorClassifier struct {
	config       config.ErrorHandlingConfig
	logger       *slog.Logger
	mu           sync.RWMutex
	stats        map[ErrorType]ErrorStats
	circuitBreakers map[string]*CircuitBreaker
}

// ErrorStats tracks error statistics for monitoring
type ErrorStats struct {
	Count       int64     `json:"count"`
	LastSeen    time.Time `json:"last_seen"`
	FirstSeen   time.Time `json:"first_seen"`
	Retries     int64     `json:"retries"`
	Successes   int64     `json:"successes"`
}

// NewErrorClassifier creates a new error classifier with the given configuration
func NewErrorClassifier(config config.ErrorHandlingConfig, logger *slog.Logger) *ErrorClassifier {
	if logger == nil {
		logger = slog.Default()
	}

	ec := &ErrorClassifier{
		config:          config,
		logger:          logger,
		stats:           make(map[ErrorType]ErrorStats),
		circuitBreakers: make(map[string]*CircuitBreaker),
	}

	return ec
}

// Classify analyzes an error and returns a ClassifiedError with retry metadata
func (ec *ErrorClassifier) Classify(err error, component, operation string) *ClassifiedError {
	if err == nil {
		return nil
	}

	// Check if already classified
	if ce, ok := err.(*ClassifiedError); ok {
		return ce
	}

	// Classify the error type
	errorType := ec.classifyErrorType(err)
	severity := ec.determineSeverity(errorType, err)
	retryable := ec.isRetryable(errorType, err)

	classified := &ClassifiedError{
		Err:         err,
		Type:        errorType,
		Severity:    severity,
		Retryable:   retryable,
		Component:   component,
		Operation:   operation,
		Context:     make(map[string]interface{}),
		Timestamp:   time.Now(),
		Attempts:    0,
		LastAttempt: time.Time{},
	}

	// Update statistics
	ec.updateStats(errorType)

	ec.logger.Debug("error classified",
		"type", errorType,
		"severity", severity.String(),
		"retryable", retryable,
		"component", component,
		"operation", operation,
		"error", err.Error())

	return classified
}

// classifyErrorType determines the error type based on the error content
func (ec *ErrorClassifier) classifyErrorType(err error) ErrorType {
	errStr := strings.ToLower(err.Error())

	// Network-related errors
	if isNetworkError(err) {
		return ErrorTypeNetwork
	}

	// Timeout errors
	if isTimeoutError(err) {
		return ErrorTypeTimeout
	}

	// Rate limit errors (common patterns)
	if strings.Contains(errStr, "rate limit") || 
	   strings.Contains(errStr, "too many requests") ||
	   strings.Contains(errStr, "quota exceeded") {
		return ErrorTypeRateLimit
	}

	// Authentication errors
	if strings.Contains(errStr, "unauthorized") ||
	   strings.Contains(errStr, "forbidden") ||
	   strings.Contains(errStr, "authentication") ||
	   strings.Contains(errStr, "invalid credentials") {
		return ErrorTypeAuthentication
	}

	// Validation errors
	if strings.Contains(errStr, "validation") ||
	   strings.Contains(errStr, "invalid") ||
	   strings.Contains(errStr, "malformed") ||
	   strings.Contains(errStr, "parse") {
		return ErrorTypeValidation
	}

	// Configuration errors
	if strings.Contains(errStr, "config") ||
	   strings.Contains(errStr, "missing required") ||
	   strings.Contains(errStr, "not configured") {
		return ErrorTypeConfiguration
	}

	// Server errors (generic patterns)
	if strings.Contains(errStr, "server error") ||
	   strings.Contains(errStr, "internal server") ||
	   strings.Contains(errStr, "service unavailable") {
		return ErrorTypeServerError
	}

	// Panic recovery
	if strings.Contains(errStr, "panic") ||
	   strings.Contains(errStr, "runtime error") {
		return ErrorTypePanic
	}

	return ErrorTypeUnknown
}

// isNetworkError checks if the error is network-related
func isNetworkError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for common network error patterns
	errStr := strings.ToLower(err.Error())
	networkPatterns := []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"connection aborted",
		"no route to host",
		"host unreachable",
		"network unreachable",
		"dns",
		"resolve",
	}

	for _, pattern := range networkPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// isTimeoutError checks if the error is timeout-related
func isTimeoutError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") ||
		   strings.Contains(errStr, "deadline exceeded") ||
		   strings.Contains(errStr, "context canceled") ||
		   strings.Contains(errStr, "context deadline exceeded")
}

// determineSeverity assigns a severity level based on error type
func (ec *ErrorClassifier) determineSeverity(errorType ErrorType, err error) Severity {
	switch errorType {
	case ErrorTypeFatal, ErrorTypePanic:
		return SeverityCritical
	case ErrorTypeAuthentication, ErrorTypeConfiguration:
		return SeverityHigh
	case ErrorTypeValidation, ErrorTypeBadRequest:
		return SeverityMedium
	case ErrorTypeNetwork, ErrorTypeTimeout, ErrorTypeRateLimit:
		return SeverityLow
	default:
		return SeverityMedium
	}
}

// isRetryable determines if an error type should be retried
func (ec *ErrorClassifier) isRetryable(errorType ErrorType, err error) bool {
	// Check configuration for retryable error types
	for _, retryableType := range ec.config.GlobalRetryPolicy.RetryableErrors {
		if string(errorType) == retryableType {
			return true
		}
	}

	// Default retryable types
	switch errorType {
	case ErrorTypeNetwork, ErrorTypeTimeout, ErrorTypeRateLimit, 
		 ErrorTypeServerError, ErrorTypeTemporary:
		return true
	case ErrorTypeAuthentication, ErrorTypeBadRequest, ErrorTypeValidation,
		 ErrorTypeConfiguration, ErrorTypeFatal, ErrorTypePanic:
		return false
	default:
		// Unknown errors are retryable with caution
		return true
	}
}

// updateStats updates error statistics
func (ec *ErrorClassifier) updateStats(errorType ErrorType) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	stats := ec.stats[errorType]
	stats.Count++
	stats.LastSeen = time.Now()
	
	if stats.FirstSeen.IsZero() {
		stats.FirstSeen = stats.LastSeen
	}

	ec.stats[errorType] = stats
}

// Retry executes a function with retry logic based on classified errors
func (ec *ErrorClassifier) Retry(ctx context.Context, component, operation string, fn func() error) error {
	policy := ec.getRetryPolicy(component)
	
	// Create backoff strategy
	backoffStrategy := ec.createBackoffStrategy(policy)
	
	var lastErr error
	attempts := 0
	maxAttempts := policy.MaxAttempts
	
	for {
		attempts++
		
		// Execute the function
		err := fn()
		if err == nil {
			// Success - update statistics
			ec.recordSuccess(component, operation, attempts)
			return nil
		}

		// Classify the error
		classified := ec.Classify(err, component, operation)
		classified.Attempts = attempts
		classified.LastAttempt = time.Now()
		lastErr = classified

		ec.logger.Warn("operation failed",
			"component", component,
			"operation", operation,
			"attempt", attempts,
			"max_attempts", maxAttempts,
			"error_type", classified.Type,
			"retryable", classified.Retryable,
			"error", err.Error())

		// Check if we should retry
		if !classified.Retryable || attempts >= maxAttempts {
			break
		}

		// Check context cancellation
		if ctx.Err() != nil {
			return fmt.Errorf("context canceled during retry: %w", ctx.Err())
		}

		// Wait before retry
		nextBackoff := backoffStrategy.NextBackOff()
		if nextBackoff == backoff.Stop {
			break
		}

		select {
		case <-time.After(nextBackoff):
			// Continue to next attempt
		case <-ctx.Done():
			return fmt.Errorf("context canceled during backoff: %w", ctx.Err())
		}
	}

	// All retries exhausted
	ec.recordFailure(component, operation, attempts)
	return fmt.Errorf("operation failed after %d attempts: %w", attempts, lastErr)
}

// getRetryPolicy returns the retry policy for a component
func (ec *ErrorClassifier) getRetryPolicy(component string) config.RetryPolicyConfig {
	// Check for component-specific policy
	if policy, exists := ec.config.ComponentPolicies[component]; exists {
		return policy
	}
	// Fall back to global policy
	return ec.config.GlobalRetryPolicy
}

// createBackoffStrategy creates a backoff strategy based on configuration
func (ec *ErrorClassifier) createBackoffStrategy(policy config.RetryPolicyConfig) backoff.BackOff {
	initialDelay, _ := time.ParseDuration(policy.InitialDelay)
	maxDelay, _ := time.ParseDuration(policy.MaxDelay)
	
	var strategy backoff.BackOff
	
	switch policy.BackoffStrategy {
	case "fixed":
		strategy = backoff.NewConstantBackOff(initialDelay)
	case "linear":
		// Simple linear backoff implementation
		strategy = &LinearBackoff{
			interval: initialDelay,
			max:      maxDelay,
		}
	case "exponential":
		fallthrough
	default:
		exponential := backoff.NewExponentialBackOff()
		exponential.InitialInterval = initialDelay
		exponential.MaxInterval = maxDelay
		exponential.MaxElapsedTime = 0 // No max elapsed time
		strategy = exponential
	}

	// Add jitter if configured (manual implementation since WithJitter may not be available)
	if policy.Jitter {
		strategy = &JitteredBackoff{BackOff: strategy}
	}

	return backoff.WithMaxRetries(strategy, uint64(policy.MaxAttempts-1))
}

// recordSuccess updates success statistics
func (ec *ErrorClassifier) recordSuccess(component, operation string, attempts int) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	// Update global success stats if needed
	for errorType, stats := range ec.stats {
		stats.Successes++
		ec.stats[errorType] = stats
	}

	ec.logger.Debug("operation succeeded",
		"component", component,
		"operation", operation,
		"attempts", attempts)
}

// recordFailure updates failure statistics
func (ec *ErrorClassifier) recordFailure(component, operation string, attempts int) {
	ec.logger.Error("operation failed after all retries",
		"component", component,
		"operation", operation,
		"attempts", attempts)
}

// GetStats returns error statistics
func (ec *ErrorClassifier) GetStats() map[ErrorType]ErrorStats {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	stats := make(map[ErrorType]ErrorStats)
	for k, v := range ec.stats {
		stats[k] = v
	}
	
	return stats
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	name           string
	config         config.CircuitBreakerConfig
	state          CircuitState
	failures       int
	lastFailure    time.Time
	nextRetry      time.Time
	testRequests   int
	mu             sync.RWMutex
}

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// String returns the string representation of the circuit state
func (cs CircuitState) String() string {
	switch cs {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string, config config.CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		name:   name,
		config: config,
		state:  CircuitClosed,
	}
}

// Call executes a function through the circuit breaker
func (cb *CircuitBreaker) Call(fn func() error) error {
	if !cb.allowRequest() {
		return &ClassifiedError{
			Err:       fmt.Errorf("circuit breaker is open for %s", cb.name),
			Type:      ErrorTypeCircuitOpen,
			Severity:  SeverityMedium,
			Retryable: true,
			Component: "circuit_breaker",
			Operation: cb.name,
			Timestamp: time.Now(),
		}
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// allowRequest checks if a request should be allowed through
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		return time.Now().After(cb.nextRetry)
	case CircuitHalfOpen:
		return cb.testRequests < cb.config.HalfOpenRequests
	default:
		return false
	}
}

// recordResult records the result of a request
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		// Success
		cb.onSuccess()
	} else {
		// Failure
		cb.onFailure()
	}
}

// onSuccess handles successful requests
func (cb *CircuitBreaker) onSuccess() {
	switch cb.state {
	case CircuitHalfOpen:
		cb.testRequests++
		if cb.testRequests >= cb.config.HalfOpenRequests {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.testRequests = 0
		}
	case CircuitClosed:
		cb.failures = 0
	}
}

// onFailure handles failed requests
func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
			cb.setNextRetry()
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.testRequests = 0
		cb.setNextRetry()
	}
}

// setNextRetry sets the next retry time
func (cb *CircuitBreaker) setNextRetry() {
	timeout, _ := time.ParseDuration(cb.config.RecoveryTimeout)
	cb.nextRetry = time.Now().Add(timeout)
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// LinearBackoff implements a simple linear backoff strategy
type LinearBackoff struct {
	interval time.Duration
	max      time.Duration
	current  time.Duration
}

// NextBackOff returns the next backoff interval
func (lb *LinearBackoff) NextBackOff() time.Duration {
	if lb.current == 0 {
		lb.current = lb.interval
	} else {
		lb.current += lb.interval
	}

	if lb.current > lb.max {
		lb.current = lb.max
	}

	return lb.current
}

// Reset resets the backoff to its initial state
func (lb *LinearBackoff) Reset() {
	lb.current = 0
}

// JitteredBackoff adds jitter to another backoff strategy
type JitteredBackoff struct {
	backoff.BackOff
}

// NextBackOff returns the next backoff interval with jitter
func (jb *JitteredBackoff) NextBackOff() time.Duration {
	next := jb.BackOff.NextBackOff()
	if next == backoff.Stop {
		return next
	}
	
	// Add ±10% jitter
	jitter := float64(next) * 0.1
	offset := (2.0 * float64(time.Now().UnixNano()%1000) / 1000.0 - 1.0) * jitter
	return next + time.Duration(offset)
}

// Utility functions

// WrapError wraps an error with additional context
func WrapError(err error, component, operation, message string) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s in %s.%s: %w", message, component, operation, err)
}

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	if ce, ok := err.(*ClassifiedError); ok {
		return ce.Retryable
	}
	return false
}

// GetErrorType extracts the error type from a classified error
func GetErrorType(err error) ErrorType {
	if ce, ok := err.(*ClassifiedError); ok {
		return ce.Type
	}
	return ErrorTypeUnknown
}

// GetSeverity extracts the severity from a classified error
func GetSeverity(err error) Severity {
	if ce, ok := err.(*ClassifiedError); ok {
		return ce.Severity
	}
	return SeverityMedium
}