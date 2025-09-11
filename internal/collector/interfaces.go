// Package collector interfaces and types for integration with other components
package collector

import (
	"fmt"
	"time"
)

// Error types for better error classification and handling

// CollectionError represents errors during data collection operations
type CollectionError struct {
	Type      string    // "fetch", "validation", "storage", "gap_detection"
	Operation string    // Specific operation that failed
	Pair      string    // Trading pair involved
	Err       error     // Underlying error
	Timestamp time.Time // When the error occurred
	Retryable bool      // Whether this error is retryable
}

func (e *CollectionError) Error() string {
	return fmt.Sprintf("collection error [%s:%s] for %s: %v", e.Type, e.Operation, e.Pair, e.Err)
}

func (e *CollectionError) Unwrap() error {
	return e.Err
}

// IsRetryable returns whether this error can be retried
func (e *CollectionError) IsRetryable() bool {
	return e.Retryable
}

// NewCollectionError creates a new collection error
func NewCollectionError(errorType, operation, pair string, err error, retryable bool) *CollectionError {
	return &CollectionError{
		Type:      errorType,
		Operation: operation,
		Pair:      pair,
		Err:       err,
		Timestamp: time.Now(),
		Retryable: retryable,
	}
}

// RateLimitError represents rate limiting errors
type RateLimitError struct {
	RetryAfter time.Duration
	Err        error
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %v: %v", e.RetryAfter, e.Err)
}

func (e *RateLimitError) Unwrap() error {
	return e.Err
}

// ValidationError represents data validation errors
type ValidationError struct {
	Pair     string
	Candle   interface{} // The candle that failed validation
	Issue    string      // Description of the validation issue
	Severity string      // "warning", "error", "critical"
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for %s: %s (severity: %s)", e.Pair, e.Issue, e.Severity)
}

// StorageError represents storage operation errors
type StorageError struct {
	Operation string // "store", "query", "delete"
	Table     string // Database table involved
	Err       error
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("storage error during %s on %s: %v", e.Operation, e.Table, e.Err)
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

// Status and health check types

// CollectorStatus represents the overall status of the collector
type CollectorStatus string

const (
	StatusStopped    CollectorStatus = "stopped"
	StatusStarting   CollectorStatus = "starting"
	StatusRunning    CollectorStatus = "running"
	StatusStopping   CollectorStatus = "stopping"
	StatusError      CollectorStatus = "error"
	StatusDegraded   CollectorStatus = "degraded"
)

// HealthStatus represents health check results
type HealthStatus struct {
	Status       CollectorStatus       `json:"status"`
	Healthy      bool                  `json:"healthy"`
	LastChecked  time.Time             `json:"last_checked"`
	Components   map[string]bool       `json:"components"`   // Component health status
	Errors       []string              `json:"errors"`       // Recent errors
	Uptime       time.Duration         `json:"uptime"`       // How long collector has been running
	MemoryUsageMB int64                `json:"memory_usage_mb"`
}

// ComponentHealth represents individual component health
type ComponentHealth struct {
	Name        string        `json:"name"`
	Healthy     bool          `json:"healthy"`
	LastChecked time.Time     `json:"last_checked"`
	LastError   string        `json:"last_error,omitempty"`
	ResponseTime time.Duration `json:"response_time"`
}

// Configuration validation

// ValidateConfig validates collector configuration
func ValidateConfig(config *Config) error {
	if config.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be positive, got %d", config.WorkerCount)
	}

	if config.WorkerCount > 50 {
		return fmt.Errorf("worker count too high, maximum 50, got %d", config.WorkerCount)
	}

	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", config.BatchSize)
	}

	if config.BatchSize > 100000 {
		return fmt.Errorf("batch size too high, maximum 100000, got %d", config.BatchSize)
	}

	if config.RateLimit <= 0 {
		return fmt.Errorf("rate limit must be positive, got %d", config.RateLimit)
	}

	if config.RateLimit > 100 {
		return fmt.Errorf("rate limit too high, maximum 100, got %d", config.RateLimit)
	}

	if config.MemoryLimitMB <= 0 {
		return fmt.Errorf("memory limit must be positive, got %d", config.MemoryLimitMB)
	}

	if config.MemoryLimitMB > 10000 {
		return fmt.Errorf("memory limit too high, maximum 10000MB, got %d", config.MemoryLimitMB)
	}

	if config.RetryAttempts < 0 {
		return fmt.Errorf("retry attempts cannot be negative, got %d", config.RetryAttempts)
	}

	if config.RetryAttempts > 10 {
		return fmt.Errorf("retry attempts too high, maximum 10, got %d", config.RetryAttempts)
	}

	return nil
}

// Helper functions for error classification

// IsRetryableError determines if an error can be retried
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error types
	switch e := err.(type) {
	case *CollectionError:
		return e.IsRetryable()
	case *RateLimitError:
		return true
	case *StorageError:
		// Most storage errors are not retryable except for connection issues
		return false
	case *ValidationError:
		// Validation errors are generally not retryable as they indicate data issues
		return false
	}

	// Check for common retryable error conditions
	errMsg := err.Error()
	retryableConditions := []string{
		"timeout",
		"connection refused",
		"temporary failure",
		"rate limit",
		"server error",
		"service unavailable",
		"too many requests",
	}

	for _, condition := range retryableConditions {
		if contains(errMsg, condition) {
			return true
		}
	}

	return false
}

// IsCriticalError determines if an error is critical and should stop processing
func IsCriticalError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific critical error conditions
	errMsg := err.Error()
	criticalConditions := []string{
		"authentication failed",
		"unauthorized",
		"forbidden",
		"invalid api key",
		"configuration error",
		"database corruption",
		"out of memory",
		"disk full",
	}

	for _, condition := range criticalConditions {
		if contains(errMsg, condition) {
			return true
		}
	}

	// Storage errors related to schema or corruption are critical
	if storageErr, ok := err.(*StorageError); ok {
		criticalStorageConditions := []string{
			"schema",
			"corruption",
			"constraint violation",
			"foreign key",
		}
		
		for _, condition := range criticalStorageConditions {
			if contains(storageErr.Error(), condition) {
				return true
			}
		}
	}

	return false
}

// GetErrorCategory categorizes errors for metrics and monitoring
func GetErrorCategory(err error) string {
	if err == nil {
		return "none"
	}

	switch err.(type) {
	case *CollectionError:
		return "collection"
	case *RateLimitError:
		return "rate_limit"
	case *StorageError:
		return "storage"
	case *ValidationError:
		return "validation"
	}

	// Categorize based on error message patterns
	errMsg := err.Error()
	
	if contains(errMsg, "timeout") {
		return "timeout"
	}
	if contains(errMsg, "network") || contains(errMsg, "connection") {
		return "network"
	}
	if contains(errMsg, "context canceled") || contains(errMsg, "context deadline") {
		return "context"
	}
	if contains(errMsg, "authentication") || contains(errMsg, "unauthorized") {
		return "auth"
	}
	if contains(errMsg, "rate limit") {
		return "rate_limit"
	}

	return "unknown"
}

// Helper function for string contains check (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		   (s == substr || 
		    len(s) > len(substr) && 
		    (hasSubstring(s, substr)))
}

func hasSubstring(s, substr string) bool {
	// Simple case-insensitive substring check
	sLower := make([]byte, len(s))
	substrLower := make([]byte, len(substr))
	
	for i, c := range []byte(s) {
		if c >= 'A' && c <= 'Z' {
			sLower[i] = c + 32
		} else {
			sLower[i] = c
		}
	}
	
	for i, c := range []byte(substr) {
		if c >= 'A' && c <= 'Z' {
			substrLower[i] = c + 32
		} else {
			substrLower[i] = c
		}
	}
	
	return bytesContains(sLower, substrLower)
}

func bytesContains(b, subslice []byte) bool {
	if len(subslice) == 0 {
		return true
	}
	if len(subslice) > len(b) {
		return false
	}
	
	for i := 0; i <= len(b)-len(subslice); i++ {
		if bytesEqual(b[i:i+len(subslice)], subslice) {
			return true
		}
	}
	return false
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}