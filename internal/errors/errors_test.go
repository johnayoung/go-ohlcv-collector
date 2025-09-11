package errors

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestErrorClassification(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	classifier := NewErrorClassifier(config.DefaultConfig().ErrorHandling, logger)

	tests := []struct {
		name           string
		error          error
		expectedType   ErrorType
		expectedRetryable bool
		expectedSeverity  Severity
	}{
		{
			name:             "network connection refused",
			error:            fmt.Errorf("connection refused"),
			expectedType:     ErrorTypeNetwork,
			expectedRetryable: true,
			expectedSeverity: SeverityLow,
		},
		{
			name:             "timeout error",
			error:            fmt.Errorf("context deadline exceeded"),
			expectedType:     ErrorTypeTimeout,
			expectedRetryable: true,
			expectedSeverity: SeverityLow,
		},
		{
			name:             "rate limit error",
			error:            fmt.Errorf("rate limit exceeded"),
			expectedType:     ErrorTypeRateLimit,
			expectedRetryable: true,
			expectedSeverity: SeverityLow,
		},
		{
			name:             "authentication error",
			error:            fmt.Errorf("unauthorized: invalid credentials"),
			expectedType:     ErrorTypeAuthentication,
			expectedRetryable: false,
			expectedSeverity: SeverityHigh,
		},
		{
			name:             "validation error",
			error:            fmt.Errorf("validation failed: invalid input"),
			expectedType:     ErrorTypeValidation,
			expectedRetryable: false,
			expectedSeverity: SeverityMedium,
		},
		{
			name:             "server error",
			error:            fmt.Errorf("internal server error"),
			expectedType:     ErrorTypeServerError,
			expectedRetryable: true,
			expectedSeverity: SeverityMedium,
		},
		{
			name:             "unknown error",
			error:            fmt.Errorf("something went wrong"),
			expectedType:     ErrorTypeUnknown,
			expectedRetryable: true,
			expectedSeverity: SeverityMedium,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classified := classifier.Classify(tt.error, "test_component", "test_operation")
			
			assert.Equal(t, tt.expectedType, classified.Type, "Error type mismatch")
			assert.Equal(t, tt.expectedRetryable, classified.Retryable, "Retryable mismatch")
			assert.Equal(t, tt.expectedSeverity, classified.Severity, "Severity mismatch")
			assert.Equal(t, "test_component", classified.Component)
			assert.Equal(t, "test_operation", classified.Operation)
			assert.NotZero(t, classified.Timestamp)
		})
	}
}

func TestNetworkErrorDetection(t *testing.T) {
	tests := []struct {
		name     string
		error    error
		expected bool
	}{
		{
			name:     "connection refused",
			error:    fmt.Errorf("connection refused"),
			expected: true,
		},
		{
			name:     "dns resolution failed",
			error:    fmt.Errorf("no such host: example.com"),
			expected: true,
		},
		{
			name:     "network unreachable",
			error:    fmt.Errorf("network unreachable"),
			expected: true,
		},
		{
			name:     "not a network error",
			error:    fmt.Errorf("validation failed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNetworkError(tt.error)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimeoutErrorDetection(t *testing.T) {
	tests := []struct {
		name     string
		error    error
		expected bool
	}{
		{
			name:     "context deadline exceeded",
			error:    fmt.Errorf("context deadline exceeded"),
			expected: true,
		},
		{
			name:     "timeout",
			error:    fmt.Errorf("request timeout"),
			expected: true,
		},
		{
			name:     "not a timeout error",
			error:    fmt.Errorf("validation failed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTimeoutError(tt.error)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRetryMechanism(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	
	// Create configuration with short delays for testing
	cfg := config.DefaultConfig().ErrorHandling
	cfg.GlobalRetryPolicy.MaxAttempts = 3
	cfg.GlobalRetryPolicy.InitialDelay = "10ms"
	cfg.GlobalRetryPolicy.MaxDelay = "50ms"
	cfg.GlobalRetryPolicy.BackoffStrategy = "fixed"
	
	classifier := NewErrorClassifier(cfg, logger)

	t.Run("successful retry after failures", func(t *testing.T) {
		attempts := 0
		fn := func() error {
			attempts++
			if attempts < 3 {
				return fmt.Errorf("temporary failure")
			}
			return nil
		}

		ctx := context.Background()
		err := classifier.Retry(ctx, "test", "operation", fn)
		
		assert.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("non-retryable error fails immediately", func(t *testing.T) {
		attempts := 0
		fn := func() error {
			attempts++
			return fmt.Errorf("unauthorized: invalid credentials")
		}

		ctx := context.Background()
		err := classifier.Retry(ctx, "test", "operation", fn)
		
		assert.Error(t, err)
		assert.Equal(t, 1, attempts)
		
		classified := classifier.Classify(fmt.Errorf("unauthorized: invalid credentials"), "test", "operation")
		assert.False(t, classified.Retryable)
	})

	t.Run("max attempts exceeded", func(t *testing.T) {
		attempts := 0
		fn := func() error {
			attempts++
			return fmt.Errorf("temporary failure")
		}

		ctx := context.Background()
		err := classifier.Retry(ctx, "test", "operation", fn)
		
		assert.Error(t, err)
		assert.Equal(t, 3, attempts) // max attempts from config
		assert.Contains(t, err.Error(), "operation failed after 3 attempts")
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		fn := func() error {
			return fmt.Errorf("temporary failure")
		}

		err := classifier.Retry(ctx, "test", "operation", fn)
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestBackoffStrategies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	
	t.Run("exponential backoff", func(t *testing.T) {
		cfg := config.DefaultConfig().ErrorHandling
		cfg.GlobalRetryPolicy.BackoffStrategy = "exponential"
		cfg.GlobalRetryPolicy.InitialDelay = "100ms"
		cfg.GlobalRetryPolicy.MaxDelay = "1s"
		cfg.GlobalRetryPolicy.Jitter = false
		
		classifier := NewErrorClassifier(cfg, logger)
		backoffStrategy := classifier.createBackoffStrategy(cfg.GlobalRetryPolicy)
		
		first := backoffStrategy.NextBackOff()
		second := backoffStrategy.NextBackOff()
		
		assert.True(t, second >= first, "Exponential backoff should increase")
		assert.True(t, first >= 100*time.Millisecond, "First delay should be at least initial delay")
	})

	t.Run("linear backoff", func(t *testing.T) {
		cfg := config.DefaultConfig().ErrorHandling
		cfg.GlobalRetryPolicy.BackoffStrategy = "linear"
		cfg.GlobalRetryPolicy.InitialDelay = "100ms"
		cfg.GlobalRetryPolicy.MaxDelay = "1s"
		
		classifier := NewErrorClassifier(cfg, logger)
		backoffStrategy := classifier.createBackoffStrategy(cfg.GlobalRetryPolicy)
		
		first := backoffStrategy.NextBackOff()
		second := backoffStrategy.NextBackOff()
		third := backoffStrategy.NextBackOff()
		
		assert.Equal(t, 100*time.Millisecond, first)
		assert.Equal(t, 200*time.Millisecond, second)
		assert.Equal(t, 300*time.Millisecond, third)
	})

	t.Run("fixed backoff", func(t *testing.T) {
		cfg := config.DefaultConfig().ErrorHandling
		cfg.GlobalRetryPolicy.BackoffStrategy = "fixed"
		cfg.GlobalRetryPolicy.InitialDelay = "200ms"
		
		classifier := NewErrorClassifier(cfg, logger)
		backoffStrategy := classifier.createBackoffStrategy(cfg.GlobalRetryPolicy)
		
		first := backoffStrategy.NextBackOff()
		second := backoffStrategy.NextBackOff()
		
		assert.Equal(t, first, second, "Fixed backoff should remain constant")
		assert.Equal(t, 200*time.Millisecond, first)
	})
}

func TestLinearBackoff(t *testing.T) {
	lb := &LinearBackoff{
		interval: 100 * time.Millisecond,
		max:      500 * time.Millisecond,
	}

	// Test incremental behavior
	first := lb.NextBackOff()
	second := lb.NextBackOff()
	third := lb.NextBackOff()

	assert.Equal(t, 100*time.Millisecond, first)
	assert.Equal(t, 200*time.Millisecond, second)
	assert.Equal(t, 300*time.Millisecond, third)

	// Test max limit
	for i := 0; i < 10; i++ {
		delay := lb.NextBackOff()
		assert.True(t, delay <= 500*time.Millisecond, "Should not exceed max delay")
	}

	// Test reset
	lb.Reset()
	resetFirst := lb.NextBackOff()
	assert.Equal(t, 100*time.Millisecond, resetFirst)
}

func TestJitteredBackoff(t *testing.T) {
	// Create a fixed backoff for testing
	fixed := &LinearBackoff{
		interval: 100 * time.Millisecond,
		max:      100 * time.Millisecond,
	}

	jb := &JitteredBackoff{BackOff: fixed}
	
	// Test that jitter adds variance
	delays := make([]time.Duration, 10)
	for i := range delays {
		jb.Reset()
		fixed.Reset()
		delays[i] = jb.NextBackOff()
	}

	// Check that we have some variance (not all delays are exactly the same)
	allSame := true
	for i := 1; i < len(delays); i++ {
		if delays[i] != delays[0] {
			allSame = false
			break
		}
	}
	assert.False(t, allSame, "Jittered backoff should produce varying delays")

	// Check that all delays are within reasonable range (90ms - 110ms for 100ms base)
	for _, delay := range delays {
		assert.True(t, delay >= 90*time.Millisecond && delay <= 110*time.Millisecond,
			"Jittered delay should be within 10%% of base delay, got %v", delay)
	}
}

func TestCircuitBreaker(t *testing.T) {
	config := config.CircuitBreakerConfig{
		FailureThreshold: 3,
		RecoveryTimeout:  "100ms",
		HalfOpenRequests: 2,
	}

	cb := NewCircuitBreaker("test_circuit", config)

	t.Run("closed state allows requests", func(t *testing.T) {
		assert.Equal(t, CircuitClosed, cb.GetState())
		
		err := cb.Call(func() error {
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("opens after failure threshold", func(t *testing.T) {
		// Cause failures to open the circuit
		for i := 0; i < 3; i++ {
			cb.Call(func() error {
				return fmt.Errorf("failure %d", i)
			})
		}
		
		assert.Equal(t, CircuitOpen, cb.GetState())
		
		// Requests should be rejected
		err := cb.Call(func() error {
			return nil
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "circuit breaker is open")
	})

	t.Run("transitions to half-open after timeout", func(t *testing.T) {
		// Wait for recovery timeout
		time.Sleep(150 * time.Millisecond)
		
		// First request should be allowed (moves to half-open)
		err := cb.Call(func() error {
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("closes after successful half-open requests", func(t *testing.T) {
		// Reset circuit breaker for clean test
		cb = NewCircuitBreaker("test_circuit_2", config)
		
		// Open the circuit
		for i := 0; i < 3; i++ {
			cb.Call(func() error {
				return fmt.Errorf("failure")
			})
		}
		assert.Equal(t, CircuitOpen, cb.GetState())
		
		// Wait for recovery
		time.Sleep(150 * time.Millisecond)
		
		// Make successful requests to close the circuit
		for i := 0; i < 2; i++ {
			err := cb.Call(func() error {
				return nil
			})
			assert.NoError(t, err)
		}
		
		// Circuit should be closed now
		assert.Equal(t, CircuitClosed, cb.GetState())
	})
}

func TestClassifiedErrorInterface(t *testing.T) {
	originalErr := fmt.Errorf("original error")
	classified := &ClassifiedError{
		Err:       originalErr,
		Type:      ErrorTypeNetwork,
		Severity:  SeverityLow,
		Component: "test",
		Operation: "test_op",
		Timestamp: time.Now(),
	}

	t.Run("error interface", func(t *testing.T) {
		errStr := classified.Error()
		assert.Contains(t, errStr, "test/network")
		assert.Contains(t, errStr, "test_op")
		assert.Contains(t, errStr, "original error")
	})

	t.Run("unwrap interface", func(t *testing.T) {
		unwrapped := classified.Unwrap()
		assert.Equal(t, originalErr, unwrapped)
	})

	t.Run("is interface", func(t *testing.T) {
		other := &ClassifiedError{Type: ErrorTypeNetwork}
		assert.True(t, classified.Is(other))
		
		different := &ClassifiedError{Type: ErrorTypeTimeout}
		assert.False(t, classified.Is(different))
	})
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("WrapError", func(t *testing.T) {
		original := fmt.Errorf("original error")
		wrapped := WrapError(original, "component", "operation", "something failed")
		
		assert.Contains(t, wrapped.Error(), "something failed")
		assert.Contains(t, wrapped.Error(), "component.operation")
		assert.Contains(t, wrapped.Error(), "original error")
	})

	t.Run("IsRetryable", func(t *testing.T) {
		retryable := &ClassifiedError{Retryable: true}
		notRetryable := &ClassifiedError{Retryable: false}
		regular := fmt.Errorf("regular error")
		
		assert.True(t, IsRetryable(retryable))
		assert.False(t, IsRetryable(notRetryable))
		assert.False(t, IsRetryable(regular))
	})

	t.Run("GetErrorType", func(t *testing.T) {
		classified := &ClassifiedError{Type: ErrorTypeNetwork}
		regular := fmt.Errorf("regular error")
		
		assert.Equal(t, ErrorTypeNetwork, GetErrorType(classified))
		assert.Equal(t, ErrorTypeUnknown, GetErrorType(regular))
	})

	t.Run("GetSeverity", func(t *testing.T) {
		classified := &ClassifiedError{Severity: SeverityCritical}
		regular := fmt.Errorf("regular error")
		
		assert.Equal(t, SeverityCritical, GetSeverity(classified))
		assert.Equal(t, SeverityMedium, GetSeverity(regular))
	})
}

func TestErrorStats(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	classifier := NewErrorClassifier(config.DefaultConfig().ErrorHandling, logger)

	// Generate some errors to populate stats
	errors := []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("timeout"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("rate limit exceeded"),
	}

	for _, err := range errors {
		classifier.Classify(err, "test", "op")
	}

	stats := classifier.GetStats()
	
	// Check that we have stats for the error types we generated
	assert.Contains(t, stats, ErrorTypeNetwork)
	assert.Contains(t, stats, ErrorTypeTimeout)
	assert.Contains(t, stats, ErrorTypeRateLimit)

	// Network errors should have count of 2
	networkStats := stats[ErrorTypeNetwork]
	assert.Equal(t, int64(2), networkStats.Count)
	assert.False(t, networkStats.FirstSeen.IsZero())
	assert.False(t, networkStats.LastSeen.IsZero())
}

// Mock net.Error for testing
type mockNetError struct {
	msg       string
	timeout   bool
	temporary bool
}

func (e mockNetError) Error() string   { return e.msg }
func (e mockNetError) Timeout() bool   { return e.timeout }
func (e mockNetError) Temporary() bool { return e.temporary }

func TestNetErrorInterface(t *testing.T) {
	timeoutErr := mockNetError{msg: "timeout", timeout: true}
	tempErr := mockNetError{msg: "temporary", temporary: true}
	netErr := mockNetError{msg: "network error"}

	assert.True(t, isNetworkError(timeoutErr))
	assert.True(t, isNetworkError(tempErr))
	assert.True(t, isNetworkError(netErr))

	assert.True(t, isTimeoutError(timeoutErr))
	assert.False(t, isTimeoutError(tempErr))
	assert.False(t, isTimeoutError(netErr))
}

func TestComponentSpecificRetryPolicy(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	
	cfg := config.DefaultConfig().ErrorHandling
	cfg.GlobalRetryPolicy.MaxAttempts = 3
	
	// Add component-specific policy
	cfg.ComponentPolicies = map[string]config.RetryPolicyConfig{
		"special_component": {
			MaxAttempts:     5,
			InitialDelay:    "10ms",
			MaxDelay:        "50ms",
			BackoffStrategy: "fixed",
			RetryableErrors: []string{"network", "timeout"},
		},
	}
	
	classifier := NewErrorClassifier(cfg, logger)

	t.Run("uses component-specific policy", func(t *testing.T) {
		policy := classifier.getRetryPolicy("special_component")
		assert.Equal(t, 5, policy.MaxAttempts)
		assert.Equal(t, "10ms", policy.InitialDelay)
	})

	t.Run("falls back to global policy", func(t *testing.T) {
		policy := classifier.getRetryPolicy("regular_component")
		assert.Equal(t, 3, policy.MaxAttempts)
	})
}