package models

import (
	"errors"
	"fmt"
	"time"
)

// GapStatus represents the possible states of a data gap in the system.
// It tracks the lifecycle of gap detection, filling, and resolution.
type GapStatus string

const (
	// GapStatusDetected indicates a gap has been identified but no action taken yet
	GapStatusDetected GapStatus = "detected"
	// GapStatusFilling indicates the gap is currently being filled
	GapStatusFilling GapStatus = "filling"
	// GapStatusFilled indicates the gap has been successfully filled
	GapStatusFilled GapStatus = "filled"
	// GapStatusPermanent indicates the gap cannot be filled (e.g., data not available)
	GapStatusPermanent GapStatus = "permanent"
)

// GapPriority represents the urgency level for filling a gap.
// Higher priority gaps should be processed before lower priority ones.
type GapPriority int

const (
	PriorityLow      GapPriority = iota // PriorityLow indicates gaps that can be filled when resources are available
	PriorityMedium                      // PriorityMedium indicates normal priority gaps
	PriorityHigh                        // PriorityHigh indicates gaps that should be filled promptly
	PriorityCritical                    // PriorityCritical indicates gaps that must be filled immediately
)

// Gap represents missing periods in historical OHLCV data that need to be backfilled.
// It contains metadata about the gap including timing, priority, and fill attempts.
type Gap struct {
	// ID is the unique gap identifier
	ID string `json:"id" db:"id" validate:"required"`

	// Pair is the trading pair symbol (e.g., "BTC-USD")
	Pair string `json:"pair" db:"pair" validate:"required"`

	// StartTime is the gap start timestamp in UTC
	StartTime time.Time `json:"start_time" db:"start_time" validate:"required"`

	// EndTime is the gap end timestamp in UTC
	EndTime time.Time `json:"end_time" db:"end_time" validate:"required"`

	// Interval is the missing data interval (e.g., "1h", "1d")
	Interval string `json:"interval" db:"interval" validate:"required"`

	// Status is the current gap status
	Status GapStatus `json:"status" db:"status" validate:"required,oneof=detected filling filled permanent"`

	// CreatedAt is when the gap was first detected
	CreatedAt time.Time `json:"created_at" db:"created_at"`

	// FilledAt is when the gap was resolved (nil if not yet filled)
	FilledAt *time.Time `json:"filled_at,omitempty" db:"filled_at"`

	// Priority is the calculated priority for filling this gap
	Priority GapPriority `json:"priority" db:"priority"`

	// Attempts tracks how many times we've tried to fill this gap
	Attempts int `json:"attempts" db:"attempts"`

	// LastAttemptAt tracks the last time we attempted to fill this gap
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty" db:"last_attempt_at"`

	// ErrorMessage contains the last error encountered when trying to fill the gap
	ErrorMessage string `json:"error_message,omitempty" db:"error_message"`
}

// NewGap creates a new Gap instance with the provided parameters and sets the status to detected.
// It validates the gap data, calculates priority, and sets the creation timestamp.
// Returns an error if the gap parameters are invalid.
//
// Example:
//
//	gap, err := NewGap("gap-123", "BTC-USD", start, end, "1h")
func NewGap(id, pair string, startTime, endTime time.Time, interval string) (*Gap, error) {
	gap := &Gap{
		ID:        id,
		Pair:      pair,
		StartTime: startTime,
		EndTime:   endTime,
		Interval:  interval,
		Status:    GapStatusDetected,
		CreatedAt: time.Now().UTC(),
		Attempts:  0,
	}

	if err := gap.Validate(); err != nil {
		return nil, fmt.Errorf("invalid gap: %w", err)
	}

	gap.calculatePriority()
	return gap, nil
}

// Validate performs comprehensive validation on the gap data.
// It checks that all required fields are present, times are valid,
// status transitions are correct, and timestamps are consistent.
// Returns an error if any validation fails.
func (g *Gap) Validate() error {
	if g.ID == "" {
		return errors.New("gap ID cannot be empty")
	}

	if g.Pair == "" {
		return errors.New("gap pair cannot be empty")
	}

	if g.Interval == "" {
		return errors.New("gap interval cannot be empty")
	}

	if g.StartTime.IsZero() {
		return errors.New("gap start time cannot be zero")
	}

	if g.EndTime.IsZero() {
		return errors.New("gap end time cannot be zero")
	}

	if !g.EndTime.After(g.StartTime) {
		return errors.New("gap end time must be after start time")
	}

	// Validate status is one of the allowed values
	switch g.Status {
	case GapStatusDetected, GapStatusFilling, GapStatusFilled, GapStatusPermanent:
		// Valid status
	default:
		return fmt.Errorf("invalid gap status: %s", g.Status)
	}

	// If status is filled, FilledAt should be set
	if g.Status == GapStatusFilled && g.FilledAt == nil {
		return errors.New("filled gaps must have a filled_at timestamp")
	}

	// If status is not filled, FilledAt should not be set
	if g.Status != GapStatusFilled && g.FilledAt != nil {
		return errors.New("only filled gaps can have a filled_at timestamp")
	}

	return nil
}

// StartFilling transitions the gap from detected to filling status.
// It updates the attempt counter, sets the last attempt time, and clears any previous error.
// Returns an error if the gap is not in detected status.
func (g *Gap) StartFilling() error {
	if g.Status != GapStatusDetected {
		return fmt.Errorf("cannot start filling gap with status %s, must be %s", g.Status, GapStatusDetected)
	}

	g.Status = GapStatusFilling
	now := time.Now().UTC()
	g.LastAttemptAt = &now
	g.Attempts++
	g.ErrorMessage = "" // Clear any previous error

	return nil
}

// MarkFilled transitions the gap from filling to filled status.
// It sets the filled timestamp and clears any error message.
// Returns an error if the gap is not currently being filled.
func (g *Gap) MarkFilled() error {
	if g.Status != GapStatusFilling {
		return fmt.Errorf("cannot mark gap as filled with status %s, must be %s", g.Status, GapStatusFilling)
	}

	g.Status = GapStatusFilled
	now := time.Now().UTC()
	g.FilledAt = &now
	g.ErrorMessage = "" // Clear any error message

	return nil
}

// MarkPermanent transitions the gap from detected to permanent status.
// This should be used when a gap cannot be filled due to data unavailability.
// The reason parameter should explain why the gap is unfillable.
// Returns an error if the gap is not in detected status.
func (g *Gap) MarkPermanent(reason string) error {
	if g.Status != GapStatusDetected {
		return fmt.Errorf("cannot mark gap as permanent with status %s, must be %s", g.Status, GapStatusDetected)
	}

	g.Status = GapStatusPermanent
	g.ErrorMessage = reason

	return nil
}

// RecordFailure records a failed attempt to fill the gap and transitions back to detected status.
// It preserves the attempt count and timestamp for retry logic.
// The errorMessage parameter should contain details about why the fill attempt failed.
// Returns an error if the gap is not currently being filled.
func (g *Gap) RecordFailure(errorMessage string) error {
	if g.Status != GapStatusFilling {
		return fmt.Errorf("cannot record failure for gap with status %s, must be %s", g.Status, GapStatusFilling)
	}

	g.Status = GapStatusDetected
	g.ErrorMessage = errorMessage
	// LastAttemptAt and Attempts are already updated in StartFilling

	return nil
}

// Duration calculates the time span of the gap from start to end time.
// This helps determine the amount of data that needs to be backfilled.
func (g *Gap) Duration() time.Duration {
	return g.EndTime.Sub(g.StartTime)
}

// IsActive returns true if the gap is in detected or filling status.
// Active gaps are candidates for backfill processing.
func (g *Gap) IsActive() bool {
	return g.Status == GapStatusDetected || g.Status == GapStatusFilling
}

// CanFill returns true if the gap is in detected status and can be transitioned to filling.
// This is used by the gap processing system to identify fillable gaps.
func (g *Gap) CanFill() bool {
	return g.Status == GapStatusDetected
}

// IsFilled returns true if the gap has been successfully filled with data.
// Filled gaps do not require further processing.
func (g *Gap) IsFilled() bool {
	return g.Status == GapStatusFilled
}

// IsPermanent returns true if the gap is marked as permanently unfillable.
// Permanent gaps are excluded from backfill processing.
func (g *Gap) IsPermanent() bool {
	return g.Status == GapStatusPermanent
}

// IsInProgress returns true if the gap is currently being filled.
// Only one worker should fill a gap at a time.
func (g *Gap) IsInProgress() bool {
	return g.Status == GapStatusFilling
}

// Age returns the duration since the gap was first detected.
// This is used in priority calculations and monitoring.
func (g *Gap) Age() time.Duration {
	return time.Since(g.CreatedAt)
}

// TimeSinceLastAttempt returns the duration since the last fill attempt was made.
// Returns 0 if no attempts have been made yet.
// This is used to implement retry delays.
func (g *Gap) TimeSinceLastAttempt() time.Duration {
	if g.LastAttemptAt == nil {
		return time.Duration(0)
	}
	return time.Since(*g.LastAttemptAt)
}

// ShouldRetry determines if a failed gap should be retried based on attempt count and time.
// It checks that the gap is in detected status, hasn't exceeded max attempts,
// and sufficient time has passed since the last attempt.
// Returns true if the gap should be retried.
func (g *Gap) ShouldRetry(maxAttempts int, retryDelay time.Duration) bool {
	if g.Status != GapStatusDetected {
		return false
	}

	if g.Attempts >= maxAttempts {
		return false
	}

	if g.LastAttemptAt == nil {
		return true
	}

	return g.TimeSinceLastAttempt() >= retryDelay
}

// calculatePriority determines the priority for filling this gap based on duration and age.
// Longer gaps and older gaps receive higher priority.
// Very recent short gaps receive lower priority.
func (g *Gap) calculatePriority() {
	duration := g.Duration()
	age := g.Age()

	// Start with medium priority
	priority := PriorityMedium

	// Increase priority for longer gaps
	if duration > 24*time.Hour {
		priority = PriorityHigh
	}
	if duration > 7*24*time.Hour {
		priority = PriorityCritical
	}

	// Increase priority for older gaps
	if age > 24*time.Hour {
		if priority < PriorityHigh {
			priority = PriorityHigh
		}
	}
	if age > 7*24*time.Hour {
		priority = PriorityCritical
	}

	// Recent gaps (< 1 hour) get lower priority unless they're very long
	if age < time.Hour && duration < time.Hour {
		priority = PriorityLow
	}

	g.Priority = priority
}

// UpdatePriority recalculates and updates the gap priority based on current time.
// This should be called periodically to ensure priorities reflect current conditions.
func (g *Gap) UpdatePriority() {
	g.calculatePriority()
}

// GetPriorityString returns a human-readable string representation of the gap priority.
// This is useful for logging and monitoring purposes.
func (g *Gap) GetPriorityString() string {
	switch g.Priority {
	case PriorityLow:
		return "low"
	case PriorityMedium:
		return "medium"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// ExpectedCandles estimates the number of candles needed to fill this gap.
// It calculates based on the gap duration and interval.
// Returns an error for unsupported interval formats.
// This helps estimate the amount of work required to fill the gap.
func (g *Gap) ExpectedCandles() (int, error) {
	duration := g.Duration()

	// Parse interval to get time duration
	var intervalDuration time.Duration
	switch g.Interval {
	case "1m":
		intervalDuration = time.Minute
	case "5m":
		intervalDuration = 5 * time.Minute
	case "15m":
		intervalDuration = 15 * time.Minute
	case "1h":
		intervalDuration = time.Hour
	case "4h":
		intervalDuration = 4 * time.Hour
	case "1d":
		intervalDuration = 24 * time.Hour
	case "1w":
		intervalDuration = 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported interval: %s", g.Interval)
	}

	expectedCandles := int(duration / intervalDuration)
	if expectedCandles == 0 {
		expectedCandles = 1 // Minimum one candle
	}

	return expectedCandles, nil
}

// String returns a human-readable representation of the gap.
// It includes key identifying information and current status.
// This method implements the fmt.Stringer interface.
func (g *Gap) String() string {
	return fmt.Sprintf("Gap{ID: %s, Pair: %s, Duration: %v, Status: %s, Priority: %s}",
		g.ID, g.Pair, g.Duration(), g.Status, g.GetPriorityString())
}
