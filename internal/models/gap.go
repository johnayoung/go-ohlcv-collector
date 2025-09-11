package models

import (
	"errors"
	"fmt"
	"time"
)

// GapStatus represents the possible states of a data gap
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

// GapPriority represents the urgency level for filling a gap
type GapPriority int

const (
	PriorityLow GapPriority = iota
	PriorityMedium
	PriorityHigh
	PriorityCritical
)

// Gap represents missing periods in historical OHLCV data
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

// NewGap creates a new gap with the detected status
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

// Validate checks if the gap has valid field values
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

// StartFilling transitions the gap from detected to filling status
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

// MarkFilled transitions the gap from filling to filled status
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

// MarkPermanent transitions the gap from detected to permanent status
func (g *Gap) MarkPermanent(reason string) error {
	if g.Status != GapStatusDetected {
		return fmt.Errorf("cannot mark gap as permanent with status %s, must be %s", g.Status, GapStatusDetected)
	}
	
	g.Status = GapStatusPermanent
	g.ErrorMessage = reason
	
	return nil
}

// RecordFailure records a failed attempt to fill the gap and transitions back to detected
func (g *Gap) RecordFailure(errorMessage string) error {
	if g.Status != GapStatusFilling {
		return fmt.Errorf("cannot record failure for gap with status %s, must be %s", g.Status, GapStatusFilling)
	}
	
	g.Status = GapStatusDetected
	g.ErrorMessage = errorMessage
	// LastAttemptAt and Attempts are already updated in StartFilling
	
	return nil
}

// Duration calculates the time span of the gap
func (g *Gap) Duration() time.Duration {
	return g.EndTime.Sub(g.StartTime)
}

// IsActive returns true if the gap is not filled or permanent
func (g *Gap) IsActive() bool {
	return g.Status == GapStatusDetected || g.Status == GapStatusFilling
}

// CanFill returns true if the gap can be transitioned to filling status
func (g *Gap) CanFill() bool {
	return g.Status == GapStatusDetected
}

// IsFilled returns true if the gap has been successfully filled
func (g *Gap) IsFilled() bool {
	return g.Status == GapStatusFilled
}

// IsPermanent returns true if the gap is marked as permanently unfillable
func (g *Gap) IsPermanent() bool {
	return g.Status == GapStatusPermanent
}

// IsInProgress returns true if the gap is currently being filled
func (g *Gap) IsInProgress() bool {
	return g.Status == GapStatusFilling
}

// Age returns how long ago the gap was created
func (g *Gap) Age() time.Duration {
	return time.Since(g.CreatedAt)
}

// TimeSinceLastAttempt returns how long since the last fill attempt
func (g *Gap) TimeSinceLastAttempt() time.Duration {
	if g.LastAttemptAt == nil {
		return time.Duration(0)
	}
	return time.Since(*g.LastAttemptAt)
}

// ShouldRetry determines if a failed gap should be retried based on attempts and time
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

// calculatePriority determines the priority for filling this gap based on various factors
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

// UpdatePriority recalculates and updates the gap priority
func (g *Gap) UpdatePriority() {
	g.calculatePriority()
}

// GetPriorityString returns a human-readable priority string
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

// ExpectedCandles estimates how many candles should fill this gap
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

// String returns a human-readable representation of the gap
func (g *Gap) String() string {
	return fmt.Sprintf("Gap{ID: %s, Pair: %s, Duration: %v, Status: %s, Priority: %s}",
		g.ID, g.Pair, g.Duration(), g.Status, g.GetPriorityString())
}