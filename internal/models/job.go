package models

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// JobStatus represents the current state of a collection job
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

// JobType represents the type of data collection task
type JobType string

const (
	JobTypeInitialSync JobType = "initial_sync"
	JobTypeUpdate      JobType = "update"
	JobTypeBackfill    JobType = "backfill"
)

// MaxRetryAttempts defines the maximum number of retry attempts
const MaxRetryAttempts = 5

// BackoffMultiplier for exponential backoff calculation
const BackoffMultiplier = 2.0

// Job represents a data collection task with status tracking
type Job struct {
	ID               string    `json:"id" db:"id"`
	Type             JobType   `json:"type" db:"type"`
	Pair             string    `json:"pair" db:"pair"`
	StartTime        time.Time `json:"start_time" db:"start_time"`
	EndTime          time.Time `json:"end_time" db:"end_time"`
	Interval         string    `json:"interval" db:"interval"`
	Status           JobStatus `json:"status" db:"status"`
	Progress         int       `json:"progress" db:"progress"`
	RecordsCollected int       `json:"records_collected" db:"records_collected"`
	Error            string    `json:"error,omitempty" db:"error"`
	RetryCount       int       `json:"retry_count" db:"retry_count"`
	CreatedAt        time.Time `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time `json:"updated_at" db:"updated_at"`
}

// JobError represents validation and operational errors
type JobError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e JobError) Error() string {
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// NewJob creates a new Job with default values
func NewJob(id string, jobType JobType, pair string, startTime, endTime time.Time, interval string) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:               id,
		Type:             jobType,
		Pair:             pair,
		StartTime:        startTime,
		EndTime:          endTime,
		Interval:         interval,
		Status:           StatusPending,
		Progress:         0,
		RecordsCollected: 0,
		Error:            "",
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

// Validate performs comprehensive validation of the Job
func (j *Job) Validate() error {
	var errors []JobError

	// Required fields validation
	if j.ID == "" {
		errors = append(errors, JobError{Field: "ID", Message: "job ID is required"})
	}

	if j.Pair == "" {
		errors = append(errors, JobError{Field: "Pair", Message: "trading pair is required"})
	}

	if j.Interval == "" {
		errors = append(errors, JobError{Field: "Interval", Message: "interval is required"})
	}

	// Job type validation
	if !j.isValidJobType() {
		errors = append(errors, JobError{
			Field:   "Type",
			Message: fmt.Sprintf("invalid job type '%s', must be one of: %s, %s, %s", 
				j.Type, JobTypeInitialSync, JobTypeUpdate, JobTypeBackfill),
		})
	}

	// Status validation
	if !j.isValidStatus() {
		errors = append(errors, JobError{
			Field:   "Status",
			Message: fmt.Sprintf("invalid status '%s', must be one of: %s, %s, %s, %s", 
				j.Status, StatusPending, StatusRunning, StatusCompleted, StatusFailed),
		})
	}

	// Time validation
	if j.StartTime.IsZero() {
		errors = append(errors, JobError{Field: "StartTime", Message: "start time is required"})
	}

	if j.EndTime.IsZero() {
		errors = append(errors, JobError{Field: "EndTime", Message: "end time is required"})
	}

	if !j.StartTime.IsZero() && !j.EndTime.IsZero() && j.StartTime.After(j.EndTime) {
		errors = append(errors, JobError{
			Field:   "StartTime",
			Message: "start time must be before end time",
		})
	}

	// Progress validation
	if j.Progress < 0 || j.Progress > 100 {
		errors = append(errors, JobError{
			Field:   "Progress",
			Message: "progress must be between 0 and 100",
		})
	}

	// Records collected validation
	if j.RecordsCollected < 0 {
		errors = append(errors, JobError{
			Field:   "RecordsCollected",
			Message: "records collected cannot be negative",
		})
	}

	// Retry count validation
	if j.RetryCount < 0 {
		errors = append(errors, JobError{
			Field:   "RetryCount",
			Message: "retry count cannot be negative",
		})
	}

	// Interval format validation
	if !j.isValidInterval() {
		errors = append(errors, JobError{
			Field:   "Interval",
			Message: fmt.Sprintf("invalid interval format '%s', expected formats like '1m', '5m', '1h', '1d'", j.Interval),
		})
	}

	if len(errors) > 0 {
		return fmt.Errorf("validation failed with %d errors: %v", len(errors), errors)
	}

	return nil
}

// isValidJobType checks if the job type is valid
func (j *Job) isValidJobType() bool {
	switch j.Type {
	case JobTypeInitialSync, JobTypeUpdate, JobTypeBackfill:
		return true
	default:
		return false
	}
}

// isValidStatus checks if the status is valid
func (j *Job) isValidStatus() bool {
	switch j.Status {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed:
		return true
	default:
		return false
	}
}

// isValidInterval checks if the interval format is valid
func (j *Job) isValidInterval() bool {
	validIntervals := map[string]bool{
		"1m": true, "5m": true, "15m": true, "30m": true,
		"1h": true, "2h": true, "4h": true, "6h": true, "12h": true,
		"1d": true, "3d": true, "1w": true, "1M": true,
	}
	return validIntervals[j.Interval]
}

// State Transition Methods

// Start transitions the job from pending to running
func (j *Job) Start() error {
	if j.Status != StatusPending {
		return fmt.Errorf("cannot start job: current status is %s, expected %s", j.Status, StatusPending)
	}

	j.Status = StatusRunning
	j.UpdatedAt = time.Now().UTC()
	j.Error = "" // Clear any previous error
	return nil
}

// Complete transitions the job from running to completed
func (j *Job) Complete() error {
	if j.Status != StatusRunning {
		return fmt.Errorf("cannot complete job: current status is %s, expected %s", j.Status, StatusRunning)
	}

	j.Status = StatusCompleted
	j.Progress = 100
	j.UpdatedAt = time.Now().UTC()
	j.Error = ""
	return nil
}

// Fail transitions the job from running to failed
func (j *Job) Fail(errorMsg string) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("cannot fail job: current status is %s, expected %s", j.Status, StatusRunning)
	}

	j.Status = StatusFailed
	j.Error = errorMsg
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// Retry transitions the job from failed back to pending for retry
func (j *Job) Retry() error {
	if j.Status != StatusFailed {
		return fmt.Errorf("cannot retry job: current status is %s, expected %s", j.Status, StatusFailed)
	}

	if !j.CanRetry() {
		return fmt.Errorf("cannot retry job: maximum retry attempts (%d) exceeded", MaxRetryAttempts)
	}

	j.Status = StatusPending
	j.RetryCount++
	j.UpdatedAt = time.Now().UTC()
	// Keep the error for debugging purposes
	return nil
}

// Progress Tracking and Metrics Methods

// UpdateProgress updates the job's progress percentage and records collected
func (j *Job) UpdateProgress(progress int, recordsCollected int) error {
	if progress < 0 || progress > 100 {
		return fmt.Errorf("invalid progress value: %d, must be between 0 and 100", progress)
	}

	if recordsCollected < 0 {
		return fmt.Errorf("invalid records collected: %d, cannot be negative", recordsCollected)
	}

	j.Progress = progress
	j.RecordsCollected = recordsCollected
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// IncrementRecordsCollected adds to the current count of records collected
func (j *Job) IncrementRecordsCollected(count int) error {
	if count < 0 {
		return fmt.Errorf("cannot increment by negative value: %d", count)
	}

	j.RecordsCollected += count
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// CalculateProgressFromTimeRange calculates progress based on time coverage
func (j *Job) CalculateProgressFromTimeRange(currentTime time.Time) int {
	if j.StartTime.IsZero() || j.EndTime.IsZero() || currentTime.Before(j.StartTime) {
		return 0
	}

	if currentTime.After(j.EndTime) {
		return 100
	}

	totalDuration := j.EndTime.Sub(j.StartTime)
	completedDuration := currentTime.Sub(j.StartTime)

	if totalDuration <= 0 {
		return 100
	}

	progress := int((completedDuration.Nanoseconds() * 100) / totalDuration.Nanoseconds())
	if progress > 100 {
		progress = 100
	}
	if progress < 0 {
		progress = 0
	}

	return progress
}

// Helper Methods for Job Management

// IsComplete returns true if the job is completed
func (j *Job) IsComplete() bool {
	return j.Status == StatusCompleted
}

// IsFailed returns true if the job has failed
func (j *Job) IsFailed() bool {
	return j.Status == StatusFailed
}

// IsRunning returns true if the job is currently running
func (j *Job) IsRunning() bool {
	return j.Status == StatusRunning
}

// IsPending returns true if the job is pending
func (j *Job) IsPending() bool {
	return j.Status == StatusPending
}

// CanRetry returns true if the job can be retried
func (j *Job) CanRetry() bool {
	return j.Status == StatusFailed && j.RetryCount < MaxRetryAttempts
}

// GetNextRetryDelay calculates the delay for the next retry using exponential backoff
func (j *Job) GetNextRetryDelay() time.Duration {
	if j.RetryCount == 0 {
		return time.Minute // Base delay of 1 minute
	}

	// Exponential backoff: base_delay * (multiplier ^ retry_count)
	baseDelaySeconds := 60 // 1 minute in seconds
	delaySeconds := float64(baseDelaySeconds) * math.Pow(BackoffMultiplier, float64(j.RetryCount))
	
	// Cap at 30 minutes
	maxDelaySeconds := float64(30 * 60)
	if delaySeconds > maxDelaySeconds {
		delaySeconds = maxDelaySeconds
	}

	return time.Duration(delaySeconds) * time.Second
}

// GetEstimatedDuration returns the estimated total duration of the job
func (j *Job) GetEstimatedDuration() time.Duration {
	if j.StartTime.IsZero() || j.EndTime.IsZero() {
		return 0
	}
	return j.EndTime.Sub(j.StartTime)
}

// GetElapsedTime returns the time elapsed since the job was created
func (j *Job) GetElapsedTime() time.Duration {
	if j.CreatedAt.IsZero() {
		return 0
	}
	return time.Since(j.CreatedAt)
}

// GetRemainingTime estimates the remaining time based on current progress
func (j *Job) GetRemainingTime() time.Duration {
	if j.Progress == 0 || j.Status != StatusRunning {
		return 0
	}

	elapsedTime := j.GetElapsedTime()
	if elapsedTime <= 0 {
		return 0
	}

	// Estimate total time based on current progress
	estimatedTotalTime := time.Duration(float64(elapsedTime.Nanoseconds()) * (100.0 / float64(j.Progress)))
	remainingTime := estimatedTotalTime - elapsedTime

	if remainingTime < 0 {
		return 0
	}

	return remainingTime
}

// GetRecordsPerSecond calculates the collection rate
func (j *Job) GetRecordsPerSecond() decimal.Decimal {
	elapsedTime := j.GetElapsedTime()
	if elapsedTime <= 0 || j.RecordsCollected == 0 {
		return decimal.Zero
	}

	elapsedSeconds := decimal.NewFromFloat(elapsedTime.Seconds())
	recordsCollected := decimal.NewFromInt(int64(j.RecordsCollected))

	return recordsCollected.Div(elapsedSeconds)
}

// Summary returns a human-readable summary of the job
func (j *Job) Summary() string {
	return fmt.Sprintf("Job %s: %s %s on %s [%s to %s] - Status: %s (%d%% complete, %d records)",
		j.ID,
		j.Type,
		j.Interval,
		j.Pair,
		j.StartTime.Format("2006-01-02 15:04:05"),
		j.EndTime.Format("2006-01-02 15:04:05"),
		j.Status,
		j.Progress,
		j.RecordsCollected,
	)
}

// ToJSON converts the job to JSON string
func (j *Job) ToJSON() (string, error) {
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal job to JSON: %w", err)
	}
	return string(data), nil
}

// FromJSON creates a job from JSON string
func FromJSON(jsonStr string) (*Job, error) {
	var job Job
	if err := json.Unmarshal([]byte(jsonStr), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to job: %w", err)
	}
	
	if err := job.Validate(); err != nil {
		return nil, fmt.Errorf("invalid job data: %w", err)
	}
	
	return &job, nil
}

// Clone creates a deep copy of the job
func (j *Job) Clone() *Job {
	return &Job{
		ID:               j.ID,
		Type:             j.Type,
		Pair:             j.Pair,
		StartTime:        j.StartTime,
		EndTime:          j.EndTime,
		Interval:         j.Interval,
		Status:           j.Status,
		Progress:         j.Progress,
		RecordsCollected: j.RecordsCollected,
		Error:            j.Error,
		RetryCount:       j.RetryCount,
		CreatedAt:        j.CreatedAt,
		UpdatedAt:        j.UpdatedAt,
	}
}