package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// WorkerJob represents a data collection task for the worker pool
type WorkerJob struct {
	Pair     string
	Interval string
	Start    *time.Time // nil for latest data collection
	End      *time.Time // nil for latest data collection
	IsUpdate bool       // true for scheduled updates, false for historical
}

// WorkerPool manages a pool of workers for concurrent data collection
type WorkerPool struct {
	workerCount int
	rateLimiter *rate.Limiter
	logger      *slog.Logger

	// Channels for job distribution
	jobQueue    chan *jobWrapper
	workerQueue chan chan *jobWrapper

	// Worker management
	workers []Worker
	quit    chan bool
	wg      sync.WaitGroup

	// Statistics
	stats       *workerPoolStats
	isStarted   int32
}

// jobWrapper wraps a job with its callback
type jobWrapper struct {
	job      *WorkerJob
	callback func(error)
	ctx      context.Context
}

// Worker represents a single worker in the pool
type Worker struct {
	ID          int
	WorkerQueue chan chan *jobWrapper
	JobChannel  chan *jobWrapper
	Quit        chan bool
	rateLimiter *rate.Limiter
	logger      *slog.Logger
	stats       *workerStats
}

// workerPoolStats tracks worker pool statistics
type workerPoolStats struct {
	activeWorkers   int32
	queuedJobs      int32
	completedJobs   int64
	failedJobs      int64
	totalJobTime    int64 // nanoseconds
	jobCount        int64
	mu              sync.RWMutex
}

// workerStats tracks individual worker statistics
type workerStats struct {
	completedJobs int64
	failedJobs    int64
	totalJobTime  int64 // nanoseconds
	lastJobTime   time.Time
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workerCount int, rateLimiter *rate.Limiter, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		rateLimiter: rateLimiter,
		logger:      logger,
		jobQueue:    make(chan *jobWrapper, workerCount*2),
		workerQueue: make(chan chan *jobWrapper, workerCount),
		quit:        make(chan bool),
		stats:       &workerPoolStats{},
	}
}

// Start initializes and starts all workers
func (wp *WorkerPool) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&wp.isStarted, 0, 1) {
		return fmt.Errorf("worker pool is already started")
	}

	wp.logger.Info("Starting worker pool", "worker_count", wp.workerCount)

	// Create and start workers
	wp.workers = make([]Worker, wp.workerCount)
	for i := 0; i < wp.workerCount; i++ {
		worker := Worker{
			ID:          i + 1,
			WorkerQueue: wp.workerQueue,
			JobChannel:  make(chan *jobWrapper),
			Quit:        make(chan bool),
			rateLimiter: wp.rateLimiter,
			logger:      wp.logger,
			stats:       &workerStats{},
		}

		wp.workers[i] = worker
		wp.wg.Add(1)
		go worker.Start(wp.wg.Done)
		atomic.AddInt32(&wp.stats.activeWorkers, 1)
	}

	// Start the dispatcher
	wp.wg.Add(1)
	go wp.dispatch()

	wp.logger.Info("Worker pool started successfully")
	return nil
}

// Stop gracefully shuts down the worker pool
func (wp *WorkerPool) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&wp.isStarted, 1, 0) {
		return fmt.Errorf("worker pool is not started")
	}

	wp.logger.Info("Stopping worker pool")

	// Signal all workers to quit
	close(wp.quit)

	// Wait for workers to finish or timeout
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		wp.logger.Info("Worker pool stopped successfully")
		return nil
	case <-ctx.Done():
		wp.logger.Warn("Worker pool stop timed out")
		return ctx.Err()
	}
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(ctx context.Context, job *WorkerJob, callback func(error)) {
	atomic.AddInt32(&wp.stats.queuedJobs, 1)

	wrapper := &jobWrapper{
		job:      job,
		callback: callback,
		ctx:      ctx,
	}

	select {
	case wp.jobQueue <- wrapper:
		// Job queued successfully
	case <-ctx.Done():
		// Context cancelled, call callback with error
		atomic.AddInt32(&wp.stats.queuedJobs, -1)
		if callback != nil {
			callback(ctx.Err())
		}
	}
}

// GetStats returns current worker pool statistics
func (wp *WorkerPool) GetStats() *WorkerPoolStats {
	wp.stats.mu.RLock()
	defer wp.stats.mu.RUnlock()

	avgJobDuration := time.Duration(0)
	if wp.stats.jobCount > 0 {
		avgJobDuration = time.Duration(wp.stats.totalJobTime / wp.stats.jobCount)
	}

	return &WorkerPoolStats{
		ActiveWorkers:   int(atomic.LoadInt32(&wp.stats.activeWorkers)),
		QueuedJobs:      int(atomic.LoadInt32(&wp.stats.queuedJobs)),
		CompletedJobs:   atomic.LoadInt64(&wp.stats.completedJobs),
		FailedJobs:      atomic.LoadInt64(&wp.stats.failedJobs),
		AvgJobDuration:  avgJobDuration,
	}
}

// dispatch distributes jobs to available workers
func (wp *WorkerPool) dispatch() {
	defer wp.wg.Done()

	for {
		select {
		case job := <-wp.jobQueue:
			// Decrement queued jobs count
			atomic.AddInt32(&wp.stats.queuedJobs, -1)

			// Find an available worker
			select {
			case jobChannel := <-wp.workerQueue:
				// Send job to available worker
				jobChannel <- job
			case <-wp.quit:
				// Worker pool is shutting down
				if job.callback != nil {
					job.callback(fmt.Errorf("worker pool is shutting down"))
				}
				return
			}

		case <-wp.quit:
			// Worker pool is shutting down
			wp.logger.Info("Dispatcher shutting down")
			return
		}
	}
}

// Start starts the worker and begins processing jobs
func (w *Worker) Start(done func()) {
	defer done()

	w.logger.Debug("Worker started", "worker_id", w.ID)

	for {
		// Add worker to the worker queue
		w.WorkerQueue <- w.JobChannel

		select {
		case job := <-w.JobChannel:
			// Process the job
			w.processJob(job)

		case <-w.Quit:
			// Worker is being shut down
			w.logger.Debug("Worker shutting down", "worker_id", w.ID)
			return
		}
	}
}

// processJob processes a single collection job
func (w *Worker) processJob(jobWrapper *jobWrapper) {
	startTime := time.Now()
	w.stats.lastJobTime = startTime

	w.logger.Debug("Processing job",
		"worker_id", w.ID,
		"pair", jobWrapper.job.Pair,
		"interval", jobWrapper.job.Interval,
		"is_update", jobWrapper.job.IsUpdate,
	)

	// Rate limiting - wait for permission to make API call
	if err := w.rateLimiter.Wait(jobWrapper.ctx); err != nil {
		w.recordJobFailure(time.Since(startTime))
		if jobWrapper.callback != nil {
			jobWrapper.callback(fmt.Errorf("rate limiting failed: %w", err))
		}
		return
	}

	// Simulate job processing (this would be the actual collection logic)
	err := w.executeCollection(jobWrapper.ctx, jobWrapper.job)

	// Record job completion
	duration := time.Since(startTime)
	if err != nil {
		w.recordJobFailure(duration)
		w.logger.Error("Job failed",
			"worker_id", w.ID,
			"pair", jobWrapper.job.Pair,
			"error", err,
			"duration", duration,
		)
	} else {
		w.recordJobSuccess(duration)
		w.logger.Debug("Job completed successfully",
			"worker_id", w.ID,
			"pair", jobWrapper.job.Pair,
			"duration", duration,
		)
	}

	// Call the callback with the result
	if jobWrapper.callback != nil {
		jobWrapper.callback(err)
	}
}

// executeCollection performs the actual data collection
func (w *Worker) executeCollection(ctx context.Context, job *WorkerJob) error {
	// This is a placeholder implementation
	// In the real implementation, this would:
	// 1. Determine time range for collection
	// 2. Fetch data from exchange
	// 3. Validate data
	// 4. Store data
	// 5. Detect gaps

	// For now, just simulate some work
	select {
	case <-time.After(100 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recordJobSuccess records successful job completion statistics
func (w *Worker) recordJobSuccess(duration time.Duration) {
	atomic.AddInt64(&w.stats.completedJobs, 1)
	atomic.AddInt64(&w.stats.totalJobTime, duration.Nanoseconds())
}

// recordJobFailure records failed job statistics
func (w *Worker) recordJobFailure(duration time.Duration) {
	atomic.AddInt64(&w.stats.failedJobs, 1)
	atomic.AddInt64(&w.stats.totalJobTime, duration.Nanoseconds())
}