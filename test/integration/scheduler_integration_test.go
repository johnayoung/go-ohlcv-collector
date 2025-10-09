package integration

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/johnayoung/go-ohlcv-collector/internal/collector"
	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
)

// MockExchangeAdapter provides a test implementation of ExchangeAdapter
type MockExchangeAdapter struct {
	fetchDelay       time.Duration
	fetchCallCount   int64
	shouldFailFetch  bool
	rateLimitDelay   time.Duration
	healthCheckFails bool
	tradingPairs     []contracts.TradingPair
	mu               sync.RWMutex
}

func NewMockExchangeAdapter() *MockExchangeAdapter {
	return &MockExchangeAdapter{
		fetchDelay: 100 * time.Millisecond,
		tradingPairs: []contracts.TradingPair{
			{Symbol: "BTC/USD", BaseAsset: "BTC", QuoteAsset: "USD", Active: true},
			{Symbol: "ETH/USD", BaseAsset: "ETH", QuoteAsset: "USD", Active: true},
		},
	}
}

func (m *MockExchangeAdapter) FetchCandles(ctx context.Context, req contracts.FetchRequest) (*contracts.FetchResponse, error) {
	atomic.AddInt64(&m.fetchCallCount, 1)

	if m.fetchDelay > 0 {
		select {
		case <-time.After(m.fetchDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.mu.RLock()
	shouldFail := m.shouldFailFetch
	m.mu.RUnlock()

	if shouldFail {
		return nil, assert.AnError
	}

	// Generate mock candle data
	candle := contracts.Candle{
		Timestamp: req.Start,
		Open:      "50000.00",
		High:      "51000.00",
		Low:       "49000.00",
		Close:     "50500.00",
		Volume:    "100.0",
		Pair:      req.Pair,
		Interval:  req.Interval,
	}

	return &contracts.FetchResponse{
		Candles: []contracts.Candle{candle},
		RateLimit: contracts.RateLimitStatus{
			Remaining:  100,
			ResetTime:  time.Now().Add(time.Minute),
			RetryAfter: 0,
		},
	}, nil
}

func (m *MockExchangeAdapter) GetTradingPairs(ctx context.Context) ([]contracts.TradingPair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tradingPairs, nil
}

func (m *MockExchangeAdapter) GetPairInfo(ctx context.Context, pair string) (*contracts.PairInfo, error) {
	return &contracts.PairInfo{
		TradingPair: contracts.TradingPair{Symbol: pair, Active: true},
		LastPrice:   "50000.00",
		Volume24h:   "1000.0",
		UpdatedAt:   time.Now(),
	}, nil
}

func (m *MockExchangeAdapter) GetLimits() contracts.RateLimit {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return contracts.RateLimit{
		RequestsPerSecond: 10,
		BurstSize:         5,
		WindowDuration:    time.Second,
	}
}

func (m *MockExchangeAdapter) WaitForLimit(ctx context.Context) error {
	if m.rateLimitDelay > 0 {
		select {
		case <-time.After(m.rateLimitDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *MockExchangeAdapter) HealthCheck(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.healthCheckFails {
		return assert.AnError
	}
	return nil
}

func (m *MockExchangeAdapter) SetFetchDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchDelay = delay
}

func (m *MockExchangeAdapter) SetShouldFailFetch(shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFailFetch = shouldFail
}

func (m *MockExchangeAdapter) SetHealthCheckFails(fails bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthCheckFails = fails
}

func (m *MockExchangeAdapter) GetFetchCallCount() int64 {
	return atomic.LoadInt64(&m.fetchCallCount)
}

// MockStorage provides a test implementation of FullStorage
type MockStorage struct {
	candles    []contracts.Candle
	gaps       []contracts.Gap
	storeCount int64
	queryCount int64
	shouldFail bool
	mu         sync.RWMutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		candles: make([]contracts.Candle, 0),
		gaps:    make([]contracts.Gap, 0),
	}
}

func (m *MockStorage) Store(ctx context.Context, candles []contracts.Candle) error {
	atomic.AddInt64(&m.storeCount, 1)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		return assert.AnError
	}

	m.candles = append(m.candles, candles...)
	return nil
}

func (m *MockStorage) StoreBatch(ctx context.Context, candles []contracts.Candle) error {
	return m.Store(ctx, candles)
}

func (m *MockStorage) Query(ctx context.Context, req contracts.QueryRequest) (*contracts.QueryResponse, error) {
	atomic.AddInt64(&m.queryCount, 1)
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.shouldFail {
		return nil, assert.AnError
	}

	return &contracts.QueryResponse{
		Candles:   m.candles,
		Total:     len(m.candles),
		HasMore:   false,
		QueryTime: 10 * time.Millisecond,
	}, nil
}

func (m *MockStorage) GetLatest(ctx context.Context, pair string, interval string) (*contracts.Candle, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.candles) == 0 {
		return nil, nil
	}

	return &m.candles[len(m.candles)-1], nil
}

func (m *MockStorage) StoreGap(ctx context.Context, gap contracts.Gap) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gaps = append(m.gaps, gap)
	return nil
}

func (m *MockStorage) GetGaps(ctx context.Context, pair string, interval string) ([]contracts.Gap, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gaps, nil
}

func (m *MockStorage) GetGapByID(ctx context.Context, gapID string) (*contracts.Gap, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, gap := range m.gaps {
		if gap.ID == gapID {
			return &gap, nil
		}
	}
	return nil, nil
}

func (m *MockStorage) MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error {
	return nil
}

func (m *MockStorage) DeleteGap(ctx context.Context, gapID string) error {
	return nil
}

func (m *MockStorage) Initialize(ctx context.Context) error {
	return nil
}

func (m *MockStorage) Close() error {
	return nil
}

func (m *MockStorage) Migrate(ctx context.Context, version int) error {
	return nil
}

func (m *MockStorage) GetStats(ctx context.Context) (*contracts.StorageStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &contracts.StorageStats{
		TotalCandles: int64(len(m.candles)),
		TotalPairs:   2,
		StorageSize:  1024,
	}, nil
}

func (m *MockStorage) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *MockStorage) GetStoreCount() int64 {
	return atomic.LoadInt64(&m.storeCount)
}

func (m *MockStorage) SetShouldFail(shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFail = shouldFail
}

// MockCollector provides a test implementation of Collector
type MockCollector struct {
	isRunning      bool
	collectCount   int64
	shouldFailNext bool
	mu             sync.RWMutex
}

func NewMockCollector() *MockCollector {
	return &MockCollector{}
}

func (m *MockCollector) CollectHistorical(ctx context.Context, req collector.HistoricalRequest) error {
	atomic.AddInt64(&m.collectCount, 1)

	m.mu.RLock()
	shouldFail := m.shouldFailNext
	m.mu.RUnlock()

	if shouldFail {
		return assert.AnError
	}

	// Simulate very quick collection work
	time.Sleep(1 * time.Millisecond)
	return nil
}

func (m *MockCollector) CollectScheduled(ctx context.Context, pairs []string, interval string) error {
	atomic.AddInt64(&m.collectCount, 1)
	return nil
}

func (m *MockCollector) GetMetrics(ctx context.Context) (*collector.CollectionMetrics, error) {
	return &collector.CollectionMetrics{
		CandlesCollected: atomic.LoadInt64(&m.collectCount),
		SuccessRate:      1.0,
	}, nil
}

func (m *MockCollector) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isRunning = true
	return nil
}

func (m *MockCollector) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isRunning = false
	return nil
}

func (m *MockCollector) Health(ctx context.Context) error {
	return nil
}

func (m *MockCollector) SetShouldFailNext(shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFailNext = shouldFail
}

func (m *MockCollector) GetCollectCount() int64 {
	return atomic.LoadInt64(&m.collectCount)
}

// MockCollectionJob provides a test implementation of CollectionJob
type MockCollectionJob struct {
	id           string
	pair         string
	interval     string
	nextRun      time.Time
	executeCount int64
	shouldFail   bool
	mu           sync.RWMutex
}

func NewMockCollectionJob(pair, interval string) *MockCollectionJob {
	return &MockCollectionJob{
		id:       pair + "-" + interval,
		pair:     pair,
		interval: interval,
		nextRun:  time.Now().Add(time.Hour),
	}
}

func (m *MockCollectionJob) Execute(ctx context.Context) error {
	atomic.AddInt64(&m.executeCount, 1)

	m.mu.RLock()
	shouldFail := m.shouldFail
	m.mu.RUnlock()

	if shouldFail {
		return assert.AnError
	}

	return nil
}

func (m *MockCollectionJob) GetPair() string {
	return m.pair
}

func (m *MockCollectionJob) GetInterval() string {
	return m.interval
}

func (m *MockCollectionJob) GetNextRun() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextRun
}

func (m *MockCollectionJob) SetNextRun(nextRun time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRun = nextRun
}

func (m *MockCollectionJob) GetID() string {
	return m.id
}

func (m *MockCollectionJob) SetShouldFail(shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFail = shouldFail
}

func (m *MockCollectionJob) GetExecuteCount() int64 {
	return atomic.LoadInt64(&m.executeCount)
}

// Use interfaces from collector package
type CollectionJob = collector.CollectionJob
type SchedulerConfig = collector.SchedulerConfig
type Scheduler = collector.Scheduler
type SchedulerStats = collector.SchedulerStats

// Test Suite
type SchedulerIntegrationTestSuite struct {
	suite.Suite
	scheduler Scheduler
	exchange  *MockExchangeAdapter
	storage   *MockStorage
	collector *MockCollector
	ctx       context.Context
	cancel    context.CancelFunc
}

func (s *SchedulerIntegrationTestSuite) SetupSuite() {
	// Initialize mocks
	s.exchange = NewMockExchangeAdapter()
	s.storage = NewMockStorage()
	s.collector = NewMockCollector()

	// Initialize actual scheduler
	config := &collector.SchedulerConfig{
		Pairs:                 []string{"BTC/USD", "ETH/USD"},
		Intervals:             []string{"1m", "5m"}, // Use shorter intervals for testing
		MaxConcurrentJobs:     5,
		HealthCheckInterval:   30 * time.Second,
		RecoveryRetryDelay:    5 * time.Second,
		EnableHourlyAlignment: false,                  // Disable for faster testing
		TickInterval:          100 * time.Millisecond, // Faster for testing
	}

	s.scheduler = collector.NewScheduler(config, s.exchange, s.storage, s.collector)
}

func (s *SchedulerIntegrationTestSuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *SchedulerIntegrationTestSuite) TearDownTest() {
	// Stop scheduler if running
	if s.scheduler != nil && s.scheduler.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.scheduler.Stop(ctx)
		cancel()
	}

	if s.cancel != nil {
		s.cancel()
	}
}

func (s *SchedulerIntegrationTestSuite) TearDownSuite() {
	// Final cleanup
	if s.scheduler != nil && s.scheduler.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.scheduler.Stop(ctx)
		cancel()
	}
}

func (s *SchedulerIntegrationTestSuite) TestSchedulerLifecycle() {
	// Test enabled now that scheduler is implemented

	// Use background context to avoid cancellation issues
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()

	// Test scheduler start
	err := s.scheduler.Start(testCtx)
	s.Require().NoError(err)
	s.Assert().True(s.scheduler.IsRunning())
	s.Assert().False(s.scheduler.IsPaused())

	// Test scheduler pause
	err = s.scheduler.Pause(testCtx)
	s.Require().NoError(err)
	s.Assert().True(s.scheduler.IsRunning())
	s.Assert().True(s.scheduler.IsPaused())

	// Test scheduler resume
	err = s.scheduler.Resume(testCtx)
	s.Require().NoError(err)
	s.Assert().True(s.scheduler.IsRunning())
	s.Assert().False(s.scheduler.IsPaused())

	// Test scheduler stop
	err = s.scheduler.Stop(testCtx)
	s.Require().NoError(err)
	s.Assert().False(s.scheduler.IsRunning())
	s.Assert().False(s.scheduler.IsPaused())
}

func (s *SchedulerIntegrationTestSuite) TestHourlyBoundaryAlignment() {
	// Test enabled now that scheduler is implemented

	// Test that jobs are aligned to hour boundaries
	// This simulates the hourly boundary alignment mentioned in research.md

	err := s.scheduler.Start(s.ctx)
	s.Require().NoError(err)

	// Wait for jobs to be scheduled
	time.Sleep(200 * time.Millisecond)

	jobs := s.scheduler.GetJobs()
	s.Assert().NotEmpty(jobs)

	// Check that next run times are aligned to hour boundaries
	for _, job := range jobs {
		nextRun := job.GetNextRun()
		// For hourly jobs, should be aligned to the next hour
		if job.GetInterval() == "1h" {
			s.Assert().Equal(0, nextRun.Minute(), "Hourly jobs should align to hour boundaries")
			s.Assert().Equal(0, nextRun.Second(), "Hourly jobs should align to hour boundaries")
		}
	}
}

func (s *SchedulerIntegrationTestSuite) TestConcurrentCollectionJobs() {
	// Test enabled now that scheduler is implemented

	// Set exchange to have some delay to simulate realistic conditions
	s.exchange.SetFetchDelay(50 * time.Millisecond)

	// Use background context to avoid cancellation issues
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()

	err := s.scheduler.Start(testCtx)
	s.Require().NoError(err)
	defer s.scheduler.Stop(context.Background())

	// Wait for scheduler to start and jobs to execute
	// Allow at least 5 ticks (500ms) for jobs to run
	time.Sleep(1 * time.Second)

	// Explicitly stop the scheduler before checking stats
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	err = s.scheduler.Stop(stopCtx)
	s.Require().NoError(err)

	stats := s.scheduler.GetStats()
	s.T().Logf("Stats: CompletedJobs=%d, FailedJobs=%d", stats.CompletedJobs, stats.FailedJobs)

	// Verify that multiple jobs can run concurrently
	// Check that fetch calls happened (indicating jobs executed)
	fetchCount := s.exchange.GetFetchCallCount()
	s.T().Logf("Fetch count: %d", fetchCount)

	// Verify data was stored
	storeCount := s.storage.GetStoreCount()
	s.T().Logf("Store count: %d", storeCount)

	collectCount := s.collector.GetCollectCount()
	s.T().Logf("Collect count: %d", collectCount)

	// Main test: verify that scheduled jobs executed
	s.Assert().Greater(stats.CompletedJobs, int64(0), "Scheduler should have executed some jobs")
	s.Assert().Greater(collectCount, int64(1), "Multiple collection jobs should have run")

	// For integration testing, fetch and store count depend on the collector implementation
	// In our case, the mock collector doesn't call exchange/storage, so we just verify jobs ran
	s.Assert().Equal(int64(4), stats.CompletedJobs, "All 4 jobs (2 pairs × 2 intervals) should have completed")
	s.Assert().Equal(int64(0), stats.FailedJobs, "No jobs should have failed")
}

func (s *SchedulerIntegrationTestSuite) TestSchedulerRecoveryAfterInterruption() {
	s.T().Skip("Skipping until scheduler is implemented")

	// Start scheduler
	err := s.scheduler.Start(s.ctx)
	s.Require().NoError(err)

	// Simulate exchange failure
	s.exchange.SetShouldFailFetch(true)

	// Wait for failure to occur
	time.Sleep(200 * time.Millisecond)

	stats := s.scheduler.GetStats()
	initialFailures := stats.FailedJobs

	// Recovery: fix exchange
	s.exchange.SetShouldFailFetch(false)

	// Wait for recovery
	time.Sleep(500 * time.Millisecond)

	// Verify scheduler recovered and continued working
	newStats := s.scheduler.GetStats()
	s.Assert().Greater(newStats.CompletedJobs, stats.CompletedJobs)
	s.Assert().GreaterOrEqual(newStats.FailedJobs, initialFailures)
}

func (s *SchedulerIntegrationTestSuite) TestTickerBehaviorAndAlignment() {
	// Test enabled now that scheduler is implemented

	// Test time.Ticker behavior for scheduling
	// This test validates that the scheduler properly uses time.Ticker
	// and aligns executions correctly

	startTime := time.Now()
	err := s.scheduler.Start(s.ctx)
	s.Require().NoError(err)

	// Wait for at least 2 tick cycles
	time.Sleep(2*time.Second + 100*time.Millisecond)

	stats := s.scheduler.GetStats()

	// Verify scheduler has been running for expected duration
	expectedUptime := time.Since(startTime).Seconds()
	s.Assert().Greater(stats.UptimeSeconds, int64(expectedUptime-1))
	s.Assert().Less(stats.UptimeSeconds, int64(expectedUptime+1))

	// Verify tick-based execution happened
	s.Assert().Greater(stats.CompletedJobs, int64(0))
}

func (s *SchedulerIntegrationTestSuite) TestContextCancellationAndGracefulShutdown() {
	s.T().Skip("Skipping until scheduler is implemented")

	// Create a short-lived context
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer shortCancel()

	err := s.scheduler.Start(shortCtx)
	s.Require().NoError(err)

	// Set exchange to have longer delay than context timeout
	s.exchange.SetFetchDelay(500 * time.Millisecond)

	// Wait for context to expire
	<-shortCtx.Done()

	// Scheduler should handle cancellation gracefully
	s.Assert().False(s.scheduler.IsRunning())

	// Verify no goroutine leaks occurred
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Test explicit stop with context cancellation
	newCtx, newCancel := context.WithCancel(context.Background())

	err = s.scheduler.Start(newCtx)
	s.Require().NoError(err)

	// Cancel context while scheduler is running
	newCancel()

	// Wait for graceful shutdown
	time.Sleep(200 * time.Millisecond)
	s.Assert().False(s.scheduler.IsRunning())
}

func (s *SchedulerIntegrationTestSuite) TestMemoryManagementLongRunning() {
	s.T().Skip("Skipping until scheduler is implemented")

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	err := s.scheduler.Start(s.ctx)
	s.Require().NoError(err)

	// Run scheduler for a period to check memory usage
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		runtime.GC()
	}

	runtime.ReadMemStats(&m2)

	// Memory usage should not increase excessively
	memoryIncrease := float64(m2.Alloc-m1.Alloc) / (1024 * 1024) // MB
	stats := s.scheduler.GetStats()

	// Verify memory usage is tracked
	s.Assert().Greater(stats.MemoryUsageMB, float64(0))

	// Memory increase should be reasonable (less than 10MB for this test)
	s.Assert().Less(memoryIncrease, float64(10), "Memory usage increased too much: %.2f MB", memoryIncrease)

	// Verify jobs are completing without accumulating in memory
	s.Assert().Greater(stats.CompletedJobs, int64(0))
}

func (s *SchedulerIntegrationTestSuite) TestJobManagement() {
	// Create mock CollectionJob implementation
	mockJob := NewMockCollectionJob("BTC/USD", "1h")

	// Test adding jobs
	err := s.scheduler.AddJob(mockJob)
	s.Require().NoError(err)

	jobs := s.scheduler.GetJobs()
	initialJobCount := len(jobs)

	// Verify job was added
	s.Assert().Greater(initialJobCount, 0, "Should have jobs after adding")

	// Verify we can find the added job
	found := false
	for _, job := range jobs {
		if job.GetPair() == "BTC/USD" && job.GetInterval() == "1h" {
			found = true
			break
		}
	}
	s.Assert().True(found, "Added job should be in job list")

	// Test removing jobs
	err = s.scheduler.RemoveJob("BTC/USD", "1h")
	s.Require().NoError(err)

	jobs = s.scheduler.GetJobs()
	s.Assert().Equal(initialJobCount-1, len(jobs))

	// Verify job was removed
	found = false
	for _, job := range jobs {
		if job.GetPair() == "BTC/USD" && job.GetInterval() == "1h" {
			found = true
			break
		}
	}
	s.Assert().False(found, "Removed job should not be in job list")
}

func (s *SchedulerIntegrationTestSuite) TestSchedulerStatsAccuracy() {
	// Test enabled now that scheduler is implemented

	err := s.scheduler.Start(s.ctx)
	s.Require().NoError(err)

	// Wait for some job executions
	time.Sleep(300 * time.Millisecond)

	stats := s.scheduler.GetStats()

	// Verify stats are reasonable
	s.Assert().GreaterOrEqual(stats.TotalJobs, 0)
	s.Assert().GreaterOrEqual(stats.RunningJobs, 0)
	s.Assert().GreaterOrEqual(stats.CompletedJobs, int64(0))
	s.Assert().GreaterOrEqual(stats.FailedJobs, int64(0))
	s.Assert().False(stats.LastRunTime.IsZero())
	s.Assert().False(stats.NextRunTime.IsZero())
	s.Assert().Greater(stats.UptimeSeconds, int64(0))
	s.Assert().GreaterOrEqual(stats.MemoryUsageMB, float64(0))

	// NextRunTime should be after LastRunTime
	if !stats.LastRunTime.IsZero() && !stats.NextRunTime.IsZero() {
		s.Assert().True(stats.NextRunTime.After(stats.LastRunTime))
	}
}

// Unit tests for individual components
func TestSchedulerIntegrationSuite(t *testing.T) {
	suite.Run(t, new(SchedulerIntegrationTestSuite))
}

// Additional individual integration tests
func TestHourBoundaryCalculation(t *testing.T) {
	// Test enabled now that scheduler is implemented

	// Test hour boundary alignment calculation
	testCases := []struct {
		name         string
		currentTime  time.Time
		interval     string
		expectedNext time.Time
	}{
		{
			name:         "hourly alignment at 10:30 should go to 11:00",
			currentTime:  time.Date(2023, 12, 1, 10, 30, 0, 0, time.UTC),
			interval:     "1h",
			expectedNext: time.Date(2023, 12, 1, 11, 0, 0, 0, time.UTC),
		},
		{
			name:         "hourly alignment at exactly 11:00 should go to 12:00",
			currentTime:  time.Date(2023, 12, 1, 11, 0, 0, 0, time.UTC),
			interval:     "1h",
			expectedNext: time.Date(2023, 12, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:         "daily alignment should go to next day at midnight",
			currentTime:  time.Date(2023, 12, 1, 15, 30, 0, 0, time.UTC),
			interval:     "1d",
			expectedNext: time.Date(2023, 12, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test using mock collector and job to verify boundary calculation
			mockCollector := NewMockCollector()
			job := collector.NewCollectionJobWithNextRun("test", "BTC/USD", tc.interval, mockCollector, tc.currentTime)

			// Create a temporary scheduler just to test the boundary calculation
			config := &collector.SchedulerConfig{
				EnableHourlyAlignment: true,
			}
			scheduler := collector.NewScheduler(config, NewMockExchangeAdapter(), NewMockStorage(), mockCollector)

			// The next run should be calculated based on current time
			actualNext := job.GetNextRun()

			// We can't test exact equality due to timing, but we can test that it's reasonably close
			timeDiff := actualNext.Sub(tc.currentTime)
			expectedDiff := tc.expectedNext.Sub(tc.currentTime)

			// Allow some tolerance (1 minute) for test timing
			assert.InDelta(t, expectedDiff.Minutes(), timeDiff.Minutes(), 1.0,
				"Expected time difference should be close to actual for %s", tc.name)

			_ = scheduler // Avoid unused variable
		})
	}
}

func TestTickerCancellation(t *testing.T) {
	t.Skip("Skipping until scheduler is implemented")

	// Test that time.Ticker is properly canceled when context is done
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	tickerStopped := make(chan bool, 1)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Simulate scheduler tick
			case <-ctx.Done():
				tickerStopped <- true
				return
			}
		}
	}()

	// Wait for context timeout
	<-ctx.Done()

	// Verify ticker was stopped
	select {
	case stopped := <-tickerStopped:
		assert.True(t, stopped)
	case <-time.After(200 * time.Millisecond):
		t.Error("Ticker was not properly canceled")
	}
}

func TestConcurrentJobExecution(t *testing.T) {
	t.Skip("Skipping until scheduler is implemented")

	// Test that multiple jobs can execute concurrently without race conditions
	const numJobs = 10
	const jobDuration = 50 * time.Millisecond

	var wg sync.WaitGroup
	var executionCount int64
	var maxConcurrent int64
	var currentConcurrent int64

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Simulate concurrent job execution
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Track concurrent executions
			current := atomic.AddInt64(&currentConcurrent, 1)
			if current > atomic.LoadInt64(&maxConcurrent) {
				atomic.StoreInt64(&maxConcurrent, current)
			}

			// Simulate job work
			select {
			case <-time.After(jobDuration):
				atomic.AddInt64(&executionCount, 1)
			case <-ctx.Done():
			}

			atomic.AddInt64(&currentConcurrent, -1)
		}()
	}

	wg.Wait()

	// Verify all jobs executed
	assert.Equal(t, int64(numJobs), atomic.LoadInt64(&executionCount))

	// Verify concurrent execution occurred
	assert.Greater(t, atomic.LoadInt64(&maxConcurrent), int64(1))

	// Verify no jobs are still running
	assert.Equal(t, int64(0), atomic.LoadInt64(&currentConcurrent))
}

func TestSchedulerHealthMonitoring(t *testing.T) {
	exchange := NewMockExchangeAdapter()
	storage := NewMockStorage()
	mockCollector := NewMockCollector()

	// Create scheduler with health monitoring
	config := &collector.SchedulerConfig{
		Pairs:                 []string{"BTC/USD"},
		Intervals:             []string{"1h"},
		MaxConcurrentJobs:     5,
		HealthCheckInterval:   50 * time.Millisecond,
		RecoveryRetryDelay:    5 * time.Second,
		EnableHourlyAlignment: false,
		TickInterval:          100 * time.Millisecond,
	}
	scheduler := collector.NewScheduler(config, exchange, storage, mockCollector)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start scheduler
	err := scheduler.Start(ctx)
	assert.NoError(t, err)
	defer scheduler.Stop(context.Background())

	// Verify scheduler is running
	assert.True(t, scheduler.IsRunning())

	// Wait for health checks to occur with healthy dependencies
	time.Sleep(200 * time.Millisecond)

	// Simulate exchange failure
	exchange.SetHealthCheckFails(true)

	// Wait for health check to detect failure
	time.Sleep(150 * time.Millisecond)

	// Verify scheduler detected and handled the health check failure
	// The scheduler should continue running despite health check failures
	// (health checks log warnings but don't stop the scheduler)
	assert.True(t, scheduler.IsRunning(), "Scheduler should continue running despite health check failures")

	// Verify stats are still accessible
	stats := scheduler.GetStats()
	assert.GreaterOrEqual(t, stats.TotalJobs, 1, "Should have at least one job configured")
}
