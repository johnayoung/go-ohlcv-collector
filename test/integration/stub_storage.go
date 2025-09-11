package integration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
)

// StubStorage provides an in-memory storage implementation for testing
type StubStorage struct {
	candles map[string][]contracts.Candle // key: pair_interval
	gaps    map[string][]contracts.Gap    // key: pair_interval
	mu      sync.RWMutex
	closed  bool
}

// NewStubStorage creates a new stub storage instance
func NewStubStorage() *StubStorage {
	return &StubStorage{
		candles: make(map[string][]contracts.Candle),
		gaps:    make(map[string][]contracts.Gap),
	}
}

// CandleStorer implementation

func (s *StubStorage) Store(ctx context.Context, candles []contracts.Candle) error {
	return s.StoreBatch(ctx, candles)
}

func (s *StubStorage) StoreBatch(ctx context.Context, candles []contracts.Candle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	for _, candle := range candles {
		key := s.getKey(candle.Pair, candle.Interval)
		s.candles[key] = append(s.candles[key], candle)
	}

	// Sort candles by timestamp to maintain order
	for key := range s.candles {
		sort.Slice(s.candles[key], func(i, j int) bool {
			return s.candles[key][i].Timestamp.Before(s.candles[key][j].Timestamp)
		})
	}

	return nil
}

// CandleReader implementation

func (s *StubStorage) Query(ctx context.Context, req contracts.QueryRequest) (*contracts.QueryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	key := s.getKey(req.Pair, req.Interval)
	allCandles, exists := s.candles[key]
	if !exists {
		return &contracts.QueryResponse{
			Candles:   []contracts.Candle{},
			Total:     0,
			HasMore:   false,
			QueryTime: time.Microsecond,
		}, nil
	}

	// Filter by time range
	var filteredCandles []contracts.Candle
	for _, candle := range allCandles {
		if (!req.Start.IsZero() && candle.Timestamp.Before(req.Start)) ||
			(!req.End.IsZero() && candle.Timestamp.After(req.End)) {
			continue
		}
		filteredCandles = append(filteredCandles, candle)
	}

	// Apply ordering
	if req.OrderBy == "timestamp_desc" {
		sort.Slice(filteredCandles, func(i, j int) bool {
			return filteredCandles[i].Timestamp.After(filteredCandles[j].Timestamp)
		})
	}

	// Apply limit and offset
	total := len(filteredCandles)
	start := req.Offset
	if start > total {
		start = total
	}

	end := start + req.Limit
	if req.Limit == 0 || end > total {
		end = total
	}

	result := filteredCandles[start:end]
	hasMore := end < total

	return &contracts.QueryResponse{
		Candles:    result,
		Total:      total,
		HasMore:    hasMore,
		NextOffset: end,
		QueryTime:  time.Microsecond,
	}, nil
}

func (s *StubStorage) GetLatest(ctx context.Context, pair string, interval string) (*contracts.Candle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	key := s.getKey(pair, interval)
	candles, exists := s.candles[key]
	if !exists || len(candles) == 0 {
		return nil, fmt.Errorf("no candles found")
	}

	// Return the latest candle (last in sorted slice)
	latest := candles[len(candles)-1]
	return &latest, nil
}

// GapStorage implementation

func (s *StubStorage) StoreGap(ctx context.Context, gap contracts.Gap) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	key := s.getKey(gap.Pair, gap.Interval)
	s.gaps[key] = append(s.gaps[key], gap)
	return nil
}

func (s *StubStorage) GetGaps(ctx context.Context, pair string, interval string) ([]contracts.Gap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	key := s.getKey(pair, interval)
	gaps, exists := s.gaps[key]
	if !exists {
		return []contracts.Gap{}, nil
	}

	// Return a copy
	result := make([]contracts.Gap, len(gaps))
	copy(result, gaps)
	return result, nil
}

func (s *StubStorage) GetGapByID(ctx context.Context, gapID string) (*contracts.Gap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	for _, gaps := range s.gaps {
		for _, gap := range gaps {
			if gap.ID == gapID {
				return &gap, nil
			}
		}
	}

	return nil, fmt.Errorf("gap not found")
}

func (s *StubStorage) MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	for key, gaps := range s.gaps {
		for i, gap := range gaps {
			if gap.ID == gapID {
				gap.Status = "filled"
				gap.FilledAt = &filledAt
				s.gaps[key][i] = gap
				return nil
			}
		}
	}

	return fmt.Errorf("gap not found")
}

func (s *StubStorage) DeleteGap(ctx context.Context, gapID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	for key, gaps := range s.gaps {
		for i, gap := range gaps {
			if gap.ID == gapID {
				s.gaps[key] = append(gaps[:i], gaps[i+1:]...)
				return nil
			}
		}
	}

	return fmt.Errorf("gap not found")
}

// StorageManager implementation

func (s *StubStorage) Initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	// Nothing to initialize for in-memory storage
	return nil
}

func (s *StubStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.candles = nil
	s.gaps = nil
	return nil
}

func (s *StubStorage) Migrate(ctx context.Context, version int) error {
	// No migrations needed for stub storage
	return nil
}

func (s *StubStorage) GetStats(ctx context.Context) (*contracts.StorageStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	totalCandles := int64(0)
	pairSet := make(map[string]struct{})
	var earliest, latest time.Time

	for pair, candles := range s.candles {
		totalCandles += int64(len(candles))
		pairSet[pair] = struct{}{}

		for _, candle := range candles {
			if earliest.IsZero() || candle.Timestamp.Before(earliest) {
				earliest = candle.Timestamp
			}
			if latest.IsZero() || candle.Timestamp.After(latest) {
				latest = candle.Timestamp
			}
		}
	}

	return &contracts.StorageStats{
		TotalCandles: totalCandles,
		TotalPairs:   len(pairSet),
		EarliestData: earliest,
		LatestData:   latest,
		StorageSize:  0, // Not applicable for memory
		IndexSize:    0, // Not applicable for memory
		QueryPerformance: map[string]time.Duration{
			"query": time.Microsecond,
		},
	}, nil
}

func (s *StubStorage) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	return nil
}

// Helper methods

func (s *StubStorage) getKey(pair, interval string) string {
	return fmt.Sprintf("%s_%s", pair, interval)
}

// Additional methods for testing

func (s *StubStorage) GetCandleCount(pair, interval string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.getKey(pair, interval)
	candles, exists := s.candles[key]
	if !exists {
		return 0
	}
	return len(candles)
}

func (s *StubStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.candles = make(map[string][]contracts.Candle)
	s.gaps = make(map[string][]contracts.Gap)
}
