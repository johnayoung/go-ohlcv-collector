package contract

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/specs/001-ohlcv-data-collector/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGapStorageContract validates the GapStorage interface implementation
// These tests MUST fail initially as they test against unimplemented interfaces
func TestGapStorageContract(t *testing.T) {
	// This will fail initially - no implementation exists yet
	var storage contracts.GapStorage = nil
	require.NotNil(t, storage, "GapStorage implementation must be provided for contract tests")

	testCases := []struct {
		name string
		test func(t *testing.T, storage contracts.GapStorage)
	}{
		{"StoreGap", testStoreGap},
		{"GetGaps", testGetGaps},
		{"MarkGapFilled", testMarkGapFilled},
		{"DeleteGap", testDeleteGap},
		{"GapLifecycle", testGapLifecycle},
		{"ConcurrentOperations", testConcurrentGapOperations},
		{"OverlappingGaps", testOverlappingGaps},
		{"EdgeCases", testGapEdgeCases},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.test(t, storage)
		})
	}
}

func testStoreGap(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name    string
		gap     contracts.Gap
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_gap",
			gap: contracts.Gap{
				ID:        "gap-001",
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
				Status:    "detected",
				CreatedAt: now,
			},
			wantErr: false,
		},
		{
			name: "gap_with_empty_id",
			gap: contracts.Gap{
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
				Status:    "detected",
				CreatedAt: now,
			},
			wantErr: true,
			errMsg:  "gap ID cannot be empty",
		},
		{
			name: "gap_with_invalid_status",
			gap: contracts.Gap{
				ID:        "gap-002",
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
				Status:    "invalid_status",
				CreatedAt: now,
			},
			wantErr: true,
			errMsg:  "invalid status",
		},
		{
			name: "gap_with_start_after_end",
			gap: contracts.Gap{
				ID:        "gap-003",
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(-1 * time.Hour),
				EndTime:   now.Add(-2 * time.Hour),
				Status:    "detected",
				CreatedAt: now,
			},
			wantErr: true,
			errMsg:  "start time must be before end time",
		},
		{
			name: "duplicate_gap_id",
			gap: contracts.Gap{
				ID:        "gap-001", // Same ID as first test
				Pair:      "ETH/USDT",
				Interval:  "5m",
				StartTime: now.Add(-30 * time.Minute),
				EndTime:   now.Add(-15 * time.Minute),
				Status:    "detected",
				CreatedAt: now,
			},
			wantErr: true,
			errMsg:  "gap ID already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.StoreGap(ctx, tt.gap)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.errMsg))
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func testGetGaps(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Setup test gaps
	testGaps := []contracts.Gap{
		{
			ID:        "gap-btc-001",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-3 * time.Hour),
			EndTime:   now.Add(-2 * time.Hour),
			Status:    "detected",
			CreatedAt: now.Add(-10 * time.Minute),
		},
		{
			ID:        "gap-btc-002",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-5 * time.Hour),
			EndTime:   now.Add(-4 * time.Hour),
			Status:    "filling",
			CreatedAt: now.Add(-20 * time.Minute),
		},
		{
			ID:        "gap-eth-001",
			Pair:      "ETH/USDT",
			Interval:  "1h",
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-1 * time.Hour),
			Status:    "detected",
			CreatedAt: now.Add(-5 * time.Minute),
		},
		{
			ID:        "gap-btc-5m-001",
			Pair:      "BTC/USDT",
			Interval:  "5m",
			StartTime: now.Add(-30 * time.Minute),
			EndTime:   now.Add(-25 * time.Minute),
			Status:    "filled",
			CreatedAt: now.Add(-15 * time.Minute),
			FilledAt:  &[]time.Time{now.Add(-5 * time.Minute)}[0],
		},
	}

	// Store test gaps
	for _, gap := range testGaps {
		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err, "Failed to store test gap: %s", gap.ID)
	}

	tests := []struct {
		name          string
		pair          string
		interval      string
		expectedIDs   []string
		expectedCount int
	}{
		{
			name:          "get_btc_1h_gaps",
			pair:          "BTC/USDT",
			interval:      "1h",
			expectedIDs:   []string{"gap-btc-001", "gap-btc-002"},
			expectedCount: 2,
		},
		{
			name:          "get_eth_1h_gaps",
			pair:          "ETH/USDT",
			interval:      "1h",
			expectedIDs:   []string{"gap-eth-001"},
			expectedCount: 1,
		},
		{
			name:          "get_btc_5m_gaps",
			pair:          "BTC/USDT",
			interval:      "5m",
			expectedIDs:   []string{"gap-btc-5m-001"},
			expectedCount: 1,
		},
		{
			name:          "get_nonexistent_pair",
			pair:          "LTC/USDT",
			interval:      "1h",
			expectedIDs:   []string{},
			expectedCount: 0,
		},
		{
			name:          "get_nonexistent_interval",
			pair:          "BTC/USDT",
			interval:      "1d",
			expectedIDs:   []string{},
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gaps, err := storage.GetGaps(ctx, tt.pair, tt.interval)
			require.NoError(t, err)
			assert.Len(t, gaps, tt.expectedCount)

			// Verify gap IDs
			gotIDs := make([]string, len(gaps))
			for i, gap := range gaps {
				gotIDs[i] = gap.ID
			}
			assert.ElementsMatch(t, tt.expectedIDs, gotIDs)

			// Verify all gaps have correct pair and interval
			for _, gap := range gaps {
				assert.Equal(t, tt.pair, gap.Pair)
				assert.Equal(t, tt.interval, gap.Interval)
			}
		})
	}
}

func testMarkGapFilled(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Setup test gap
	testGap := contracts.Gap{
		ID:        "gap-fill-001",
		Pair:      "BTC/USDT",
		Interval:  "1h",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    "filling",
		CreatedAt: now.Add(-10 * time.Minute),
	}

	err := storage.StoreGap(ctx, testGap)
	require.NoError(t, err)

	tests := []struct {
		name     string
		gapID    string
		filledAt time.Time
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "mark_existing_gap_filled",
			gapID:    "gap-fill-001",
			filledAt: now,
			wantErr:  false,
		},
		{
			name:     "mark_nonexistent_gap_filled",
			gapID:    "gap-nonexistent",
			filledAt: now,
			wantErr:  true,
			errMsg:   "gap not found",
		},
		{
			name:     "mark_gap_with_empty_id",
			gapID:    "",
			filledAt: now,
			wantErr:  true,
			errMsg:   "gap ID cannot be empty",
		},
		{
			name:     "mark_gap_with_future_filled_time",
			gapID:    "gap-fill-001",
			filledAt: now.Add(1 * time.Hour),
			wantErr:  true,
			errMsg:   "filled time cannot be in the future",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.MarkGapFilled(ctx, tt.gapID, tt.filledAt)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.errMsg))
				}
			} else {
				assert.NoError(t, err)

				// Verify gap status was updated
				gaps, err := storage.GetGaps(ctx, testGap.Pair, testGap.Interval)
				require.NoError(t, err)

				var updatedGap *contracts.Gap
				for _, gap := range gaps {
					if gap.ID == tt.gapID {
						updatedGap = &gap
						break
					}
				}

				require.NotNil(t, updatedGap, "Gap should still exist after marking filled")
				assert.Equal(t, "filled", updatedGap.Status)
				assert.NotNil(t, updatedGap.FilledAt)
				assert.Equal(t, tt.filledAt.Unix(), updatedGap.FilledAt.Unix())
			}
		})
	}
}

func testDeleteGap(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Setup test gaps
	testGaps := []contracts.Gap{
		{
			ID:        "gap-delete-001",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-1 * time.Hour),
			Status:    "detected",
			CreatedAt: now.Add(-10 * time.Minute),
		},
		{
			ID:        "gap-delete-002",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-4 * time.Hour),
			EndTime:   now.Add(-3 * time.Hour),
			Status:    "filled",
			CreatedAt: now.Add(-20 * time.Minute),
			FilledAt:  &[]time.Time{now.Add(-5 * time.Minute)}[0],
		},
	}

	for _, gap := range testGaps {
		err := storage.StoreGap(ctx, gap)
		require.NoError(t, err)
	}

	tests := []struct {
		name    string
		gapID   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "delete_existing_gap",
			gapID:   "gap-delete-001",
			wantErr: false,
		},
		{
			name:    "delete_filled_gap",
			gapID:   "gap-delete-002",
			wantErr: false,
		},
		{
			name:    "delete_nonexistent_gap",
			gapID:   "gap-nonexistent",
			wantErr: true,
			errMsg:  "gap not found",
		},
		{
			name:    "delete_with_empty_id",
			gapID:   "",
			wantErr: true,
			errMsg:  "gap ID cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.DeleteGap(ctx, tt.gapID)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.errMsg))
				}
			} else {
				assert.NoError(t, err)

				// Verify gap was deleted by checking it no longer exists
				gaps, err := storage.GetGaps(ctx, "BTC/USDT", "1h")
				require.NoError(t, err)

				for _, gap := range gaps {
					assert.NotEqual(t, tt.gapID, gap.ID, "Gap should be deleted")
				}
			}
		})
	}
}

func testGapLifecycle(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	gapID := "gap-lifecycle-001"
	pair := "BTC/USDT"
	interval := "1h"

	// Step 1: Create gap in "detected" state
	gap := contracts.Gap{
		ID:        gapID,
		Pair:      pair,
		Interval:  interval,
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Status:    "detected",
		CreatedAt: now.Add(-10 * time.Minute),
	}

	err := storage.StoreGap(ctx, gap)
	require.NoError(t, err)

	// Verify initial state
	gaps, err := storage.GetGaps(ctx, pair, interval)
	require.NoError(t, err)
	require.Len(t, gaps, 1)
	assert.Equal(t, "detected", gaps[0].Status)
	assert.Nil(t, gaps[0].FilledAt)

	// Step 2: Update to "filling" state (this would typically be done via StoreGap with updated status)
	gap.Status = "filling"
	err = storage.StoreGap(ctx, gap)
	require.NoError(t, err)

	gaps, err = storage.GetGaps(ctx, pair, interval)
	require.NoError(t, err)
	require.Len(t, gaps, 1)
	assert.Equal(t, "filling", gaps[0].Status)

	// Step 3: Mark as filled
	filledTime := now
	err = storage.MarkGapFilled(ctx, gapID, filledTime)
	require.NoError(t, err)

	gaps, err = storage.GetGaps(ctx, pair, interval)
	require.NoError(t, err)
	require.Len(t, gaps, 1)
	assert.Equal(t, "filled", gaps[0].Status)
	assert.NotNil(t, gaps[0].FilledAt)
	assert.Equal(t, filledTime.Unix(), gaps[0].FilledAt.Unix())

	// Step 4: Delete gap
	err = storage.DeleteGap(ctx, gapID)
	require.NoError(t, err)

	gaps, err = storage.GetGaps(ctx, pair, interval)
	require.NoError(t, err)
	assert.Len(t, gaps, 0)
}

func testConcurrentGapOperations(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	const numGoroutines = 10
	const gapsPerGoroutine = 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*gapsPerGoroutine)

	// Concurrent gap creation
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < gapsPerGoroutine; j++ {
				gap := contracts.Gap{
					ID:        fmt.Sprintf("concurrent-gap-%d-%d", goroutineID, j),
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(time.Duration(-j-1) * time.Hour),
					EndTime:   now.Add(time.Duration(-j) * time.Hour),
					Status:    "detected",
					CreatedAt: now,
				}

				if err := storage.StoreGap(ctx, gap); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	assert.Empty(t, errs, "Concurrent gap creation should not produce errors")

	// Verify all gaps were created
	gaps, err := storage.GetGaps(ctx, "BTC/USDT", "1h")
	require.NoError(t, err)
	assert.Len(t, gaps, numGoroutines*gapsPerGoroutine)

	// Concurrent gap operations (mark filled and delete)
	wg = sync.WaitGroup{}
	errors = make(chan error, len(gaps)*2)

	for i, gap := range gaps {
		wg.Add(2)

		// Mark some gaps as filled
		if i%2 == 0 {
			go func(gapID string) {
				defer wg.Done()
				if err := storage.MarkGapFilled(ctx, gapID, now); err != nil {
					errors <- err
				}
			}(gap.ID)
		} else {
			// Skip marking this one
			go func() {
				defer wg.Done()
			}()
		}

		// Delete some gaps
		if i%3 == 0 {
			go func(gapID string) {
				defer wg.Done()
				// Add small delay to allow mark filled to complete
				time.Sleep(10 * time.Millisecond)
				if err := storage.DeleteGap(ctx, gapID); err != nil {
					errors <- err
				}
			}(gap.ID)
		} else {
			// Skip deleting this one
			go func() {
				defer wg.Done()
			}()
		}
	}

	wg.Wait()
	close(errors)

	// Check for errors (some might be expected due to race conditions)
	errs = nil
	for err := range errors {
		errs = append(errs, err)
	}
	// We expect some "gap not found" errors due to concurrent deletes
	for _, err := range errs {
		assert.True(t,
			strings.Contains(strings.ToLower(err.Error()), "gap not found") ||
				strings.Contains(strings.ToLower(err.Error()), "not found"),
			"Unexpected error type: %v", err)
	}
}

func testOverlappingGaps(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Create overlapping gaps
	gaps := []contracts.Gap{
		{
			ID:        "overlap-001",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-4 * time.Hour),
			EndTime:   now.Add(-2 * time.Hour), // Ends at -2h
			Status:    "detected",
			CreatedAt: now.Add(-30 * time.Minute),
		},
		{
			ID:        "overlap-002",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-3 * time.Hour), // Starts at -3h, overlaps with previous
			EndTime:   now.Add(-1 * time.Hour),
			Status:    "detected",
			CreatedAt: now.Add(-25 * time.Minute),
		},
		{
			ID:        "overlap-003",
			Pair:      "BTC/USDT",
			Interval:  "1h",
			StartTime: now.Add(-5 * time.Hour),
			EndTime:   now.Add(-4 * time.Hour), // Adjacent to first gap
			Status:    "detected",
			CreatedAt: now.Add(-20 * time.Minute),
		},
	}

	// Store all gaps - implementation should handle overlaps appropriately
	for _, gap := range gaps {
		err := storage.StoreGap(ctx, gap)
		// The behavior here depends on implementation - might allow overlaps or merge them
		// For contract testing, we just verify the operation completes
		if err != nil {
			t.Logf("Gap %s storage resulted in: %v", gap.ID, err)
		}
	}

	// Retrieve gaps and verify they can be queried
	retrievedGaps, err := storage.GetGaps(ctx, "BTC/USDT", "1h")
	require.NoError(t, err)
	assert.NotEmpty(t, retrievedGaps, "Should be able to retrieve gaps even with overlaps")

	// Verify all retrieved gaps have valid time ranges
	for _, gap := range retrievedGaps {
		assert.True(t, gap.StartTime.Before(gap.EndTime),
			"Gap %s has invalid time range: start %v, end %v",
			gap.ID, gap.StartTime, gap.EndTime)
	}
}

func testGapEdgeCases(t *testing.T, storage contracts.GapStorage) {
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "zero_duration_gap",
			test: func(t *testing.T) {
				gap := contracts.Gap{
					ID:        "zero-duration",
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(-1 * time.Hour),
					EndTime:   now.Add(-1 * time.Hour), // Same time
					Status:    "detected",
					CreatedAt: now,
				}
				err := storage.StoreGap(ctx, gap)
				assert.Error(t, err, "Should not allow zero-duration gaps")
			},
		},
		{
			name: "gap_in_future",
			test: func(t *testing.T) {
				gap := contracts.Gap{
					ID:        "future-gap",
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(1 * time.Hour), // Future start
					EndTime:   now.Add(2 * time.Hour),
					Status:    "detected",
					CreatedAt: now,
				}
				err := storage.StoreGap(ctx, gap)
				// Implementation might allow future gaps for scheduled operations
				if err != nil {
					assert.Contains(t, strings.ToLower(err.Error()), "future")
				}
			},
		},
		{
			name: "very_long_gap",
			test: func(t *testing.T) {
				gap := contracts.Gap{
					ID:        "long-gap",
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(-365 * 24 * time.Hour), // 1 year ago
					EndTime:   now.Add(-1 * time.Hour),
					Status:    "detected",
					CreatedAt: now,
				}
				err := storage.StoreGap(ctx, gap)
				// Should handle very long gaps gracefully
				assert.NoError(t, err)
			},
		},
		{
			name: "special_characters_in_pair",
			test: func(t *testing.T) {
				gap := contracts.Gap{
					ID:        "special-chars",
					Pair:      "BTC-USD/EUR_TEST",
					Interval:  "1h",
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now.Add(-1 * time.Hour),
					Status:    "detected",
					CreatedAt: now,
				}
				err := storage.StoreGap(ctx, gap)
				assert.NoError(t, err)
			},
		},
		{
			name: "mark_already_filled_gap",
			test: func(t *testing.T) {
				gap := contracts.Gap{
					ID:        "already-filled",
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now.Add(-1 * time.Hour),
					Status:    "filled",
					CreatedAt: now.Add(-30 * time.Minute),
					FilledAt:  &[]time.Time{now.Add(-10 * time.Minute)}[0],
				}

				err := storage.StoreGap(ctx, gap)
				require.NoError(t, err)

				// Try to mark it filled again
				err = storage.MarkGapFilled(ctx, "already-filled", now)
				// Implementation might allow re-marking or reject it
				if err != nil {
					assert.Contains(t, strings.ToLower(err.Error()), "already")
				}
			},
		},
		{
			name: "context_cancellation",
			test: func(t *testing.T) {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel() // Cancel immediately

				gap := contracts.Gap{
					ID:        "cancelled-context",
					Pair:      "BTC/USDT",
					Interval:  "1h",
					StartTime: now.Add(-2 * time.Hour),
					EndTime:   now.Add(-1 * time.Hour),
					Status:    "detected",
					CreatedAt: now,
				}

				err := storage.StoreGap(cancelCtx, gap)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "context")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

// BenchmarkGapStorage provides performance benchmarks for gap operations
func BenchmarkGapStorage(b *testing.B) {
	// This will fail initially - no implementation exists yet
	var storage contracts.GapStorage = nil
	if storage == nil {
		b.Skip("GapStorage implementation not available for benchmarking")
	}

	ctx := context.Background()
	now := time.Now().UTC()

	b.Run("StoreGap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			gap := contracts.Gap{
				ID:        fmt.Sprintf("bench-gap-%d", i),
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(time.Duration(-i-2) * time.Hour),
				EndTime:   now.Add(time.Duration(-i-1) * time.Hour),
				Status:    "detected",
				CreatedAt: now,
			}
			storage.StoreGap(ctx, gap)
		}
	})

	b.Run("GetGaps", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			storage.GetGaps(ctx, "BTC/USDT", "1h")
		}
	})

	b.Run("MarkGapFilled", func(b *testing.B) {
		// Setup gaps first
		for i := 0; i < b.N; i++ {
			gap := contracts.Gap{
				ID:        fmt.Sprintf("mark-bench-gap-%d", i),
				Pair:      "BTC/USDT",
				Interval:  "1h",
				StartTime: now.Add(time.Duration(-i-2) * time.Hour),
				EndTime:   now.Add(time.Duration(-i-1) * time.Hour),
				Status:    "filling",
				CreatedAt: now,
			}
			storage.StoreGap(ctx, gap)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			storage.MarkGapFilled(ctx, fmt.Sprintf("mark-bench-gap-%d", i), now)
		}
	})
}
