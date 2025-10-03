// Package storage provides contract adapters to bridge between internal types and contract interfaces
package storage

import (
	"context"
	"time"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
)

// StorageContractAdapter wraps any storage implementation to satisfy the contracts interface
type StorageContractAdapter struct {
	impl interface {
		Store(ctx context.Context, candles []models.Candle) error
		StoreBatch(ctx context.Context, candles []models.Candle) error
		Query(ctx context.Context, req QueryRequest) (*QueryResponse, error)
		GetLatest(ctx context.Context, pair string, interval string) (*models.Candle, error)
		StoreGap(ctx context.Context, gap models.Gap) error
		GetGaps(ctx context.Context, pair string, interval string) ([]models.Gap, error)
		GetGapByID(ctx context.Context, gapID string) (*models.Gap, error)
		MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error
		DeleteGap(ctx context.Context, gapID string) error
		Initialize(ctx context.Context) error
		Close() error
		Migrate(ctx context.Context, version int) error
		GetStats(ctx context.Context) (*StorageStats, error)
		HealthCheck(ctx context.Context) error
	}
}

// NewStorageContractAdapter creates a new adapter wrapping the given storage implementation
func NewStorageContractAdapter(impl interface {
	Store(ctx context.Context, candles []models.Candle) error
	StoreBatch(ctx context.Context, candles []models.Candle) error
	Query(ctx context.Context, req QueryRequest) (*QueryResponse, error)
	GetLatest(ctx context.Context, pair string, interval string) (*models.Candle, error)
	StoreGap(ctx context.Context, gap models.Gap) error
	GetGaps(ctx context.Context, pair string, interval string) ([]models.Gap, error)
	GetGapByID(ctx context.Context, gapID string) (*models.Gap, error)
	MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error
	DeleteGap(ctx context.Context, gapID string) error
	Initialize(ctx context.Context) error
	Close() error
	Migrate(ctx context.Context, version int) error
	GetStats(ctx context.Context) (*StorageStats, error)
	HealthCheck(ctx context.Context) error
}) *StorageContractAdapter {
	return &StorageContractAdapter{impl: impl}
}

// Store implements contracts.CandleStorer
func (a *StorageContractAdapter) Store(ctx context.Context, candles []contracts.Candle) error {
	modelCandles := make([]models.Candle, len(candles))
	for i, c := range candles {
		modelCandles[i] = contractCandleToModel(c)
	}
	return a.impl.Store(ctx, modelCandles)
}

// StoreBatch implements contracts.CandleStorer
func (a *StorageContractAdapter) StoreBatch(ctx context.Context, candles []contracts.Candle) error {
	modelCandles := make([]models.Candle, len(candles))
	for i, c := range candles {
		modelCandles[i] = contractCandleToModel(c)
	}
	return a.impl.StoreBatch(ctx, modelCandles)
}

// Query implements contracts.CandleReader
func (a *StorageContractAdapter) Query(ctx context.Context, req contracts.QueryRequest) (*contracts.QueryResponse, error) {
	// Convert contract request to model request
	modelReq := QueryRequest{
		Pair:     req.Pair,
		Start:    req.Start,
		End:      req.End,
		Interval: req.Interval,
		Limit:    req.Limit,
		Offset:   req.Offset,
		OrderBy:  req.OrderBy,
	}

	resp, err := a.impl.Query(ctx, modelReq)
	if err != nil {
		return nil, err
	}

	// Convert response back to contract types
	contractCandles := make([]contracts.Candle, len(resp.Candles))
	for i, c := range resp.Candles {
		contractCandles[i] = modelCandleToContract(c)
	}

	return &contracts.QueryResponse{
		Candles:    contractCandles,
		Total:      resp.Total,
		HasMore:    resp.HasMore,
		NextOffset: resp.NextOffset,
		QueryTime:  resp.QueryTime,
	}, nil
}

// GetLatest implements contracts.CandleReader
func (a *StorageContractAdapter) GetLatest(ctx context.Context, pair string, interval string) (*contracts.Candle, error) {
	candle, err := a.impl.GetLatest(ctx, pair, interval)
	if err != nil {
		return nil, err
	}
	if candle == nil {
		return nil, nil
	}
	contractCandle := modelCandleToContract(*candle)
	return &contractCandle, nil
}

// StoreGap implements contracts.GapStorage
func (a *StorageContractAdapter) StoreGap(ctx context.Context, gap contracts.Gap) error {
	modelGap := contractGapToModel(gap)
	return a.impl.StoreGap(ctx, modelGap)
}

// GetGaps implements contracts.GapStorage
func (a *StorageContractAdapter) GetGaps(ctx context.Context, pair string, interval string) ([]contracts.Gap, error) {
	modelGaps, err := a.impl.GetGaps(ctx, pair, interval)
	if err != nil {
		return nil, err
	}

	contractGaps := make([]contracts.Gap, len(modelGaps))
	for i, g := range modelGaps {
		contractGaps[i] = modelGapToContract(g)
	}
	return contractGaps, nil
}

// GetGapByID implements contracts.GapStorage
func (a *StorageContractAdapter) GetGapByID(ctx context.Context, gapID string) (*contracts.Gap, error) {
	gap, err := a.impl.GetGapByID(ctx, gapID)
	if err != nil {
		return nil, err
	}
	if gap == nil {
		return nil, nil
	}
	contractGap := modelGapToContract(*gap)
	return &contractGap, nil
}

// MarkGapFilled implements contracts.GapStorage
func (a *StorageContractAdapter) MarkGapFilled(ctx context.Context, gapID string, filledAt time.Time) error {
	return a.impl.MarkGapFilled(ctx, gapID, filledAt)
}

// DeleteGap implements contracts.GapStorage
func (a *StorageContractAdapter) DeleteGap(ctx context.Context, gapID string) error {
	return a.impl.DeleteGap(ctx, gapID)
}

// Initialize implements contracts.StorageManager
func (a *StorageContractAdapter) Initialize(ctx context.Context) error {
	return a.impl.Initialize(ctx)
}

// Close implements contracts.StorageManager
func (a *StorageContractAdapter) Close() error {
	return a.impl.Close()
}

// Migrate implements contracts.StorageManager
func (a *StorageContractAdapter) Migrate(ctx context.Context, version int) error {
	return a.impl.Migrate(ctx, version)
}

// GetStats implements contracts.StorageManager
func (a *StorageContractAdapter) GetStats(ctx context.Context) (*contracts.StorageStats, error) {
	stats, err := a.impl.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	return &contracts.StorageStats{
		TotalCandles:     stats.TotalCandles,
		TotalPairs:       stats.TotalPairs,
		EarliestData:     stats.EarliestData,
		LatestData:       stats.LatestData,
		StorageSize:      stats.StorageSize,
		IndexSize:        stats.IndexSize,
		QueryPerformance: stats.QueryPerformance,
	}, nil
}

// HealthCheck implements contracts.HealthChecker
func (a *StorageContractAdapter) HealthCheck(ctx context.Context) error {
	return a.impl.HealthCheck(ctx)
}

// Helper functions to convert between contract and model types

func contractCandleToModel(c contracts.Candle) models.Candle {
	return models.Candle{
		Timestamp: c.Timestamp,
		Open:      c.Open,
		High:      c.High,
		Low:       c.Low,
		Close:     c.Close,
		Volume:    c.Volume,
		Pair:      c.Pair,
		Interval:  c.Interval,
	}
}

func modelCandleToContract(c models.Candle) contracts.Candle {
	return contracts.Candle{
		Timestamp: c.Timestamp,
		Open:      c.Open,
		High:      c.High,
		Low:       c.Low,
		Close:     c.Close,
		Volume:    c.Volume,
		Pair:      c.Pair,
		Interval:  c.Interval,
	}
}

func contractGapToModel(g contracts.Gap) models.Gap {
	return models.Gap{
		ID:        g.ID,
		Pair:      g.Pair,
		Interval:  g.Interval,
		StartTime: g.StartTime,
		EndTime:   g.EndTime,
		Status:    models.GapStatus(g.Status),
		CreatedAt: g.CreatedAt,
		FilledAt:  g.FilledAt,
	}
}

func modelGapToContract(g models.Gap) contracts.Gap {
	return contracts.Gap{
		ID:        g.ID,
		Pair:      g.Pair,
		Interval:  g.Interval,
		StartTime: g.StartTime,
		EndTime:   g.EndTime,
		Status:    string(g.Status),
		CreatedAt: g.CreatedAt,
		FilledAt:  g.FilledAt,
	}
}
