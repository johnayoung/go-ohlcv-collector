// Package exchange provides contract adapters to bridge between internal types and contract interfaces
package exchange

import (
	"context"

	"github.com/johnayoung/go-ohlcv-collector/internal/contracts"
	"github.com/johnayoung/go-ohlcv-collector/internal/models"
)

// ExchangeContractAdapter wraps any exchange implementation to satisfy the contracts interface
type ExchangeContractAdapter struct {
	impl ExchangeAdapter
}

// NewExchangeContractAdapter creates a new adapter wrapping the given exchange implementation
func NewExchangeContractAdapter(impl ExchangeAdapter) *ExchangeContractAdapter {
	return &ExchangeContractAdapter{impl: impl}
}

// FetchCandles implements contracts.CandleFetcher
func (a *ExchangeContractAdapter) FetchCandles(ctx context.Context, req contracts.FetchRequest) (*contracts.FetchResponse, error) {
	// Convert contract request to exchange request
	exchangeReq := FetchRequest{
		Pair:     req.Pair,
		Start:    req.Start,
		End:      req.End,
		Interval: req.Interval,
		Limit:    req.Limit,
	}

	resp, err := a.impl.FetchCandles(ctx, exchangeReq)
	if err != nil {
		return nil, err
	}

	// Convert response back to contract types
	contractCandles := make([]contracts.Candle, len(resp.Candles))
	for i, c := range resp.Candles {
		contractCandles[i] = exchangeCandleToContract(c)
	}

	return &contracts.FetchResponse{
		Candles:   contractCandles,
		NextToken: resp.NextToken,
		RateLimit: contracts.RateLimitStatus{
			Remaining:  resp.RateLimit.Remaining,
			ResetTime:  resp.RateLimit.ResetTime,
			RetryAfter: resp.RateLimit.RetryAfter,
		},
	}, nil
}

// GetTradingPairs implements contracts.PairProvider
func (a *ExchangeContractAdapter) GetTradingPairs(ctx context.Context) ([]contracts.TradingPair, error) {
	pairs, err := a.impl.GetTradingPairs(ctx)
	if err != nil {
		return nil, err
	}

	contractPairs := make([]contracts.TradingPair, len(pairs))
	for i, p := range pairs {
		contractPairs[i] = contracts.TradingPair{
			Symbol:     p.Symbol,
			BaseAsset:  p.BaseAsset,
			QuoteAsset: p.QuoteAsset,
			Active:     p.Active,
			MinVolume:  p.MinVolume,
			MaxVolume:  p.MaxVolume,
			PriceStep:  p.PriceStep,
		}
	}
	return contractPairs, nil
}

// GetPairInfo implements contracts.PairProvider
func (a *ExchangeContractAdapter) GetPairInfo(ctx context.Context, pair string) (*contracts.PairInfo, error) {
	info, err := a.impl.GetPairInfo(ctx, pair)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}

	return &contracts.PairInfo{
		TradingPair: contracts.TradingPair{
			Symbol:     info.Symbol,
			BaseAsset:  info.BaseAsset,
			QuoteAsset: info.QuoteAsset,
			Active:     info.Active,
			MinVolume:  info.MinVolume,
			MaxVolume:  info.MaxVolume,
			PriceStep:  info.PriceStep,
		},
		LastPrice:   info.LastPrice,
		Volume24h:   info.Volume24h,
		PriceChange: info.PriceChange,
		UpdatedAt:   info.UpdatedAt,
	}, nil
}

// GetLimits implements contracts.RateLimitInfo
func (a *ExchangeContractAdapter) GetLimits() contracts.RateLimit {
	limits := a.impl.GetLimits()
	return contracts.RateLimit{
		RequestsPerSecond: limits.RequestsPerSecond,
		BurstSize:         limits.BurstSize,
		WindowDuration:    limits.WindowDuration,
	}
}

// WaitForLimit implements contracts.RateLimitInfo
func (a *ExchangeContractAdapter) WaitForLimit(ctx context.Context) error {
	return a.impl.WaitForLimit(ctx)
}

// HealthCheck implements contracts.HealthChecker
func (a *ExchangeContractAdapter) HealthCheck(ctx context.Context) error {
	return a.impl.HealthCheck(ctx)
}

// Helper functions to convert between contract and exchange types

func exchangeCandleToContract(c models.Candle) contracts.Candle {
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