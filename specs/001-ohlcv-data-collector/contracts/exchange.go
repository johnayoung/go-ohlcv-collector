// Exchange adapter interface contracts for OHLCV data collection

package contracts

import (
	"context"
	"time"
)

// CandleFetcher retrieves OHLCV data from exchanges
type CandleFetcher interface {
	FetchCandles(ctx context.Context, req FetchRequest) (*FetchResponse, error)
}

// PairProvider manages trading pair metadata
type PairProvider interface {
	GetTradingPairs(ctx context.Context) ([]TradingPair, error)
	GetPairInfo(ctx context.Context, pair string) (*PairInfo, error)
}

// RateLimitInfo provides rate limiting metadata
type RateLimitInfo interface {
	GetLimits() RateLimit
	WaitForLimit(ctx context.Context) error
}

// ExchangeAdapter combines all exchange capabilities
type ExchangeAdapter interface {
	CandleFetcher
	PairProvider
	RateLimitInfo
	HealthChecker
}

// Request/Response types
type FetchRequest struct {
	Pair     string
	Start    time.Time
	End      time.Time
	Interval string // "1d", "1h", "5m", etc.
	Limit    int    // Max candles per request
}

type FetchResponse struct {
	Candles   []Candle
	NextToken string // For pagination
	RateLimit RateLimitStatus
}

type TradingPair struct {
	Symbol     string
	BaseAsset  string
	QuoteAsset string
	Active     bool
	MinVolume  string
	MaxVolume  string
	PriceStep  string
}

type PairInfo struct {
	TradingPair
	LastPrice   string
	Volume24h   string
	PriceChange string
	UpdatedAt   time.Time
}

type Candle struct {
	Timestamp time.Time
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
	Pair      string
	Interval  string
}

type RateLimit struct {
	RequestsPerSecond int
	BurstSize         int
	WindowDuration    time.Duration
}

type RateLimitStatus struct {
	Remaining  int
	ResetTime  time.Time
	RetryAfter time.Duration
}

type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}
