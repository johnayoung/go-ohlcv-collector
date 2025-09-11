# Data Model: OHLCV Data Collector

## Core Entities

### OHLCV Candle
Represents price and volume data for a specific trading pair at a time interval.

**Fields**:
- `Timestamp` (time.Time): Candle start time in UTC
- `Open` (string): Opening price as decimal string (for precision)
- `High` (string): Highest price during interval as decimal string
- `Low` (string): Lowest price during interval as decimal string
- `Close` (string): Closing price as decimal string
- `Volume` (string): Trading volume as decimal string
- `Pair` (string): Trading pair symbol (e.g., "BTC-USD")
- `Exchange` (string): Source exchange identifier
- `Interval` (string): Time interval ("1d", "1h", "5m", etc.)

**Go Implementation Note**: 
Prices and volumes stored as strings to preserve precision from exchange APIs. Use `github.com/shopspring/decimal` package for mathematical operations on financial data.

**Validation Rules**:
- High ≥ max(Open, Close)
- Low ≤ min(Open, Close)
- All prices > 0
- Volume ≥ 0
- Timestamp not null

### Trading Pair
Represents a tradeable market on an exchange.

**Fields**:
- `Symbol` (string): Pair identifier (e.g., "BTC-USD")
- `BaseAsset` (string): Base currency (e.g., "BTC")
- `QuoteAsset` (string): Quote currency (e.g., "USD")
- `Exchange` (string): Exchange identifier
- `Active` (bool): Whether pair is currently tradeable
- `MinVolume` (decimal): Minimum trade volume
- `PriceDecimals` (int): Price precision

### Data Gap
Represents missing periods in historical data.

**Fields**:
- `ID` (string): Unique gap identifier
- `Pair` (string): Trading pair symbol
- `StartTime` (time.Time): Gap start timestamp
- `EndTime` (time.Time): Gap end timestamp
- `Interval` (string): Missing data interval
- `Status` (string): "detected", "filling", "filled", "permanent"
- `CreatedAt` (time.Time): When gap was detected
- `FilledAt` (time.Time): When gap was resolved (if applicable)

### Collection Job
Represents a data collection task with status tracking.

**Fields**:
- `ID` (string): Unique job identifier
- `Type` (string): "initial_sync", "update", "backfill"
- `Pair` (string): Target trading pair
- `StartTime` (time.Time): Collection start time
- `EndTime` (time.Time): Collection end time
- `Interval` (string): Candle interval
- `Status` (string): "pending", "running", "completed", "failed"
- `Progress` (int): Percentage complete (0-100)
- `RecordsCollected` (int): Number of candles collected
- `Error` (string): Last error message (if any)
- `RetryCount` (int): Number of retry attempts
- `CreatedAt` (time.Time): Job creation time
- `UpdatedAt` (time.Time): Last status update

### Validation Result
Tracks data quality validation outcomes.

**Fields**:
- `ID` (string): Unique result identifier
- `CandleTimestamp` (time.Time): Validated candle timestamp
- `Pair` (string): Trading pair
- `ValidationTime` (time.Time): When validation ran
- `ChecksPassed` ([]string): List of successful validations
- `Anomalies` ([]Anomaly): Detected anomalies
- `Severity` (string): "info", "warning", "error", "critical"
- `ActionTaken` (string): Response to validation result

### Anomaly
Represents a detected data quality issue.

**Fields**:
- `Type` (string): "price_spike", "volume_surge", "logic_error", "sequence_gap"
- `Description` (string): Human-readable anomaly description
- `Value` (decimal): Anomalous value (if applicable)
- `Threshold` (decimal): Threshold that was exceeded
- `Confidence` (float): Confidence score (0.0-1.0)

## Relationships

- **OHLCV Candle** belongs to **Trading Pair**
- **Data Gap** references **Trading Pair** and time range
- **Collection Job** targets **Trading Pair** and produces **OHLCV Candles**
- **Validation Result** validates **OHLCV Candles** and produces **Anomalies**
- **Trading Pair** is sourced from **Exchange**

## State Transitions

### Collection Job States
```
pending → running → completed
pending → running → failed → pending (retry)
```

### Data Gap States
```
detected → filling → filled
detected → permanent (cannot fill)
```

### Trading Pair States
```
active ⟷ inactive (bidirectional)
```

## Storage Considerations

- **Primary Keys**: Composite keys on (Pair, Timestamp, Interval) for candles
- **Indexes**: Time-based queries require timestamp indexes
- **Partitioning**: Consider date partitioning for large historical datasets
- **Compression**: DuckDB provides automatic columnar compression
- **Retention**: Indefinite storage as specified in requirements