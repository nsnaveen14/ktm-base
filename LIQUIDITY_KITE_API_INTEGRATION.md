# Liquidity Analysis - Kite API Integration

## Summary of Changes

The liquidity analysis now uses Kite API historical data instead of the candle_stick table. The analysis fetches fresh data directly from Kite every time it runs, ensuring accurate and real-time swing high/low detection.

## Key Changes

### 1. Data Source Migration
**Before:** Used `CandleStickRepository` to fetch candles from database  
**After:** Uses `InstrumentService.getHistoricalData()` to fetch from Kite API

### 2. Automatic Data Refresh
- Every 5 minutes during market hours (9:20 AM - 3:30 PM IST)
- Fetches fresh historical data for each timeframe:
  - **5min:** Last 5 days of data
  - **15min:** Last 7 days of data  
  - **1hour:** Last 15 days of data

### 3. Swing High/Low Detection
The application now:
- Identifies swing highs and lows from live Kite data
- Detects when these levels are taken or broken
- Tracks changes in swing points as new candles form
- Updates liquidity zones based on fresh price action

## Modified Files

### Backend

#### LiquidityZoneServiceImpl.java
- ✅ Removed `CandleStickRepository` dependency
- ✅ Removed `DailyJobService` dependency
- ✅ Removed `KiteTickerProvider` dependency
- ✅ Added `getHistoricalCandlesFromKite()` method
- ✅ Added `mapTimeframeToKiteInterval()` helper
- ✅ Updated all analysis methods to use `HistoricalCandle`
- ✅ Removed database-based candle aggregation
- ✅ Removed ticker-based price fetching

#### Key Methods Updated:
1. **analyzeLiquidityZones()** - Now fetches from Kite API
2. **identifySwingPoints()** - Works with HistoricalCandle
3. **detectLiquidityGrabs()** - Uses Kite candle data
4. **analyzeMarketStructure()** - Analyzes live data
5. **setCurrentDayHighLow()** - Filters today's candles from API response

### Scheduler

#### LiquidityAnalysisScheduler.java
- Already configured to run every 5 minutes
- Each run fetches fresh data from Kite API
- No changes needed - existing scheduler works perfectly

## How It Works

### 1. Scheduled Analysis (Every 5 Minutes)
```
9:20 AM - Fetch latest 5min/15min/1hour data from Kite
9:25 AM - Fetch again (new candles added)
9:30 AM - Fetch again
...
3:30 PM - Final analysis
```

### 2. Data Fetching Process
```java
// For each analysis run:
1. Call Kite API with:
   - instrumentToken (NIFTY/SENSEX)
   - interval (5minute/15minute/60minute)
   - fromDate (5-15 days back)
   - toDate (now)

2. Receive historical candles from Kite

3. Identify swing highs/lows:
   - A swing high: candle.high > 2 candles before & 2 candles after
   - A swing low: candle.low < 2 candles before & 2 candles after

4. Detect liquidity grabs:
   - Buy-side grab: price swept above swing high then closed below
   - Sell-side grab: price swept below swing low then closed above

5. Save analysis to database
```

### 3. Swing Detection Algorithm
```java
// Pivot-based swing detection
for (int i = 2; i < candles.size() - 2; i++) {
    current = candles[i];
    
    // Swing High
    if (current.high > prev2.high && 
        current.high > prev1.high &&
        current.high > next1.high && 
        current.high > next2.high) {
        → Identified Swing High
    }
    
    // Swing Low
    if (current.low < prev2.low && 
        current.low < prev1.low &&
        current.low < next1.low && 
        current.low < next2.low) {
        → Identified Swing Low
    }
}
```

## API Intervals Used

| Internal Timeframe | Kite API Interval | Lookback Period |
|-------------------|-------------------|-----------------|
| 5min              | 5minute           | 5 days          |
| 15min             | 15minute          | 7 days          |
| 1hour             | 60minute          | 15 days         |

## Benefits

### 1. Always Fresh Data
- No dependency on database candle population
- Directly queries Kite for latest data
- Works even if ticker/candle jobs are not running

### 2. Accurate Swing Detection
- Uses actual Kite candle data
- No aggregation errors
- Real-time swing high/low identification

### 3. Liquidity Grab Detection
- Identifies when swing levels are taken
- Detects stop-loss hunt patterns
- Tracks changes in liquidity zones

### 4. Independent Operation
- Doesn't require `CandleStickRepository`
- Doesn't need `KiteTickerProvider` for live prices
- Doesn't depend on `DailyJobService`

## Testing

### During Market Hours
1. Scheduler runs automatically every 5 minutes
2. Fetches latest data from Kite API
3. Identifies current swing highs/lows
4. Detects liquidity grabs
5. Updates UI with fresh analysis

### During Non-Market Hours
1. Click "Analyze All" button in UI
2. Calls `/api/liquidity/trigger-analysis`
3. Fetches historical data from Kite (up to 15 days)
4. Analyzes last known swing points
5. Shows previous day's liquidity levels

### Manual Testing via API
```bash
# Trigger analysis manually
POST http://localhost:8080/api/liquidity/trigger-analysis

# Response:
{
  "success": true,
  "message": "On-demand analysis completed",
  "successCount": 6,
  "failureCount": 0,
  "totalAnalyzed": 6
}

# Get results
GET http://localhost:8080/api/liquidity/all-indices

# Response:
{
  "NIFTY": {
    "instrumentToken": 256265,
    "instrumentName": "NIFTY",
    "timeframes": {
      "5min": {
        "timeframeHigh1": 23550.50,  ← Latest swing high
        "timeframeLow1": 23450.25,   ← Latest swing low
        "buySideGrabbed": false,
        "sellSideGrabbed": true,     ← Stop-loss sweep detected
        "tradeSignal": "SHORT_COVER"
      },
      "15min": {...},
      "1hour": {...}
    }
  },
  "SENSEX": {...}
}
```

## Configuration

No additional configuration needed. The scheduler automatically:
- Runs every 5 minutes during market hours
- Fetches data from Kite API
- Analyzes swing points
- Detects liquidity grabs
- Saves to database

## Monitoring

### Check Logs
```
2026-01-11 09:25:00 INFO  - Fetching historical data from Kite API for token: 256265, interval: 5minute
2026-01-11 09:25:01 INFO  - Retrieved 50 candles from Kite API for token: 256265
2026-01-11 09:25:01 DEBUG - Identified 3 swing highs and 4 swing lows
2026-01-11 09:25:01 INFO  - Sell-side liquidity grabbed at level: 23450.25
2026-01-11 09:25:01 INFO  - Valid LONG_UNWIND setup identified at 23455.50
```

### Check Database
```sql
-- View latest analysis
SELECT 
    instrument_name,
    timeframe,
    timeframe_high_1,
    timeframe_low_1,
    buy_side_grabbed,
    sell_side_grabbed,
    trade_signal,
    analysis_timestamp
FROM liquidity_zone_analysis
WHERE DATE(analysis_timestamp) = CURRENT_DATE
ORDER BY analysis_timestamp DESC
LIMIT 10;
```

## Expected Behavior

### Swing High/Low Detection
- Swings are identified using 2-candle pivot logic
- Most recent 3 swing highs/lows are tracked
- Updates automatically as new candles form

### Liquidity Grab Detection
- **Buy-Side Grab:** Price wicks above swing high then closes below
- **Sell-Side Grab:** Price wicks below swing low then closes above
- Indicates stop-loss hunt and potential reversal

### Trade Signals
- **SHORT_COVER:** Buy-side liquidity grabbed → expect upward move
- **LONG_UNWIND:** Sell-side liquidity grabbed → expect downward move
- **NO_SIGNAL:** No liquidity grab detected

## Troubleshooting

### No Data Appearing?
1. Check if Kite API is accessible
2. Verify instrument tokens are correct
3. Check application logs for API errors
4. Ensure access token is valid

### Swing Points Not Updating?
1. Verify scheduler is running
2. Check that analysis timestamp is recent
3. Ensure sufficient candles available (need at least 5)
4. Verify Kite API is returning data

### API Rate Limits
- Kite API has rate limits
- Scheduler runs every 5 minutes to stay within limits
- If needed, increase interval in scheduler

## Summary

✅ **Data Source:** Kite API (not database)  
✅ **Refresh Rate:** Every 5 minutes during market hours  
✅ **Swing Detection:** Pivot-based algorithm with 2-candle confirmation  
✅ **Liquidity Grabs:** Automatically detected when swings are taken  
✅ **Independence:** No dependency on candle_stick table or ticker  
✅ **Real-Time:** Always fetches latest data from Kite  

The liquidity analysis is now completely independent and always uses fresh data from Kite API!

