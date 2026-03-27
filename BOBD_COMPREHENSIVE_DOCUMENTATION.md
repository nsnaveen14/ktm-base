# BOBD (Breakout/Breakdown Failure) Trading Strategy - Comprehensive Documentation

## Table of Contents
1. [Overview](#overview)
2. [Strategy Concept](#strategy-concept)
3. [Implementation Architecture](#implementation-architecture)
4. [Core Components](#core-components)
5. [Signal Detection Logic](#signal-detection-logic)
6. [Configuration Options](#configuration-options)
7. [Trade Management](#trade-management)
8. [Telegram Notifications](#telegram-notifications)
9. [Multi-Timeframe Analysis](#multi-timeframe-analysis)
10. [Options Greeks Integration](#options-greeks-integration)
11. [Backtesting](#backtesting)
12. [Performance Analytics](#performance-analytics)
13. [API Endpoints](#api-endpoints)
14. [Frontend Components](#frontend-components)
15. [Database Schema](#database-schema)
16. [Known Issues & Fixes](#known-issues--fixes)
17. [Enhancement Suggestions](#enhancement-suggestions)

---

## Overview

The BOBD (Breakout/Breakdown Failure) Trading Strategy is an intraday Nifty 50 options trading system that capitalizes on **fakeout scenarios** - situations where price breaks a key level but fails to sustain and reverses.

### Key Features
- **Real-time signal detection** for breakout/breakdown failures
- **Multi-timeframe analysis** (5min, 15min, 1hour)
- **Options Greeks integration** for optimal strike selection
- **Risk management** with configurable stop-loss and targets
- **Telegram notifications** with toggle control
- **Backtesting** for strategy validation
- **Performance analytics** dashboard

---

## Strategy Concept

### What is a Fakeout?

A **fakeout** (also called a "false breakout") occurs when price temporarily breaks through a significant support or resistance level but quickly reverses, trapping traders who entered on the breakout.

### Key Levels Monitored

| Level Type | Description |
|------------|-------------|
| **PDH** | Prior Day High - Previous trading day's highest price |
| **PDL** | Prior Day Low - Previous trading day's lowest price |
| **ORH_15** | Opening Range High (15 min) - Highest price in first 15 minutes |
| **ORL_15** | Opening Range Low (15 min) - Lowest price in first 15 minutes |
| **ORH_30** | Opening Range High (30 min) - Highest price in first 30 minutes |
| **ORL_30** | Opening Range Low (30 min) - Lowest price in first 30 minutes |

### Signal Types

1. **BULLISH_FAKEOUT** (Buy PUT)
   - Price breaks above resistance (PDH, ORH)
   - Break is 0.2-0.5% beyond the level (50-100 points on Nifty ~25,000)
   - Price fails to sustain and closes back below the level within 2-5 candles
   - Entry: Short position via PUT options

2. **BEARISH_FAKEOUT** (Buy CALL)
   - Price breaks below support (PDL, ORL)
   - Break is 0.2-0.5% beyond the level
   - Price fails to sustain and closes back above the level within 2-5 candles
   - Entry: Long position via CALL options

---

## Implementation Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (Angular)                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐ │
│  │ BOBDFailures    │  │ Dashboard       │  │ Configuration    │ │
│  │ Component       │  │ Charts          │  │ Panel            │ │
│  └────────┬────────┘  └────────┬────────┘  └────────┬─────────┘ │
│           │                    │                     │           │
│  ┌────────▼────────────────────▼─────────────────────▼─────────┐ │
│  │                      BOBD Service                           │ │
│  └─────────────────────────────┬───────────────────────────────┘ │
└────────────────────────────────┼─────────────────────────────────┘
                                 │ HTTP REST API
┌────────────────────────────────▼─────────────────────────────────┐
│                         Backend (Spring Boot)                     │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  BOBDFailureController                      │ │
│  │  /api/bobd/*                                                │ │
│  └─────────────────────────────┬───────────────────────────────┘ │
│                                │                                  │
│  ┌─────────────────────────────▼───────────────────────────────┐ │
│  │                  BOBDFailureServiceImpl                     │ │
│  │  - Signal Detection                                         │ │
│  │  - Trade Management                                         │ │
│  │  - Multi-Timeframe Analysis                                 │ │
│  │  - Options Greeks Calculation                               │ │
│  │  - Backtesting                                              │ │
│  │  - Performance Analytics                                    │ │
│  └───────┬─────────────┬──────────────┬───────────────────────┘ │
│          │             │              │                          │
│  ┌───────▼──────┐ ┌────▼────────┐ ┌──▼────────────────────────┐ │
│  │ BOBDSignal   │ │ BOBDConfig  │ │ TelegramNotificationService│ │
│  │ Repository   │ │ Repository  │ └────────────────────────────┘ │
│  └───────┬──────┘ └─────┬───────┘                                │
└──────────┼──────────────┼────────────────────────────────────────┘
           │              │
    ┌──────▼──────────────▼──────┐
    │        PostgreSQL DB       │
    │   bobd_signals             │
    │   bobd_configuration       │
    │   bobd_backtest_results    │
    └────────────────────────────┘
```

---

## Core Components

### Backend Components

| Component | File | Description |
|-----------|------|-------------|
| **Entity: BOBDSignal** | `BOBDSignal.java` | Stores fakeout signal data |
| **Entity: BOBDConfiguration** | `BOBDConfiguration.java` | Strategy configuration |
| **Entity: BOBDBacktestResult** | `BOBDBacktestResult.java` | Backtest results |
| **Repository: BOBDSignalRepository** | `BOBDSignalRepository.java` | Signal CRUD operations |
| **Service: BOBDFailureService** | `BOBDFailureService.java` | Service interface |
| **Service Impl: BOBDFailureServiceImpl** | `BOBDFailureServiceImpl.java` | Core logic implementation |
| **Controller: BOBDFailureController** | `BOBDFailureController.java` | REST API endpoints |

### Frontend Components

| Component | File | Description |
|-----------|------|-------------|
| **BOBD Component** | `bobd-failures.component.ts` | Main UI component |
| **BOBD Service** | `bobd.service.ts` | API client service |
| **BOBD Models** | `bobd.model.ts` | TypeScript interfaces |

---

## Signal Detection Logic

### Detection Algorithm

```java
// Simplified detection flow
1. Fetch recent candles (5-min or 15-min timeframe)
2. Load key levels (PDH, PDL, ORH, ORL)
3. For each key level:
   a. Check if price broke the level (0.2-0.5% beyond)
   b. Track break price and maximum extension
   c. Monitor if price reclaims level within 2-5 candles
   d. Detect confirmation candle pattern (Engulfing, Hammer, etc.)
   e. Calculate signal confidence
4. If valid fakeout detected:
   a. Create BOBDSignal entity
   b. Calculate entry, stop-loss, and targets
   c. Save to database
   d. Send Telegram notification (if enabled)
```

### Confirmation Candle Patterns

| Pattern | Description | Signal Strength |
|---------|-------------|-----------------|
| **BEARISH_ENGULFING** | Current candle engulfs previous bullish candle | +15% confidence |
| **BULLISH_ENGULFING** | Current candle engulfs previous bearish candle | +15% confidence |
| **BEARISH_MARUBOZU** | Strong bearish candle with minimal wicks | +12% confidence |
| **BULLISH_MARUBOZU** | Strong bullish candle with minimal wicks | +12% confidence |
| **SHOOTING_STAR** | Small body with long upper wick | +10% confidence |
| **HAMMER** | Small body with long lower wick | +10% confidence |

---

## Configuration Options

### BOBDConfiguration Entity Fields

#### Timeframe Settings
```java
primaryTimeframe: "5min"           // Main analysis timeframe
confirmationTimeframe: "15min"     // Higher TF for confirmation
```

#### Key Level Settings
```java
usePriorDayHigh: true
usePriorDayLow: true
useOpeningRangeHigh: true
useOpeningRangeLow: true
openingRangeMinutes: 15            // 15 or 30 minutes
```

#### Breakout Threshold Settings
```java
minBreakPoints: 50.0               // Minimum points for valid break
maxBreakPoints: 100.0              // Maximum points (0.2-0.5%)
minBreakPercent: 0.2               // Minimum percentage
maxBreakPercent: 0.5               // Maximum percentage
```

#### Reclaim Settings
```java
minCandlesToReclaim: 2             // Minimum candles to reclaim
maxCandlesToReclaim: 5             // Maximum candles allowed
requireConfirmationCandle: true    // Require pattern confirmation
acceptedPatterns: "ENGULFING,HAMMER,MARUBOZU"
```

#### Risk Management
```java
riskProfile: "CONSERVATIVE"        // CONSERVATIVE, MODERATE, AGGRESSIVE
maxRiskPercent: 1.0                // Maximum 1% account risk
stopLossType: "FIXED_POINTS"       // FIXED_POINTS, LEVEL_BASED, ATR_BASED
stopLossPoints: 25.0               // Stop loss in points
stopLossBufferPoints: 10.0         // Buffer above/below level
```

#### Target Settings
```java
target1RrRatio: 2.0                // 1:2 Risk-Reward for Target 1
target2RrRatio: 3.0                // 1:3 Risk-Reward for Target 2
partialExitAtTarget1Percent: 50.0  // Exit 50% at Target 1
partialExitAtTarget2Percent: 25.0  // Exit 25% at Target 2
```

#### Time Filters
```java
earliestEntryTime: "09:45"         // Avoid opening volatility
latestEntryTime: "14:30"           // Allow time for targets
forceExitTime: "15:15"             // Force exit before close
avoidFirstCandle: true
avoidLast30Min: true
```

#### Event Day Filters
```java
avoidExpiryDay: true               // Skip weekly expiry (Tuesday)
avoidRbiPolicyDay: true
avoidBudgetDay: true
avoidHighVix: true
maxVixThreshold: 20.0              // Maximum VIX for trading
```

---

## Trade Management

### Signal Lifecycle

```
DETECTED → CONFIRMED → ENTRY_TRIGGERED → ACTIVE → CLOSED
    │          │              │             │         │
    │          │              │             │         ├── TARGET_HIT
    │          │              │             │         ├── SL_HIT
    │          │              │             │         ├── TIME_EXIT
    │          │              │             │         └── MANUAL
    │          │              │             │
    └── CANCELLED (invalid signal)
               └── EXPIRED (not triggered in time)
```

### Position Sizing

```java
// Conservative: 1% account risk
riskAmount = accountSize * (riskPercent / 100.0)
slPoints = |entryPrice - stopLoss|
lots = riskAmount / slPoints
lots = min(lots, maxLotsPerTrade)  // Apply lot limit
```

---

## Telegram Notifications

### Toggle Control

BOBD Failure Telegram alerts can be enabled/disabled from:
- **Settings Tab → Telegram Notifications → Trade Alerts → BOBD Failure Alerts**

### Notification Types

1. **Signal Detection** - When new fakeout signal is detected
2. **Entry Trigger** - When entry conditions are met
3. **Exit Notification** - When trade is closed (target/SL/time)
4. **Daily Summary** - End of day performance summary
5. **MTF Analysis** - Multi-timeframe alignment alerts
6. **Greeks Update** - Options Greeks analysis alerts
7. **Price Level Alerts** - When approaching key levels

### Sample Notification Format

```
🔔 *BOBD Signal Detected*

📊 Signal Type: *BULLISH_FAKEOUT*
📍 Level: *PDH* @ 23450.00
💰 Entry: 23420.00
🎯 Target 1: 23340.00 (1:2 RR)
🛑 Stop Loss: 23480.00
📈 Confidence: 75%

⏰ Time: 10:35:22
```

---

## Multi-Timeframe Analysis

### Analysis Logic

The system analyzes three timeframes for trend alignment:

| Timeframe | Purpose | Weight |
|-----------|---------|--------|
| 5-minute | Entry timing | Short-term |
| 15-minute | Trend confirmation | Medium-term |
| 1-hour | Overall direction | Major trend |

### Indicators Used

- **EMA 9, 21, 50** - Trend direction
- **RSI (14)** - Momentum confirmation
- **MACD** - Trend strength

### Confidence Calculation

```java
if (trend5min == trend15min) confidence += 40
if (trend5min == trend1hour) confidence += 30
if (trend15min == trend1hour) confidence += 30
// Maximum: 100%
```

---

## Options Greeks Integration

### Calculated Greeks

| Greek | Description | Use |
|-------|-------------|-----|
| **Delta** | Price sensitivity | Direction exposure |
| **Gamma** | Delta change rate | Position risk |
| **Theta** | Time decay | Time sensitivity |
| **Vega** | Volatility sensitivity | IV impact |
| **Rho** | Interest rate sensitivity | Cost of carry |

### Optimal Strike Selection

The system recommends strikes based on:
1. **ATM Strike** - Highest delta, most responsive
2. **OTM 1** - Balance of cost and delta
3. **OTM 2** - Lower cost, lower probability

---

## Backtesting

### Backtest Parameters

```java
instrumentToken: 256265           // Nifty 50
startDate: "2025-01-01"
endDate: "2025-01-24"
configName: "DEFAULT_CONSERVATIVE"
```

### Metrics Calculated

- **Total Signals** - Number of signals detected
- **Valid Signals** - Signals passing all filters
- **Total Trades** - Trades actually taken
- **Win Rate** - Percentage of winning trades
- **Profit Factor** - Gross profit / Gross loss
- **Net Profit** - Total profit/loss in points
- **Max Drawdown** - Maximum peak-to-trough decline

---

## Performance Analytics

### Available Metrics

```java
totalTrades: 45
winningTrades: 28
losingTrades: 17
winRate: 62.2%
totalPL: 1250.5 points
avgWin: 55.3 points
avgLoss: -28.7 points
expectancy: 27.8 points/trade
```

### Performance Breakdown

- **By Signal Type** - BULLISH_FAKEOUT vs BEARISH_FAKEOUT
- **By Level Type** - PDH, PDL, ORH, ORL performance
- **By Hour** - Best trading hours
- **Equity Curve** - Cumulative P&L over time

---

## API Endpoints

### Signal Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/bobd/scan/{instrumentToken}` | Scan for new signals |
| GET | `/api/bobd/signals/active` | Get active signals |
| GET | `/api/bobd/signals/today` | Get today's signals |
| GET | `/api/bobd/signals/{id}` | Get signal by ID |
| POST | `/api/bobd/signals/{id}/confirm` | Confirm signal |
| POST | `/api/bobd/signals/{id}/cancel` | Cancel signal |
| POST | `/api/bobd/signals/{id}/close` | Close trade |

### Key Levels

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/bobd/levels/{instrumentToken}` | Get all key levels |
| POST | `/api/bobd/levels/{instrumentToken}/update` | Refresh levels |

### Configuration

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/bobd/config/active` | Get active config |
| GET | `/api/bobd/configs` | Get all configs |
| POST | `/api/bobd/config` | Save config |
| PUT | `/api/bobd/config/{id}/activate` | Activate config |

### Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/bobd/dashboard` | Get dashboard data |
| GET | `/api/bobd/performance` | Get performance stats |
| POST | `/api/bobd/backtest` | Run backtest |

---

## Database Schema

### bobd_signals Table

```sql
CREATE TABLE bobd_signals (
    id BIGSERIAL PRIMARY KEY,
    instrument_token BIGINT NOT NULL,
    instrument_name VARCHAR(50),
    signal_date TIMESTAMP NOT NULL,
    detection_timestamp TIMESTAMP NOT NULL,
    
    -- Key Level Details
    level_type VARCHAR(20) NOT NULL,  -- PDH, PDL, ORH_15, ORL_15, etc.
    key_level DECIMAL(12,2) NOT NULL,
    
    -- Breakout Details
    signal_type VARCHAR(30) NOT NULL,  -- BULLISH_FAKEOUT, BEARISH_FAKEOUT
    break_price DECIMAL(12,2),
    break_points DECIMAL(8,2),
    break_percent DECIMAL(5,2),
    
    -- Reclaim Details
    reclaim_price DECIMAL(12,2),
    candles_to_reclaim INTEGER,
    reclaim_candle_pattern VARCHAR(30),
    
    -- Trade Setup
    option_type VARCHAR(2),  -- CE, PE
    entry_price DECIMAL(12,2),
    stop_loss DECIMAL(12,2),
    target_1 DECIMAL(12,2),
    target_2 DECIMAL(12,2),
    
    -- Status
    status VARCHAR(20) NOT NULL,
    signal_confidence DECIMAL(5,2),
    
    -- Outcome
    profit_loss_points DECIMAL(10,2),
    profit_loss_percent DECIMAL(8,2),
    
    -- Timestamps
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_bobd_instrument_date ON bobd_signals(instrument_token, signal_date);
CREATE INDEX idx_bobd_status ON bobd_signals(status);
```

---

## Known Issues & Fixes

### Issue 1: Spring Query Validation Error
**Problem:** `findTodaysSignalsByInstrument` query failing
**Solution:** Fixed JPQL query syntax for date comparison

### Issue 2: Telegram Notifications Not Respecting Toggle
**Problem:** BOBD notifications sent regardless of setting
**Solution:** Added `isAlertTypeEnabled("TRADE", "BOBD_FAILURE")` check to all notification methods

### Issue 3: Type Safety in Notification Methods
**Problem:** String.format errors with Object types
**Solution:** Added proper type casting for Map values

### Issue 4: Incomplete Detection Logic
**Problem:** `detectBreakoutFailure` returning null always
**Solution:** Implemented complete detection algorithm with pattern recognition

---

## Enhancement Suggestions

### Must-Have Enhancements (IMPLEMENTED ✅)

#### 1. **Real-Time VIX Integration** ✅ IMPLEMENTED
```
Status: COMPLETED
Description: Fetch real-time India VIX from KiteTickerProvider
Implementation:
- Uses kiteTickerProvider.vixLastPrice for live VIX data
- VIX history tracking for trend analysis (last 10 values)
- VIX trend detection (RISING, FALLING, STABLE)
- VIX-based trading recommendations
- Updated isVixAcceptable() to use real-time data

API Endpoints:
- GET /api/bobd/vix/current - Get current VIX value
- GET /api/bobd/vix/analysis - Get full VIX analysis with trend
```

#### 2. **Event Calendar Integration** ✅ IMPLEMENTED
```
Status: COMPLETED
Description: Automatic trading pauses for RBI policy, expiry, budget
Implementation:
- New MarketEventCalendar entity for storing events
- MarketEventCalendarRepository for event queries
- Automatic weekly expiry event generation
- Event-based trading status checks
- Updated isEventDay() to use calendar

Event Types Supported:
- RBI_POLICY: RBI monetary policy announcements
- BUDGET: Union Budget presentation
- EXPIRY: Weekly/Monthly F&O expiry
- FED_POLICY: US Fed policy
- ECONOMIC_DATA: Major economic data releases
- HOLIDAY: Market holidays

API Endpoints:
- GET /api/bobd/events/today - Today's events
- GET /api/bobd/events/upcoming?days=7 - Upcoming events
- GET /api/bobd/events/trading-status - Trading status
- POST /api/bobd/events - Add new event
- POST /api/bobd/events/generate-expiry/{year} - Auto-generate expiry events
```

#### 3. **Automated Paper Trading** ✅ IMPLEMENTED
```
Status: COMPLETED
Description: Simulated execution with P&L tracking
Implementation:
- BOBDPaperTrade entity for virtual trades
- BOBDPaperTradeRepository for trade queries
- Simulated order execution with slippage
- Real-time P&L tracking from live prices
- SL/Target monitoring with auto-exit
- Performance statistics and equity curve

Features:
- Entry slippage modeling
- Exit slippage modeling
- Brokerage calculation
- Greeks tracking at entry
- VIX tracking at entry/exit
- OI tracking

API Endpoints:
- POST /api/bobd/paper-trade/execute/{signalId} - Execute paper trade
- GET /api/bobd/paper-trade/open - Open trades
- GET /api/bobd/paper-trade/today - Today's trades
- POST /api/bobd/paper-trade/{tradeId}/close - Close trade
- GET /api/bobd/paper-trade/stats - Performance stats
- GET /api/bobd/paper-trade/equity-curve - Equity curve
- POST /api/bobd/paper-trade/process - Update all open trades
```

#### 4. **Options Chain Integration** ✅ IMPLEMENTED
```
Status: COMPLETED
Description: Live premium and OI data for strike selection
Implementation:
- Real-time option premium from tickerMapForJob
- Live OI data from Kite ticker
- Put-Call Ratio calculation
- Max Pain strike calculation
- Optimal strike finder with scoring
- IV surface placeholder

Features:
- Live LTP, OI, Volume for options
- Bid-Ask spread from depth
- PCR from 11 strikes around ATM
- Max Pain calculation
- Strike scoring based on Delta, Premium, OI, Theta

API Endpoints:
- GET /api/bobd/options/chain/{instrumentToken} - Options chain
- GET /api/bobd/options/premium/{optionToken} - Live premium
- GET /api/bobd/options/oi/{optionToken} - OI data
- GET /api/bobd/options/pcr/{instrumentToken} - Put-Call Ratio
- GET /api/bobd/options/max-pain/{instrumentToken} - Max pain strike
- GET /api/bobd/options/optimal-strike/{signalId} - Find optimal strike
- POST /api/bobd/options/subscribe - Subscribe to option tokens
```

### Nice-to-Have Enhancements

#### 8. **Mobile Push Notifications**
```
Description: Add Firebase push notifications
Benefits: Immediate alerts even without Telegram
```

#### 9. **Trade Journal Export**
```
Description: Export trade history to Excel/PDF
Features: Detailed trade analysis with charts
```

#### 10. **Strategy Optimization**
```
Description: Parameter optimization using genetic algorithms
Features: Automatic parameter tuning based on historical performance
```

#### 11. **Correlation Analysis**
```
Description: Analyze correlation with Bank Nifty
Features: Dual-index signal confirmation
```

#### 12. **Risk Dashboard**
```
Description: Real-time risk monitoring
Features:
- Portfolio Greeks exposure
- Max loss scenarios
- Position size warnings
```

---

## Quick Start Guide

### 1. Enable BOBD Monitoring
```
1. Navigate to BOBD Failures tab
2. Configure your settings (or use DEFAULT_CONSERVATIVE)
3. Set instrument token (256265 for Nifty 50)
4. Click "Start Monitoring"
```

### 2. Enable Telegram Alerts
```
1. Go to Settings → Telegram Notifications
2. Enable Trade Alerts
3. Toggle ON "BOBD Failure Alerts"
4. Save settings
```

### 3. View Signals
```
1. Dashboard shows today's signals
2. Active signals highlighted in green
3. Click signal for detailed view
4. Confirm/Cancel signals as needed
```

### 4. Run Backtest
```
1. Go to Backtesting tab
2. Select date range
3. Choose configuration
4. Click "Run Backtest"
5. Review results
```

---

## Troubleshooting

### Signals Not Detecting
1. Check if within trading window (9:45 AM - 2:30 PM)
2. Verify key levels are loaded
3. Ensure candle data is available
4. Check configuration filters

### Telegram Not Sending
1. Verify bot token and chat ID
2. Check if alerts are enabled
3. Confirm BOBD Failure toggle is ON
4. Check rate limiting settings

### Performance Issues
1. Clear old signals (cleanupOldData)
2. Reset daily cache (dailyReset)
3. Check database indices
4. Monitor API rate limits

---

*Last Updated: January 25, 2026*
*Version: 1.0.0*
