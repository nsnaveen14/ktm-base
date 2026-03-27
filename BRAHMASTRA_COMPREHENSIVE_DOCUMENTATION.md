# Brahmastra Triple Confirmation Trading Strategy
## Comprehensive Technical and Implementation Documentation

**Version:** 2.2  
**Last Updated:** February 22, 2026  
**Module:** KTManager Trading Platform

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Strategy Overview](#2-strategy-overview)
3. [Technical Indicators](#3-technical-indicators)
4. [Signal Generation Logic](#4-signal-generation-logic)
5. [Option Chain Integration](#5-option-chain-integration) *(NEW)*
6. [Backend Architecture](#6-backend-architecture)
7. [Frontend Architecture](#7-frontend-architecture)
8. [API Reference](#8-api-reference)
9. [Data Flow Architecture](#9-data-flow-architecture)
10. [Database Schema](#10-database-schema)
11. [Configuration Parameters](#11-configuration-parameters)
12. [Backtesting Engine](#12-backtesting-engine)
13. [Real-time Integration](#13-real-time-integration)
14. [Enhancement Suggestions](#14-enhancement-suggestions)
15. [Appendix](#15-appendix)

---

## 1. Executive Summary

**Brahmastra** is a sophisticated intraday trading strategy implemented within the KTManager platform. It employs a **Triple Confirmation** approach combining three powerful technical indicators to generate high-probability trading signals for Indian equity indices (NIFTY, BANKNIFTY, SENSEX).

### Key Features
- **Triple Confirmation Signals**: Requires alignment of Supertrend, MACD, and VWAP
- **PCR (Put-Call Ratio) Filtering**: Optional market bias confirmation
- **Option Chain Integration**: Max Pain, OI Analysis, and Gamma Exposure for signal confirmation *(NEW)*
- **Real-time Scanning**: Live signal detection via WebSocket integration
- **Comprehensive Backtesting**: Historical performance analysis with detailed metrics
- **Multi-timeframe Support**: 1m, 3m, 5m, 15m, 30m, 1h, daily
- **Risk Management**: Automated stop-loss, targets, and position sizing

### Performance Metrics Tracked
- Win Rate, Profit Factor, Sharpe Ratio
- Maximum Drawdown, Sortino Ratio, Calmar Ratio
- Risk-Reward Analysis, Expectancy

---

## 2. Strategy Overview

### 2.1 Core Philosophy

The Brahmastra strategy is based on the principle that **sustainable trading signals require multiple independent confirmations**. A trade is only executed when three different analytical approaches converge:

```
┌─────────────────────────────────────────────────────────────────┐
│                    BRAHMASTRA SIGNAL LOGIC                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐    │
│   │  SUPERTREND   │ + │     MACD      │ + │     VWAP      │    │
│   │   (Trend)     │   │  (Momentum)   │   │   (Value)     │    │
│   └───────┬───────┘   └───────┬───────┘   └───────┬───────┘    │
│           │                   │                   │             │
│           └───────────────────┼───────────────────┘             │
│                               │                                 │
│                    ┌──────────▼──────────┐                      │
│                    │  TRIPLE CONFIRMATION │                      │
│                    │   BUY / SELL Signal  │                      │
│                    └──────────┬──────────┘                      │
│                               │                                 │
│                    ┌──────────▼──────────┐                      │
│                    │    PCR FILTER       │  (Optional)          │
│                    │  (Market Bias)      │                      │
│                    └─────────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Signal Types

| Signal | Condition |
|--------|-----------|
| **BUY** | Supertrend Bullish + MACD Bullish Crossover + Price at/below VWAP (±0.2%) |
| **SELL** | Supertrend Bearish + MACD Bearish Crossover + Price at/above VWAP (±0.2%) |
| **NO_SIGNAL** | Triple confirmation not achieved |

### 2.3 Market Bias Filtering (PCR)

| PCR Value | Bias | Allowed Signals |
|-----------|------|-----------------|
| > 1.2 | BULLISH | Only BUY signals |
| < 0.8 | BEARISH | Only SELL signals |
| 0.8 - 1.2 | NEUTRAL | Both BUY and SELL |

---

## 3. Technical Indicators

### 3.1 Supertrend (ATR-based Trend Indicator)

**Current Configuration:**
- Period: **20** (previously 10)
- Multiplier: **2.0** (previously 3.0)

**Formula:**
```
ATR = Average True Range over Period
True Range = Max(High-Low, |High-PrevClose|, |Low-PrevClose|)

Basic Upper Band = (High + Low) / 2 + (Multiplier × ATR)
Basic Lower Band = (High + Low) / 2 - (Multiplier × ATR)

Final Upper Band = If Close > Prev Final Upper: Min(Basic Upper, Prev Final Upper) : Basic Upper
Final Lower Band = If Close < Prev Final Lower: Max(Basic Lower, Prev Final Lower) : Basic Lower

Trend = Close > Final Upper Band → BULLISH
        Close < Final Lower Band → BEARISH
```

**Implementation Location:** `BrahmastraServiceImpl.calculateSupertrend()`

### 3.2 MACD (Moving Average Convergence Divergence)

**Configuration:**
- Fast Period: **12**
- Slow Period: **26**
- Signal Period: **9**

**Formula:**
```
EMA Fast = 12-period Exponential Moving Average
EMA Slow = 26-period Exponential Moving Average

MACD Line = EMA Fast - EMA Slow
Signal Line = 9-period EMA of MACD Line
Histogram = MACD Line - Signal Line

BULLISH_CROSSOVER = Previous MACD ≤ Previous Signal AND Current MACD > Current Signal
BEARISH_CROSSOVER = Previous MACD ≥ Previous Signal AND Current MACD < Current Signal
```

**Implementation Location:** `BrahmastraServiceImpl.calculateMACD()`

### 3.3 VWAP (Volume Weighted Average Price)

**Configuration:**
- Tolerance: **0.2%** (±0.002)
- Reset: Daily at market open (9:15 IST)

**Formula:**
```
Typical Price = (High + Low + Close) / 3
VWAP = Cumulative(Typical Price × Volume) / Cumulative(Volume)

Resets at the start of each trading day.

Price Position:
  ABOVE: Price > VWAP × 1.002
  BELOW: Price < VWAP × 0.998
  AT_VWAP: Within ±0.2% of VWAP
```

**Implementation Location:** `BrahmastraServiceImpl.calculateVWAP()`

---

## 4. Signal Generation Logic

### 4.1 Triple Confirmation Check

```java
// BUY Signal Conditions
boolean buySignal = 
    supertrendTrend == BULLISH &&
    macdLine > macdSignal && 
    previousMacdLine <= previousMacdSignal &&  // Fresh crossover
    price <= vwap * (1 + tolerance);           // At or below VWAP

// SELL Signal Conditions
boolean sellSignal = 
    supertrendTrend == BEARISH &&
    macdLine < macdSignal && 
    previousMacdLine >= previousMacdSignal &&  // Fresh crossover
    price >= vwap * (1 - tolerance);           // At or above VWAP
```

### 4.2 Sideways Market Filter

To avoid whipsaws in ranging markets, the strategy tracks Supertrend trend flips:

```java
private boolean isSidewaysMarket(List<CandleData> candles, int currentIndex, 
                                  int lookback, int maxFlips) {
    // If more than 3 trend flips in the last 10 bars, market is sideways
    // Skip signal generation to avoid false entries
}
```

### 4.3 Entry, Stop Loss, and Target Calculation

| Position | Entry | Stop Loss | Target 1 (1:1 RR) | Target 2 (2:1 RR) |
|----------|-------|-----------|-------------------|-------------------|
| BUY | Current Close | Previous Low | Entry + (Entry - SL) | Entry + 2×(Entry - SL) |
| SELL | Current Close | Previous High | Entry - (SL - Entry) | Entry - 2×(SL - Entry) |

### 4.4 Confidence Score Calculation

The confidence score (0-100%) is calculated based on:

| Factor | Weight | Criteria |
|--------|--------|----------|
| Base Triple Confirmation | 60% | All three indicators aligned |
| MACD Histogram Strength | 10% | Larger histogram = higher confidence |
| Supertrend Distance | 10% | Greater distance from ST line = stronger trend |
| VWAP Proximity | 10% | Closer to VWAP = better value entry |
| Additional Factors | Up to 15% | Trend duration, PCR alignment |

**Maximum Score:** 95% (capped to maintain realistic expectations)

---

## 5. Option Chain Integration *(NEW in v2.1)*

The Option Chain Integration provides additional confirmation for Brahmastra signals by leveraging options-specific data including Max Pain analysis, Open Interest changes, and Gamma Exposure levels.

### 5.1 Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                   OPTION CHAIN INTEGRATION                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────┐   │
│  │  MAX PAIN   │   │ OI ANALYSIS │   │   GAMMA EXPOSURE (GEX)  │   │
│  │  (S/R Level)│   │ (Trend Conf)│   │   (Reversal Zones)      │   │
│  └──────┬──────┘   └──────┬──────┘   └───────────┬─────────────┘   │
│         │                 │                      │                  │
│         └─────────────────┼──────────────────────┘                  │
│                           │                                         │
│              ┌────────────▼────────────┐                            │
│              │  OPTION CHAIN SIGNAL    │                            │
│              │  (Confirms/Invalidates  │                            │
│              │   Triple Confirmation)  │                            │
│              └─────────────────────────┘                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Max Pain Analysis

Max Pain is the strike price at which option sellers (writers) have minimum loss. It acts as a gravitational pull for the underlying price, especially near expiry.

**Implementation:**

| Field | Description |
|-------|-------------|
| `maxPainStrike` | Primary Max Pain strike price |
| `maxPainSecondStrike` | Secondary Max Pain level |
| `distanceFromSpot` | Distance in points from current price |
| `priceRelation` | ABOVE_MAX_PAIN, BELOW_MAX_PAIN, AT_MAX_PAIN |
| `confirmsBullish` | True if price below Max Pain (tends to rise) |
| `confirmsBearish` | True if price above Max Pain (tends to fall) |
| `pullStrength` | 0-100, strength of Max Pain gravitational pull |

**Signal Logic:**
```java
// Price below Max Pain → Bullish bias (price tends to gravitate up)
if (distanceFromSpotPercent < -0.3) {
    confirmsBullish = true;
    actsAsResistance = true;
}

// Price above Max Pain → Bearish bias (price tends to gravitate down)
if (distanceFromSpotPercent > 0.3) {
    confirmsBearish = true;
    actsAsSupport = true;
}
```

### 5.3 OI (Open Interest) Analysis

OI changes provide insights into market participant positioning and trend strength.

**Key Metrics:**

| Metric | Description |
|--------|-------------|
| `pcr` | Put-Call Ratio |
| `oiBuildUpType` | LONG_BUILD_UP, SHORT_BUILD_UP, LONG_UNWINDING, SHORT_COVERING |
| `highestCallOIStrike` | Resistance level from call writers |
| `highestPutOIStrike` | Support level from put writers |
| `confirmsUptrend` | OI structure supports bullish move |
| `confirmsDowntrend` | OI structure supports bearish move |

**OI Build-up Interpretation:**

| Build-up Type | Price Action | OI Change | Market View |
|---------------|--------------|-----------|-------------|
| LONG_BUILD_UP | Rising | Rising | Bullish |
| SHORT_BUILD_UP | Falling | Rising | Bearish |
| LONG_UNWINDING | Falling | Falling | Bearish |
| SHORT_COVERING | Rising | Falling | Bullish |

### 5.4 Gamma Exposure (GEX) Analysis

GEX measures the sensitivity of market makers' delta hedging to price changes. It identifies reversal zones and trending environments.

**Key Concepts:**

| Metric | Description |
|--------|-------------|
| `netGEX` | Net Gamma Exposure (positive = mean-reverting, negative = trending) |
| `gexFlipLevel` | Price where GEX changes sign (critical pivot) |
| `callWallStrike` | Resistance from high call gamma |
| `putWallStrike` | Support from high put gamma |
| `marketRegime` | MEAN_REVERTING, TRENDING_BULLISH, TRENDING_BEARISH, VOLATILE |
| `isInReversalZone` | True if price near gamma walls in positive GEX |
| `reversalDirection` | Expected reversal direction (UP, DOWN, NONE) |

**Market Regime Detection:**
```java
if (isPositiveGEX) {
    // Mean-reverting environment
    // Price tends to stay within gamma walls
    // Fade moves at extremes
    return netGEX > HIGH_GEX_THRESHOLD ? "MEAN_REVERTING_STRONG" : "MEAN_REVERTING";
} else {
    // Trending environment
    // Breakouts more likely
    return spotPrice > maxGammaStrike ? "TRENDING_BULLISH" : "TRENDING_BEARISH";
}
```

### 5.5 Combined Option Chain Signal

The option chain signal is calculated by combining all three components:

**Scoring System:**

| Component | Max Points | Conditions |
|-----------|------------|------------|
| Max Pain | 2 | Confirms bullish/bearish bias |
| OI Analysis | 3 | OI build-up type + PCR signal |
| GEX Signal | 3 | GEX signal + reversal zone |

**Signal Determination:**
```java
if (bullishScore >= 4 && bullishScore > bearishScore + 1) return "BUY";
if (bearishScore >= 4 && bearishScore > bullishScore + 1) return "SELL";
return "NO_SIGNAL";
```

### 5.6 Integration with Triple Confirmation

The option chain data can be used to:

1. **Confirm Signals**: Validate Brahmastra signals with option chain bias
2. **Filter Signals**: Skip signals that contradict option chain analysis
3. **Adjust Confidence**: Increase/decrease confidence based on option chain alignment
4. **Identify Reversal Zones**: Warn when entering trades near gamma walls

**Confirmation Logic:**
```java
public boolean doesOptionChainConfirmSignal(String symbol, String signalType) {
    OptionChainMetrics metrics = getOptionChainMetrics(symbol);
    
    // At least 2 out of 3 components should confirm
    int confirmCount = 0;
    if (maxPainConfirms) confirmCount++;
    if (oiConfirms) confirmCount++;
    if (gexConfirms) confirmCount++;
    
    return confirmCount >= 2;
}
```

### 5.7 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/brahmastra/option-chain/{symbol}` | GET | Get option chain metrics for a symbol |
| `/api/brahmastra/option-chain/all` | GET | Get option chain metrics for all symbols |
| `/api/brahmastra/option-chain/{symbol}/confirm/{signalType}` | GET | Check signal confirmation |

### 5.8 Frontend Display

The Dashboard tab now includes an **Option Chain Insights** section with:

- **Max Pain Card**: Max Pain strike, distance, pull strength, bullish/bearish confirmation
- **OI Analysis Card**: PCR, OI build-up type, highest OI strikes
- **Gamma Exposure Card**: Net GEX, flip level, walls, market regime, reversal zones
- **Combined Signal Card**: Option chain signal, bias, confidence, recommendation

---

## 6. Backend Architecture

### 5.1 Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Java 17+ |
| Framework | Spring Boot 3.x |
| Build Tool | Gradle |
| Database | (Configured via application.properties) |
| Cache | ConcurrentHashMap (in-memory) |
| API Documentation | OpenAPI/Swagger |

### 5.2 Package Structure

```
src/main/java/com/trading/kalyani/KTManager/
├── controller/
│   └── BrahmastraController.java        # REST API endpoints
├── service/
│   ├── BrahmastraService.java           # Service interface
│   └── serviceImpl/
│       └── BrahmastraServiceImpl.java   # Core implementation (2124 lines)
├── dto/brahmastra/
│   ├── SignalRequest.java               # Signal generation request
│   ├── SignalDTO.java                   # Signal data transfer object
│   ├── BacktestRequest.java             # Backtest configuration
│   ├── BacktestResult.java              # Backtest results
│   ├── DashboardSummary.java            # Dashboard aggregations
│   ├── SymbolSummary.java               # Per-symbol metrics
│   ├── LiveScanResult.java              # Real-time scan results
│   ├── IndicatorMetrics.java            # Indicator values & history
│   ├── TradeLog.java                    # Trade execution log
│   ├── EquityPoint.java                 # Equity curve point
│   └── DrawdownPoint.java               # Drawdown tracking
├── entity/
│   ├── BrahmastraSignal.java            # Persisted signals
│   └── BrahmastraBacktestResult.java    # Persisted backtest results
└── repository/
    ├── BrahmastraSignalRepository.java
    └── BrahmastraBacktestResultRepository.java
```

### 5.3 Service Layer Design

```java
@Service
public class BrahmastraServiceImpl implements BrahmastraService {
    
    // ===== Constants =====
    private static final int DEFAULT_SUPERTREND_PERIOD = 20;
    private static final double DEFAULT_SUPERTREND_MULTIPLIER = 2.0;
    private static final int DEFAULT_MACD_FAST_PERIOD = 12;
    private static final int DEFAULT_MACD_SLOW_PERIOD = 26;
    private static final int DEFAULT_MACD_SIGNAL_PERIOD = 9;
    private static final double DEFAULT_VWAP_TOLERANCE = 0.002;
    
    // ===== Caching =====
    private final Map<String, List<CandleData>> candleCache = new ConcurrentHashMap<>();
    private final Map<String, IndicatorState> indicatorCache = new ConcurrentHashMap<>();
    private final Map<String, BrahmastraSignal> lastSignalCache = new ConcurrentHashMap<>();
    
    // ===== Core Methods =====
    // - generateSignals(SignalRequest)
    // - runBacktest(BacktestRequest)
    // - scanSymbol(String, String)
    // - getIndicatorMetrics(String, String, int)
    // - processLiveTick(Long, Double, Integer)
}
```

### 5.4 Inner Classes

| Class | Purpose |
|-------|---------|
| `CandleData` | OHLCV data with calculated indicator values |
| `IndicatorState` | Cached indicator state for real-time updates |
| `BacktestEngine` | Simulation engine for historical backtesting |

---

## 6. Frontend Architecture

### 6.1 Technology Stack

| Component | Technology |
|-----------|------------|
| Framework | Angular 16+ |
| UI Library | Angular Material |
| Charts | Chart.js, Lightweight Charts |
| State Management | RxJS |
| Styling | SCSS/CSS |

### 6.2 Component Structure

```
ktm-ui/src/app/
├── components/
│   └── brahmastra/
│       ├── brahmastra.component.ts      # Main component (1050+ lines)
│       ├── brahmastra.component.html    # Template (1100+ lines)
│       └── brahmastra.component.css     # Styles (2000+ lines)
├── services/
│   └── brahmastra.service.ts            # API client service
└── models/
    └── brahmastra.model.ts              # TypeScript interfaces (480+ lines)
```

### 6.3 Component Features

#### Tab Layout
1. **Dashboard** - Overview with indicator metrics, charts, and active signals
2. **Signal Generator** - Manual signal generation with custom parameters
3. **Backtest** - Historical strategy testing with detailed metrics
4. **Live Scanner** - Real-time multi-symbol scanning

#### Dashboard Elements
- **Summary Cards**: Today's Performance, Active Signals, Strategy Health, Market Bias
- **Indicator Metrics**: Supertrend, MACD, VWAP cards with real-time values
- **Charts**: Price with Supertrend, MACD histogram, VWAP bands
- **Interactive Signal Chart**: Candlestick chart with annotations (NEW in v2.2)
- **Option Chain Insights**: Max Pain, OI Analysis, GEX data
- **Symbol Selector**: Quick switch between NIFTY, BANKNIFTY, SENSEX

### 6.4 Chart Configuration

```typescript
// Price & Supertrend Chart (Chart.js)
renderPriceSupertrendChart() {
    datasets: [
        { label: 'Price', borderColor: '#ff9800' },
        { label: 'Supertrend', borderColor: dynamic based on trend }
    ]
}

// MACD Chart (Combined bar + line)
renderMACDChart() {
    datasets: [
        { type: 'bar', label: 'Histogram', backgroundColor: conditionalColor },
        { type: 'line', label: 'MACD Line', borderColor: '#00d4ff' },
        { type: 'line', label: 'Signal Line', borderColor: '#ff9800' }
    ]
}

// VWAP Chart with Bands
renderVWAPChart() {
    datasets: [
        { label: 'Price', borderColor: '#ff9800' },
        { label: 'VWAP', borderColor: '#9c27b0' },
        { label: 'Upper Band', borderDash: [5, 5] },
        { label: 'Lower Band', fill: true }
    ]
}
```

### 6.5 Interactive Signal Chart (Lightweight Charts) *(NEW in v2.2)*

The Interactive Signal Chart provides professional-grade chart annotations with visual signal markers and trade levels.

#### Features

| Feature | Description |
|---------|-------------|
| Candlestick Display | OHLC data with proper coloring |
| Supertrend Overlay | Dynamic color line (green=bullish, red=bearish) |
| VWAP Line | Dotted purple line for value reference |
| Signal Markers | Up/down arrows for BUY/SELL signals |
| Trade Level Lines | Entry, Stop Loss, Target 1/2/3, LTP |
| Toggle Controls | Show/hide signals and trade levels |
| Symbol Selector | Switch between NIFTY, BANKNIFTY, SENSEX |

#### Implementation

```typescript
// Initialize Lightweight Charts
private initSignalChart(): void {
    this.signalChart = createChart(container, {
        layout: { background: { color: '#1e222d' }, textColor: '#d1d4dc' },
        crosshair: { mode: CrosshairMode.Normal },
        // ... chart options
    });

    // Add candlestick series
    this.candleSeries = this.signalChart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350'
    });

    // Add indicator overlays
    this.supertrendLine = this.signalChart.addLineSeries({...});
    this.vwapLine = this.signalChart.addLineSeries({...});
}
```

#### Signal Markers

```typescript
// BUY Signal (Triple Confirmation)
{
    time: timestamp,
    position: 'belowBar',
    color: '#00ff88',
    shape: 'arrowUp',
    text: 'BUY',
    size: 2
}

// SELL Signal (Triple Confirmation)
{
    time: timestamp,
    position: 'aboveBar',
    color: '#ff4444',
    shape: 'arrowDown',
    text: 'SELL',
    size: 2
}

// Supertrend Trend Change
{
    time: timestamp,
    position: trend === 'BULLISH' ? 'belowBar' : 'aboveBar',
    color: trend === 'BULLISH' ? '#4CAF50' : '#F44336',
    shape: 'circle',
    text: 'ST',
    size: 1
}
```

#### Trade Level Annotations

| Level | Color | Style | Description |
|-------|-------|-------|-------------|
| Entry | #2196F3 (Blue) | Solid | Entry price |
| Stop Loss | #F44336 (Red) | Dashed | Stop loss level |
| Target 1 | #4CAF50 (Green) | Dashed | First target |
| Target 2 | #8BC34A (Light Green) | Dashed | Second target |
| Target 3 | #CDDC39 (Yellow-Green) | Dashed | Third target |
| LTP | #FF9800 (Orange) | Solid | Last traded price |

#### Active Signal Info Panel

Displays detailed information for the active signal:
- Signal Type (BUY/SELL)
- Entry Price
- Stop Loss
- Target 1, 2, 3
- Risk:Reward Ratio
- Confidence Score
- Signal Time

---

## 7. API Reference

### 7.1 Base URL
```
/api/brahmastra
```

### 7.2 Endpoints

#### Signal Generation

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/signals/generate` | Generate signals for date range |
| POST | `/signals/generate-async` | Async signal generation |

**Request Body (SignalRequest):**
```json
{
    "symbol": "NIFTY",
    "timeframe": "5m",
    "fromDate": "2026-02-01",
    "toDate": "2026-02-21",
    "usePCR": true,
    "supertrendPeriod": 20,
    "supertrendMultiplier": 2.0,
    "macdFastPeriod": 12,
    "macdSlowPeriod": 26,
    "macdSignalPeriod": 9,
    "vwapTolerance": 0.002
}
```

#### Backtesting

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/backtest/run` | Run backtest simulation |
| POST | `/backtest/run-async` | Async backtest |

**Request Body (BacktestRequest):**
```json
{
    "symbol": "NIFTY",
    "timeframe": "5m",
    "fromDate": "2026-01-01",
    "toDate": "2026-02-21",
    "initialCapital": 100000,
    "riskPerTrade": 1.0,
    "usePCR": true
}
```

#### Live Scanning

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/scan/live?symbols=NIFTY,BANKNIFTY,SENSEX` | Scan multiple symbols |
| GET | `/scan/{symbol}?timeframe=5m` | Scan single symbol |

#### Dashboard

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/dashboard/summary` | Aggregated dashboard |
| GET | `/dashboard/symbol/{symbol}` | Symbol-specific summary |

#### Signal Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/signals/active` | Get active signals |
| GET | `/signals/history?symbol=&fromDate=&toDate=` | Signal history |
| GET | `/signals/{id}` | Get signal by ID |
| PUT | `/signals/{id}/status` | Update signal status |

#### Indicator Metrics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/indicators/{symbol}?timeframe=5m&historyBars=50` | Symbol metrics |
| GET | `/indicators/all?timeframe=5m&historyBars=50` | All symbols metrics |

#### PCR

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pcr/current` | Current PCR values |

#### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Service health status |

---

## 8. Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA FLOW DIAGRAM                                │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Kite API    │────▶│  Historical  │────▶│  Candle      │
│  (Market     │     │  Data API    │     │  Repository  │
│   Data)      │     └──────────────┘     └──────┬───────┘
└──────────────┘                                  │
       │                                          ▼
       │                              ┌───────────────────┐
       │                              │  CandleData List  │
       │                              │  (In Memory)      │
       │                              └─────────┬─────────┘
       │                                        │
       │                              ┌─────────▼─────────┐
       ▼                              │   INDICATOR       │
┌──────────────┐                      │   CALCULATIONS    │
│  Live Tick   │                      │  • Supertrend     │
│  WebSocket   │                      │  • MACD           │
│  Feed        │                      │  • VWAP           │
└──────┬───────┘                      └─────────┬─────────┘
       │                                        │
       │                              ┌─────────▼─────────┐
       │                              │  SIGNAL LOGIC     │
       └─────────────────────────────▶│  Triple Check     │
                                      │  PCR Filter       │
                                      │  Sideways Filter  │
                                      └─────────┬─────────┘
                                                │
                    ┌───────────────────────────┼───────────────────────┐
                    │                           │                       │
                    ▼                           ▼                       ▼
            ┌──────────────┐           ┌──────────────┐        ┌──────────────┐
            │  Database    │           │  WebSocket   │        │  Telegram    │
            │  (Persist)   │           │  (Push)      │        │  (Alert)     │
            └──────────────┘           └──────────────┘        └──────────────┘
                    │                           │
                    └───────────────┬───────────┘
                                    │
                                    ▼
                           ┌──────────────┐
                           │  Angular UI  │
                           │  Dashboard   │
                           └──────────────┘
```

---

## 9. Database Schema

### 9.1 BrahmastraSignal Entity

```java
@Entity
public class BrahmastraSignal {
    @Id @GeneratedValue
    private Long id;
    
    // Identification
    private Long instrumentToken;
    private String symbol;
    private String timeframe;
    
    // Signal Details
    private String signalType;       // BUY, SELL
    private LocalDateTime signalTime;
    private Double entryPrice;
    private Double stopLoss;
    private Double target1, target2, target3;
    private Double riskRewardRatio;
    private Double confidenceScore;
    
    // Indicator Values at Signal
    private Double supertrendValue;
    private String supertrendTrend;
    private Double macdLine;
    private Double macdSignalLine;
    private Double vwapValue;
    private Double priceToVwapPercent;
    private Double pcrValue;
    private String pcrBias;
    private Boolean pcrFilterApplied;
    
    // Candle Data
    private Double candleOpen, candleHigh, candleLow, candleClose;
    
    // Trade Management
    private String status;           // ACTIVE, CLOSED, STOPPED_OUT, TARGET_HIT
    private LocalDateTime exitTime;
    private Double exitPrice;
    private String exitReason;
    private Double pnl;
    private Double pnlPercent;
    
    // Metadata
    private Integer appJobConfigNum;
}
```

### 9.2 BrahmastraBacktestResult Entity

```java
@Entity
public class BrahmastraBacktestResult {
    @Id @GeneratedValue
    private Long id;
    
    // Configuration
    private String symbol;
    private String timeframe;
    private LocalDateTime fromDate, toDate;
    private Double initialCapital;
    private Double riskPerTrade;
    private Boolean usePCR;
    
    // Results
    private Double finalCapital;
    private Double netPnL, netPnLPercent;
    private Integer totalTrades, winningTrades, losingTrades;
    private Double winRate;
    private Double averageWin, averageLoss;
    private Double profitFactor, expectancy;
    private Double averageRR;
    
    // Risk Metrics
    private Double maxDrawdown, maxDrawdownPercent;
    private Double sharpeRatio, sortinoRatio, calmarRatio;
    private Double volatility;
    
    // JSON Storage
    @Lob
    private String tradeLogJson;
    @Lob
    private String equityCurveJson;
}
```

---

## 10. Configuration Parameters

### 10.1 Default Indicator Settings

```java
// BrahmastraServiceImpl.java
private static final int DEFAULT_SUPERTREND_PERIOD = 20;
private static final double DEFAULT_SUPERTREND_MULTIPLIER = 2.0;
private static final int DEFAULT_MACD_FAST_PERIOD = 12;
private static final int DEFAULT_MACD_SLOW_PERIOD = 26;
private static final int DEFAULT_MACD_SIGNAL_PERIOD = 9;
private static final double DEFAULT_VWAP_TOLERANCE = 0.002;
```

### 10.2 PCR Thresholds

```java
private static final Double PCR_BULLISH_THRESHOLD = 1.2;
private static final Double PCR_BEARISH_THRESHOLD = 0.8;
```

### 10.3 Supported Symbols

```java
private static final Map<String, Long> SYMBOL_TOKEN_MAP = Map.of(
    "NIFTY", NIFTY_INSTRUMENT_TOKEN,
    "BANKNIFTY", BANK_NIFTY_INSTRUMENT_TOKEN,
    "SENSEX", SENSEX_INSTRUMENT_TOKEN
);
```

### 10.4 Timeframe Support

| UI Value | Kite API Interval |
|----------|-------------------|
| 1m | minute |
| 3m | 3minute |
| 5m | 5minute |
| 10m | 10minute |
| 15m | 15minute |
| 30m | 30minute |
| 1h | 60minute |
| 1d | day |

---

## 11. Backtesting Engine

### 11.1 Simulation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                  BACKTEST ENGINE FLOW                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. INITIALIZATION                                          │
│     ├── Load historical candles                             │
│     ├── Calculate indicators                                │
│     └── Set initial capital                                 │
│                                                             │
│  2. ITERATION (for each candle)                             │
│     ├── If IN POSITION:                                     │
│     │   ├── Check Stop Loss hit                             │
│     │   ├── Check Target hit                                │
│     │   ├── Check Signal Reversal                           │
│     │   ├── Check EOD (3:25 PM)                             │
│     │   └── Exit if condition met                           │
│     │                                                       │
│     └── If NOT IN POSITION:                                 │
│         ├── Check Triple Confirmation                       │
│         ├── Apply PCR Filter                                │
│         └── Enter if signal valid                           │
│                                                             │
│  3. RECORDING                                               │
│     ├── Calculate P&L                                       │
│     ├── Update equity curve                                 │
│     ├── Track drawdown                                      │
│     └── Log trade details                                   │
│                                                             │
│  4. FINAL METRICS                                           │
│     ├── Win Rate                                            │
│     ├── Sharpe Ratio = (Avg Return × √252) / Std Dev        │
│     ├── Sortino Ratio = Sharpe using downside deviation     │
│     ├── Calmar Ratio = Annual Return / Max Drawdown         │
│     └── Profit Factor = Total Wins / Total Losses           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 11.2 Exit Conditions

| Exit Type | Condition | Priority |
|-----------|-----------|----------|
| STOP_LOSS | Price touches stop loss level | 1 |
| TARGET | Price touches target level | 2 |
| SIGNAL_REVERSAL | Opposite triple confirmation | 3 |
| EOD | Time ≥ 15:25 IST | 4 |

### 11.3 Position Sizing

```java
double riskAmount = equity × (riskPerTrade / 100);
double positionSize = riskAmount / |entryPrice - stopLoss|;
double tradePnL = pnl × positionSize;
```

---

## 12. Real-time Integration

### 12.1 Live Tick Processing

```java
@Override
public void processLiveTick(Long instrumentToken, Double ltp, Integer appJobConfigNum) {
    // 1. Update candle cache
    updateCandleCache(instrumentToken, ltp, LocalDateTime.now());
    
    // 2. Scan for signals
    LiveScanResult result = scanSymbol(symbol, timeframe);
    
    // 3. If new signal detected
    if (!SIGNAL_NONE.equals(result.getSignalType()) && result.getIsNewSignal()) {
        // Save to database
        signalRepository.save(signal);
        
        // Push via WebSocket
        messagingService.sendBrahmastraSignal(result);
        
        // Send Telegram alert
        sendTelegramAlert(result);
    }
}
```

### 12.2 WebSocket Message Format

```json
{
    "symbol": "NIFTY",
    "signalType": "BUY",
    "entryPrice": 22450.50,
    "stopLoss": 22380.00,
    "target1": 22521.00,
    "target2": 22591.50,
    "confidenceScore": 78.5,
    "supertrendStatus": "BULLISH",
    "macdStatus": "BULLISH",
    "vwapStatus": "BELOW",
    "pcrBias": "NEUTRAL",
    "isNewSignal": true,
    "alertLevel": "HIGH"
}
```

### 12.3 Telegram Alert Format

```
🎯 *BRAHMASTRA SIGNAL*

Symbol: NIFTY
Signal: BUY
Entry: 22450.50
Stop Loss: 22380.00
Target 1: 22521.00
Target 2: 22591.50
Confidence: 78.5%

📊 Indicators:
• Supertrend: BULLISH
• MACD: BULLISH
• VWAP: BELOW
• PCR Bias: NEUTRAL
```

### 12.4 Telegram Alert Configuration

Brahmastra alerts are integrated with the centralized Telegram notification system. Alerts are only sent if:

1. **Global Telegram notifications are enabled** (`Settings > Telegram > Enable Telegram Notifications`)
2. **Trade Alerts category is enabled** (`Settings > Telegram > Trade Alerts > Enable Trade Alerts`)
3. **Brahmastra Alerts are enabled** (`Settings > Telegram > Trade Alerts > Brahmastra Alerts`)

#### Backend Implementation

```java
// BrahmastraServiceImpl.java
private void sendTelegramAlert(LiveScanResult result) {
    // Check if Brahmastra alerts are enabled in telegram settings
    if (!telegramNotificationService.isAlertTypeEnabled("TRADE", "BRAHMASTRA")) {
        logger.debug("Brahmastra telegram alerts are disabled, skipping notification");
        return;
    }
    
    // Send notification
    telegramNotificationService.sendMessage("Brahmastra Signal", message);
}
```

#### Database Schema

```sql
-- TelegramSettingsEntity field
brahmastra_alerts_enabled BOOLEAN DEFAULT TRUE
```

#### Frontend Configuration

Navigate to **Settings > Telegram > Trade Alerts** and toggle the **Brahmastra Alerts** switch to enable/disable notifications.

---

## 13. Enhancement Suggestions

### 13.1 High Priority Enhancements

#### 1. **Machine Learning Signal Validation**
```
Problem: False signals in volatile markets
Solution: Train ML model on historical signal outcomes
Implementation:
  - Feature engineering from indicator values
  - Random Forest or XGBoost classifier
  - Confidence score adjustment based on ML prediction
```

#### 2. **Adaptive Indicator Parameters**
```
Problem: Fixed parameters may not suit all market conditions
Solution: Dynamic parameter adjustment based on volatility
Implementation:
  - ATR-based Supertrend multiplier scaling
  - Market regime detection (trending vs. ranging)
  - Auto-tune MACD periods based on market characteristics
```

#### 3. **Multi-Timeframe Confirmation**
```
Problem: Single timeframe signals can be noise
Solution: Confirm signals across multiple timeframes
Implementation:
  - 5m signal + 15m trend alignment
  - Higher timeframe trend filter
  - Score boost for multi-TF confirmation
```

#### 4. **Options Chain Integration** ✅ IMPLEMENTED (v2.1)
```
Problem: Missing options-specific insights
Solution: Integrate options Greeks and OI data
Implementation:
  - Max Pain level as support/resistance ✅
  - OI change analysis for trend confirmation ✅
  - Gamma exposure levels for reversal zones ✅

Status: Fully implemented in BrahmastraServiceImpl with:
  - OptionChainMetrics DTO
  - Max Pain analysis from IndexLTP
  - OI analysis from MiniDelta
  - GEX integration from GammaExposureService
  - Frontend display in Dashboard tab
  - API endpoints: /api/brahmastra/option-chain/*
```

### 13.2 Medium Priority Enhancements

#### 5. **Trailing Stop Loss**
```
Current: Fixed stop loss at entry
Enhancement: Dynamic trailing stop
Implementation:
  - Trail by ATR
  - Supertrend-based trailing
  - Partial profit booking at target 1
```

#### 6. **Signal Strength Categorization**
```
Enhancement: Categorize signals by strength
Categories:
  - STRONG: All indicators strongly aligned
  - MODERATE: Indicators aligned with some weakness
  - WEAK: Borderline confirmation
```

#### 7. **Market Condition Classifier**
```
Enhancement: Auto-detect market regime
Regimes:
  - TRENDING_UP / TRENDING_DOWN
  - RANGING
  - VOLATILE
Behavior: Adjust strategy parameters per regime
```

#### 8. **Historical Pattern Recognition**
```
Enhancement: Identify recurring patterns before signals
Patterns:
  - Double top/bottom near VWAP
  - MACD divergence patterns
  - Supertrend flip patterns
```

### 13.3 Low Priority Enhancements

#### 9. **Social Sentiment Integration**
```
Enhancement: Incorporate market sentiment
Sources:
  - Twitter/X sentiment analysis
  - News sentiment scoring
  - Fear & Greed index
```

#### 10. **Portfolio-Level Risk Management**
```
Enhancement: Cross-symbol risk management
Features:
  - Maximum concurrent positions
  - Correlation-based position sizing
  - Daily loss limits
```

#### 11. **Performance Attribution**
```
Enhancement: Detailed P&L attribution
Metrics:
  - P&L by indicator contribution
  - P&L by time of day
  - P&L by market regime
```

### 13.4 Technical Improvements

#### 12. **Caching Optimization**
```
Current: ConcurrentHashMap in-memory cache
Enhancement: Redis-based distributed cache
Benefits:
  - Persistence across restarts
  - Shared cache for multiple instances
  - TTL-based cache expiration
```

#### 13. **Async Processing Pipeline**
```
Enhancement: Event-driven architecture
Implementation:
  - Kafka/RabbitMQ for tick processing
  - Separate indicator calculation workers
  - Async signal persistence
```

#### 14. **API Rate Limiting**
```
Enhancement: Protect against abuse
Implementation:
  - Request rate limiting per endpoint
  - Backtest request queuing
  - Priority queue for live scans
```

### 13.5 UI/UX Enhancements

#### 15. **Interactive Chart Annotations** ✅ IMPLEMENTED (v2.2)
```
Enhancement: Visual signal markers on charts
Features:
  - Entry/exit points marked on price chart ✅
  - Stop loss and target levels as horizontal lines ✅
  - Hover tooltips with signal details ✅

Status: Fully implemented using Lightweight Charts library with:
  - Candlestick chart with Supertrend and VWAP overlays
  - BUY/SELL signal markers (arrows) on triple confirmation
  - Supertrend trend change markers (circles)
  - Price lines for Entry, Stop Loss, Target 1, 2, 3, and LTP
  - Toggle controls for showing/hiding signals and trade levels
  - Legend with color-coded indicators
  - Active signal info panel with detailed trade parameters
  - Real-time chart updates when symbol changes

Implementation Files:
  - brahmastra.component.ts: initSignalChart(), updateSignalChart(), 
    addSignalMarkers(), addTradeLevelAnnotations(), clearSignalAnnotations()
  - brahmastra.component.html: Interactive Signal Chart section with controls
  - brahmastra.component.css: Signal chart styling and legend
```

#### 16. **Customizable Dashboard**
```
Enhancement: User-configurable widgets
Features:
  - Drag-and-drop widget placement
  - Save/load dashboard layouts
  - Custom watchlist integration
```

#### 17. **Mobile Responsive Design**
```
Enhancement: Fully responsive mobile UI
Features:
  - Touch-friendly controls
  - Simplified mobile chart views
  - Push notification integration
```

#### 18. **Dark/Light Theme Toggle**
```
Current: Dark theme only
Enhancement: User theme preference
Implementation:
  - CSS variables for theming
  - LocalStorage persistence
  - System preference detection
```

---

## 15. Appendix

### 15.1 Glossary

| Term | Definition |
|------|------------|
| ATR | Average True Range - volatility indicator |
| EMA | Exponential Moving Average |
| GEX | Gamma Exposure - measure of market maker hedging sensitivity |
| MACD | Moving Average Convergence Divergence |
| Max Pain | Strike where option sellers have minimum loss |
| OI | Open Interest - total outstanding option contracts |
| PCR | Put-Call Ratio |
| VWAP | Volume Weighted Average Price |
| Supertrend | ATR-based trend following indicator |
| Triple Confirmation | All three indicators agreeing on direction |
| Gamma Wall | Strike with high gamma concentration (S/R level) |
| GEX Flip Level | Price where net GEX changes sign |
| Drawdown | Peak-to-trough decline in equity |
| Sharpe Ratio | Risk-adjusted return measure |
| Profit Factor | Gross profits / Gross losses |

### 15.2 File Reference

| File | Purpose | Lines |
|------|---------|-------|
| BrahmastraServiceImpl.java | Core strategy + option chain integration | 2710 |
| BrahmastraController.java | REST API endpoints | 630 |
| BrahmastraService.java | Service interface | 159 |
| OptionChainMetrics.java | Option chain DTO | 130 |
| brahmastra.component.ts | Angular component + interactive chart | 1050+ |
| brahmastra.component.html | UI template + signal chart section | 1100+ |
| brahmastra.component.css | Styles + signal chart styling | 2000+ |
| brahmastra.model.ts | TypeScript models | 480+ |
| brahmastra.service.ts | Angular API service | 255 |
| data.service.ts | HTTP client methods | 1250+ |
| IndicatorMetrics.java | Indicator DTO | 120 |
| GammaExposureServiceImpl.java | GEX calculations | 1369 |

### 15.3 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Jan 2026 | Initial implementation |
| 1.5 | Feb 2026 | Added indicator metrics API and charts |
| 2.0 | Feb 21, 2026 | Updated Supertrend (20, 2), comprehensive documentation |
| 2.1 | Feb 22, 2026 | **Option Chain Integration**: Max Pain, OI Analysis, GEX |
| 2.2 | Feb 22, 2026 | **Interactive Chart Annotations**: Signal markers, trade levels, lightweight-charts |
| 2.0 | Feb 21, 2026 | Updated Supertrend (20, 2), comprehensive documentation |
| 2.1 | Feb 22, 2026 | **Option Chain Integration**: Max Pain, OI Analysis, GEX |

### 15.4 Contact

For questions or contributions related to the Brahmastra strategy:
- Review the codebase at `/src/main/java/com/trading/kalyani/KTManager/`
- Check the UI components at `/ktm-ui/src/app/components/brahmastra/`

---

**Document Generated:** February 21, 2026  
**Author:** KTManager Development Team  
**Classification:** Internal Technical Documentation

