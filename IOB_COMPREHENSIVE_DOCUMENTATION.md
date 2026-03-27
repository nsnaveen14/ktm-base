# Internal Order Block (IOB) Feature - Comprehensive Documentation

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Core Concepts & Theory](#2-core-concepts--theory)
3. [System Architecture](#3-system-architecture)
4. [Technical Implementation](#4-technical-implementation)
5. [Database Schema](#5-database-schema)
6. [API Reference](#6-api-reference)
7. [Frontend Implementation](#7-frontend-implementation)
8. [Enhanced Analysis Features](#8-enhanced-analysis-features)
9. [Telegram Integration](#9-telegram-integration)
10. [Scheduler & Automation](#10-scheduler--automation)
11. [High Priority Enhancements (Implemented)](#11-high-priority-enhancements-implemented)
12. [FVG 6-Factor Validation System](#12-fvg-6-factor-validation-system)
13. [Configuration Reference](#13-configuration-reference)
14. [Logging Configuration](#14-logging-configuration)
15. [Potential Future Enhancements](#15-potential-future-enhancements)
16. [Appendix](#16-appendix)
17. [Brahmastra Tab Chart & Live Tick Integration](#17-brahmastra-tab-chart--live-tick-integration)

---

## 1. Executive Summary

### 1.1 What is the IOB Feature?

The Internal Order Block (IOB) feature is a **Smart Money Concepts (SMC)** based trading analysis system that automatically identifies institutional order flow patterns in price action. It detects potential reversal or continuation zones where institutional orders (banks, hedge funds, large traders) have accumulated.

### 1.2 Key Capabilities

| Capability | Status | Description |
|------------|--------|-------------|
| **IOB Detection** | ✅ Implemented | Automated detection of bullish and bearish Internal Order Blocks |
| **Multi-Timeframe Analysis** | ✅ Implemented | Analysis across 5min, 15min, 1hour, and daily timeframes |
| **Market Structure Analysis** | ✅ Implemented | Trend identification, CHoCH detection, premium/discount zones |
| **Volume Profile Integration** | ✅ Implemented | Institutional volume detection, POC alignment, delta analysis |
| **Risk Management** | ✅ Implemented | Position sizing, ATR-based stops, portfolio heat tracking |
| **Trade Setup Generation** | ✅ Implemented | Automated entry, stop-loss, and multi-target calculation (T1, T2, T3) |
| **Telegram Alerts** | ✅ Implemented | Real-time notifications for high-confidence signals (>51%) |
| **FVG 6-Factor Validation** | ✅ Implemented | Comprehensive FVG validation: Unmitigated, Candle Reaction, S/R Confluence, Priority, Gann Box, BOS |
| **Scheduled Scanning** | ✅ Implemented | Every-minute scanning during market hours (9:16 AM - 3:29 PM) |
| **Historical Performance Tracking** | ✅ Implemented | Trade result storage, win rate, P&L tracking |
| **Backtesting Module** | ✅ Implemented | Strategy validation on historical data |
| **Interactive Chart Visualization** | ✅ Implemented | Lightweight Charts with IOB zones, levels, and swing points |
| **Auto Trading Integration** | ✅ Implemented | Automated order placement framework via Kite API |

### 1.3 Supported Instruments

| Instrument | Token | Status |
|------------|-------|--------|
| **NIFTY 50** | 256265 | ✅ Fully Supported |
| **SENSEX** | 265 | ✅ Fully Supported |
| **Bank NIFTY** | 260105 | ⚠️ Partially Supported |

### 1.4 Technology Stack

| Component | Technology |
|-----------|------------|
| Backend | Spring Boot 3.x, Java 17+ |
| Database | MySQL/PostgreSQL (JPA/Hibernate) |
| Frontend | Angular 17+, Angular Material, Lightweight Charts |
| External APIs | Kite Connect (Historical Data, Orders), Telegram Bot API |
| Scheduling | Spring Scheduler with Cron expressions |
| Logging | SLF4J + Logback with dedicated IOB log file |

---

## 2. Core Concepts & Theory

### 2.1 What is an Internal Order Block?

An IOB represents the **last opposing candle** before a significant Break of Structure (BOS). These zones mark areas where institutional orders were placed to fuel price movements.

| IOB Type | Definition | Trade Direction |
|----------|------------|-----------------|
| **Bullish IOB** | Last bearish (red) candle before a bullish Break of Structure | LONG |
| **Bearish IOB** | Last bullish (green) candle before a bearish Break of Structure | SHORT |

### 2.2 Break of Structure (BOS)

BOS occurs when price closes beyond a significant swing point:

```
Bullish BOS: Price closes ABOVE a previous swing high
   → Indicates strong buying pressure
   → Look for bullish IOB (last bearish candle before the move)

Bearish BOS: Price closes BELOW a previous swing low
   → Indicates strong selling pressure
   → Look for bearish IOB (last bullish candle before the move)
```

### 2.3 Swing Point Detection

The system identifies swing points using a **lookback period of 3 candles** on each side:

```
Swing High: Candle high is greater than the high of 3 candles before AND after it
Swing Low: Candle low is less than the low of 3 candles before AND after it
```

### 2.4 Displacement

Displacement is the strong momentum candle that confirms the institutional move:

```
Valid Displacement:
- Body size ≥ 60% of total candle range
- Direction matches the expected move (bullish/bearish)
```

### 2.5 Fair Value Gap (FVG)

An FVG is a price imbalance that adds confluence to IOB signals:

```
Bullish FVG: Candle 3's Low > Candle 1's High (gap above)
Bearish FVG: Candle 3's High < Candle 1's Low (gap below)
```

**Not all FVGs are valid.** The system applies a **6-Factor Validation** to determine if an FVG is tradeable:

| Factor | Description | Bullish FVG | Bearish FVG |
|--------|-------------|-------------|-------------|
| **1. Unmitigated** | Zone not filled/tested since creation | No candle closed in or wicked fully through zone | No candle closed in or wicked fully through zone |
| **2. Candle Reaction** | Reaction candle on retrace closes properly | Close inside FVG or above fvgLow | Close inside FVG or below fvgHigh |
| **3. S/R Confluence** | FVG overlaps with support/resistance levels | Overlaps with prior swing lows (support) | Overlaps with prior swing highs (resistance) |
| **4. Priority by Position** | Ranking among FVGs of same type | Lowest FVG = highest priority (#1) | Highest FVG = highest priority (#1) |
| **5. Gann Box Position** | FVG in correct portion of recent price range | In lower portion (0–0.5 of range) | In upper portion (0.5–1 of range) |
| **6. BOS Before Gap** | FVG formed after a Break of Structure | BOS level confirmed before FVG | BOS level confirmed before FVG |

**Validation Score:** Each factor contributes equally (~16.7%). Score ≥ 50% = **Valid FVG ✅**

---

## 3. System Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FRONTEND (Angular 17+)                            │
│  ┌──────────────────────────┐  ┌─────────────────────────────────────────┐  │
│  │   IOB Analysis Component │  │      IOB Chart Component               │  │
│  │   - Dashboard View       │  │   - Lightweight Charts Integration     │  │
│  │   - Trade Execution      │  │   - IOB Zone Visualization            │  │
│  │   - Statistics Display   │  │   - Swing Points & BOS Levels         │  │
│  └──────────────────────────┘  └─────────────────────────────────────────┘  │
│  ┌──────────────────────────┐  ┌─────────────────────────────────────────┐  │
│  │   Performance Dashboard  │  │      Data & Performance Services       │  │
│  │   - Win Rate & P&L       │  │   - HTTP calls to /api/iob/*           │  │
│  │   - Equity Curve         │  │   - Auto-refresh (60s interval)        │  │
│  │   - Backtesting Results  │  │   - Observable subscriptions           │  │
│  └──────────────────────────┘  └─────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BACKEND (Spring Boot 3.x)                         │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                     REST Controllers                                │     │
│  │  ┌────────────────────┐  ┌─────────────────┐  ┌──────────────────┐ │     │
│  │  │ IOBController      │  │ IOBAutoTrade    │  │Performance       │ │     │
│  │  │ /api/iob/*         │  │ Controller      │  │Controller        │ │     │
│  │  └────────────────────┘  └─────────────────┘  └──────────────────┘ │     │
│  │  ┌────────────────────┐  ┌─────────────────┐                       │     │
│  │  │MarketAnalysis      │  │ChartController  │                       │     │
│  │  │Controller          │  │                 │                       │     │
│  │  └────────────────────┘  └─────────────────┘                       │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                        Service Layer                                │     │
│  │  ┌──────────────────────────────────────────────────────────────┐  │     │
│  │  │              InternalOrderBlockServiceImpl                    │  │     │
│  │  │  - scanForIOBs()          - detectBullishIOBs()              │  │     │
│  │  │  - detectBearishIOBs()    - validateIOB()                    │  │     │
│  │  │  - calculateTradeSetup()  - calculateEnhancedConfidence()    │  │     │
│  │  │  - enhanceWithMarketStructure/Volume/Risk                     │  │     │
│  │  └──────────────────────────────────────────────────────────────┘  │     │
│  │                              │                                       │     │
│  │          ┌───────────────────┼───────────────────┐                  │     │
│  │          ▼                   ▼                   ▼                  │     │
│  │  ┌──────────────┐  ┌─────────────────┐  ┌────────────────────┐     │     │
│  │  │MarketStructure│  │ VolumeProfile   │  │ RiskManagement     │     │     │
│  │  │Service        │  │ Service         │  │ Service            │     │     │
│  │  └──────────────┘  └─────────────────┘  └────────────────────┘     │     │
│  │          ┌───────────────────┼───────────────────┐                  │     │
│  │          ▼                   ▼                   ▼                  │     │
│  │  ┌──────────────┐  ┌─────────────────┐  ┌────────────────────┐     │     │
│  │  │IOBAutoTrade  │  │ Performance     │  │ Backtesting        │     │     │
│  │  │Service       │  │ TrackingService │  │ Service            │     │     │
│  │  └──────────────┘  └─────────────────┘  └────────────────────┘     │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                      Scheduled Tasks                                │     │
│  │  ┌─────────────────────────────────────────────────────────────┐   │     │
│  │  │                     IOBScheduler                             │   │     │
│  │  │  - Runs every minute during market hours (9:16 AM - 3:29 PM)│   │     │
│  │  │  - Scans NIFTY & SENSEX for new IOBs                        │   │     │
│  │  │  - Checks for IOB mitigation                                 │   │     │
│  │  │  - Sends Telegram alerts for high-confidence signals (>51%) │   │     │
│  │  │  - Processes auto trading if enabled                         │   │     │
│  │  │  - Checks and updates trade outcomes                         │   │     │
│  │  └─────────────────────────────────────────────────────────────┘   │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                     Repository Layer                                │     │
│  │  ┌─────────────────┐ ┌───────────────────┐ ┌────────────────────┐  │     │
│  │  │ IOBRepository   │ │ IOBTradeResult    │ │ MarketStructure    │  │     │
│  │  │                 │ │ Repository        │ │ Repository         │  │     │
│  │  └─────────────────┘ └───────────────────┘ └────────────────────┘  │     │
│  │  ┌─────────────────┐ ┌───────────────────┐ ┌────────────────────┐  │     │
│  │  │VolumeProfile    │ │ RiskManagement    │ │ PerformanceMetrics │  │     │
│  │  │Repository       │ │ Repository        │ │ Repository         │  │     │
│  │  └─────────────────┘ └───────────────────┘ └────────────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL SERVICES                                   │
│  ┌─────────────────────────────┐    ┌────────────────────────────────────┐ │
│  │    Kite Connect API         │    │      Telegram Bot API              │ │
│  │    - Historical Candles     │    │      - IOB Signal Alerts           │ │
│  │    - Real-time Prices       │    │      - Mitigation Notifications    │ │
│  │    - Order Placement        │    │      - System Status Alerts        │ │
│  └─────────────────────────────┘    └────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Responsibilities

| Component | File | Responsibility |
|-----------|------|----------------|
| **IOB Controller** | `InternalOrderBlockController.java` | REST API endpoints for IOB operations |
| **IOB Service** | `InternalOrderBlockServiceImpl.java` | Core IOB detection and analysis logic |
| **IOB Repository** | `InternalOrderBlockRepository.java` | Database operations for IOB entities |
| **IOB Scheduler** | `IOBScheduler.java` | Scheduled scanning, alerting, auto-trading |
| **IOB Auto Trade Service** | `IOBAutoTradeServiceImpl.java` | Automated trading based on IOB signals |
| **Market Structure Service** | `MarketStructureServiceImpl.java` | Trend, CHoCH, and market phase analysis |
| **Volume Profile Service** | `VolumeProfileServiceImpl.java` | Volume-based analysis and POC calculation |
| **Risk Management Service** | `RiskManagementServiceImpl.java` | Position sizing and risk controls |
| **Performance Tracking Service** | `PerformanceTrackingServiceImpl.java` | Trade result tracking and metrics |
| **Backtesting Service** | `BacktestingServiceImpl.java` | Historical strategy validation |
| **Telegram Service** | `TelegramNotificationService.java` | Alert notifications |

---

## 4. Technical Implementation

### 4.1 IOB Detection Algorithm

The IOB detection process follows these steps:

```java
// Step 1: Fetch historical candles (last 5 days, 100 candles default)
List<HistoricalCandle> candles = fetchHistoricalCandles(instrumentToken, timeframe);

// Step 2: Identify swing points
List<SwingPoint> swingHighs = identifySwingHighs(candles);  // SWING_LOOKBACK = 3
List<SwingPoint> swingLows = identifySwingLows(candles);

// Step 3: Detect Break of Structure
// For Bullish BOS: Find candle that closes above a swing high
// For Bearish BOS: Find candle that closes below a swing low

// Step 4: Find the IOB candle
// Bullish IOB: Last bearish candle between swing point and BOS
// Bearish IOB: Last bullish candle between swing point and BOS

// Step 5: Validate displacement
boolean validDisplacement = hasValidDisplacement(candles, obIndex, bosIndex, isBullish);
// Requires: body >= 60% of range, correct direction

// Step 6: Create and validate IOB (includes FVG detection + 6-factor validation)
InternalOrderBlock iob = createIOB(...);
// checkForFVG() → detectFVG → validateFVG() with 6 factors:
//   Factor 1: Unmitigated (zone not filled since creation)
//   Factor 2: Candle Reaction (reaction candle closes properly)
//   Factor 3: S/R Confluence (overlaps with swing highs/lows)
//   Factor 5: Gann Box Position (bullish in 0-0.5, bearish in 0.5-1)
//   Factor 6: BOS Before Gap (BOS confirmed before FVG)

validateIOB(iob, currentPrice);  // Calculate confidence (includes graduated FVG boost)

// Step 7: Post-pass - Assign FVG Priority Rankings (Factor 4)
assignFvgPriority(detectedIOBs);
// Bullish: lowest FVG zone = rank #1; Bearish: highest FVG zone = rank #1
// Recalculates final FVG validation score with all 6 factors

// Step 8: Enhance with additional analysis
enhanceWithMarketStructure(iob, candles);
enhanceWithVolumeProfile(iob, candles);
enhanceWithRiskManagement(iob);
calculateEnhancedConfidence(iob);  // FVG validation score has 20% weight

// Step 9: Save and alert
iobRepository.save(iob);
sendIOBTelegramAlert(iob);  // If confidence > 51%, includes FVG validation details
```

### 4.2 Trade Setup Calculation

```java
// For Bullish IOB (LONG trade):
entryPrice = zoneMidpoint;
stopLoss = zoneLow - (zoneLow * 0.001);  // 0.1% buffer below

riskPoints = entryPrice - stopLoss;
target1 = entryPrice + (riskPoints * 1.5);  // 1:1.5 RR
target2 = entryPrice + (riskPoints * 2.5);  // 1:2.5 RR
target3 = entryPrice + (riskPoints * 4.0);  // 1:4.0 RR

// For Bearish IOB (SHORT trade):
entryPrice = zoneMidpoint;
stopLoss = zoneHigh + (zoneHigh * 0.001);  // 0.1% buffer above

riskPoints = stopLoss - entryPrice;
target1 = entryPrice - (riskPoints * 1.5);
target2 = entryPrice - (riskPoints * 2.5);
target3 = entryPrice - (riskPoints * 4.0);
```

### 4.3 Confidence Scoring

```java
// Base confidence: 70%
double confidence = 70.0;

// Zone size check
if (zoneSizePercent > 1.0) confidence -= 15;  // Too wide
if (zoneSizePercent < 0.05) confidence -= 10; // Too narrow

// Distance from current price
if (distancePercent > 2.0) confidence -= 10;  // Price too far

// FVG validation (graduated scoring based on 6-factor validation)
if (hasFVG && fvgValid) {
    // Valid FVG: graduated boost based on validation score (up to +20)
    double fvgBoost = (fvgValidationScore / 100.0) * 20.0;
    confidence += fvgBoost;
} else if (hasFVG && !fvgValid) {
    confidence += 5;  // FVG present but failed validation
}

// Price already past zone (mitigated)
if (isPastZone) confidence -= 20;

// Final: Clamp to 0-100, valid if >= 50%
confidence = Math.max(0, Math.min(100, confidence));
isValid = confidence >= 50;
```

### 4.4 Enhanced Confidence Calculation

The enhanced confidence combines multiple analysis factors:

```java
Enhanced Confidence = 
    (Base Signal Confidence × 40%) +
    (Market Structure Score × 20%) +
    (Volume Confluence Score × 20%) +
    (FVG Validation Score × 20%)

// Additional adjustments:
+ Trend Aligned: +5
- Trend Opposed: -10
+ Institutional Volume: +5
- Retail Volume: -5
+ Displacement Volume Confirmed: +5
+ POC Aligned with IOB: +5
- Risk Validation Failed: -10
+ Valid FVG (score ≥ 50%): +5
+ Top Priority FVG (#1): +3
- Invalid FVG (present but failed): -3
```

---

## 5. Database Schema

### 5.1 Internal Order Blocks Table

```sql
CREATE TABLE internal_order_blocks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    instrument_token BIGINT NOT NULL,
    instrument_name VARCHAR(50),
    timeframe VARCHAR(20) NOT NULL,
    detection_timestamp DATETIME NOT NULL,
    
    -- Order Block Candle Details
    ob_candle_time DATETIME,
    ob_type VARCHAR(20),          -- BULLISH_IOB, BEARISH_IOB
    ob_high DOUBLE,
    ob_low DOUBLE,
    ob_open DOUBLE,
    ob_close DOUBLE,
    
    -- Zone Levels
    zone_high DOUBLE,
    zone_low DOUBLE,
    zone_midpoint DOUBLE,
    
    -- Displacement Details
    displacement_high DOUBLE,
    displacement_low DOUBLE,
    displacement_body_percent DOUBLE,
    
    -- Market Context
    current_price DOUBLE,
    distance_to_zone DOUBLE,
    distance_percent DOUBLE,
    
    -- Break of Structure
    bos_level DOUBLE,
    bos_type VARCHAR(20),         -- BULLISH_BOS, BEARISH_BOS
    
    -- Fair Value Gap
    has_fvg BOOLEAN,
    fvg_high DOUBLE,
    fvg_low DOUBLE,
    
    -- FVG 6-Factor Validation
    fvg_valid BOOLEAN,                    -- Overall FVG validity (score >= 50%)
    fvg_validation_score DOUBLE,          -- Composite score 0-100
    fvg_validation_details VARCHAR(1000), -- Human-readable factor breakdown
    fvg_priority INT,                     -- Priority rank among same-type FVGs (1 = best)
    fvg_unmitigated BOOLEAN,              -- Factor 1: Zone not filled since creation
    fvg_candle_reaction_valid BOOLEAN,    -- Factor 2: Reaction candle closes properly
    fvg_sr_confluence BOOLEAN,            -- Factor 3: Overlaps with S/R levels
    fvg_gann_box_valid BOOLEAN,           -- Factor 5: Correct Gann Box position
    fvg_bos_confirmed BOOLEAN,            -- Factor 6: BOS occurred before FVG
    
    -- Trade Setup
    trade_direction VARCHAR(10),  -- LONG, SHORT
    entry_price DOUBLE,
    stop_loss DOUBLE,
    target_1 DOUBLE,
    target_2 DOUBLE,
    target_3 DOUBLE,
    risk_reward_ratio DOUBLE,
    
    -- Status
    status VARCHAR(20),           -- FRESH, MITIGATED, EXPIRED, TRADED
    is_valid BOOLEAN,
    signal_confidence DOUBLE,
    validation_notes TEXT,
    
    -- Market Structure Enhancement
    market_trend VARCHAR(20),
    trend_aligned BOOLEAN,
    price_zone VARCHAR(20),
    market_phase VARCHAR(30),
    choch_confluence BOOLEAN,
    structure_confluence_score DOUBLE,
    
    -- Volume Enhancement
    iob_volume BIGINT,
    iob_volume_ratio DOUBLE,
    volume_type VARCHAR(20),
    displacement_volume_confirmed BOOLEAN,
    poc_aligned BOOLEAN,
    volume_delta_direction VARCHAR(20),
    volume_confluence_score DOUBLE,
    
    -- Risk Enhancement
    position_size INT,
    lot_count INT,
    risk_amount DOUBLE,
    atr_value DOUBLE,
    dynamic_stop_loss DOUBLE,
    risk_validated BOOLEAN,
    risk_notes TEXT,
    enhanced_confidence DOUBLE,
    
    -- Tracking
    trade_taken BOOLEAN,
    trade_id VARCHAR(100),
    mitigation_time DATETIME,
    created_at DATETIME,
    updated_at DATETIME,
    
    INDEX idx_instrument_status (instrument_token, status),
    INDEX idx_detection_time (detection_timestamp),
    INDEX idx_ob_type (ob_type)
);
```

### 5.2 IOB Trade Results Table

```sql
CREATE TABLE iob_trade_results (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    iob_id BIGINT NOT NULL,
    trade_id VARCHAR(100) UNIQUE,
    
    -- Instrument Details
    instrument_token BIGINT,
    instrument_name VARCHAR(50),
    timeframe VARCHAR(20),
    
    -- IOB Details
    iob_type VARCHAR(20),
    trade_direction VARCHAR(10),
    signal_confidence DOUBLE,
    has_fvg BOOLEAN,
    
    -- Entry Details
    zone_high DOUBLE,
    zone_low DOUBLE,
    planned_entry DOUBLE,
    actual_entry DOUBLE,
    entry_time DATETIME,
    entry_trigger VARCHAR(30),    -- ZONE_TOUCH, ZONE_MIDPOINT, ZONE_BREAK, MANUAL
    
    -- Stop Loss Details
    planned_stop_loss DOUBLE,
    actual_stop_loss DOUBLE,
    
    -- Target Details
    target_1 DOUBLE,
    target_2 DOUBLE,
    target_3 DOUBLE,
    planned_rr DOUBLE,
    
    -- Exit Details
    exit_price DOUBLE,
    exit_time DATETIME,
    exit_reason VARCHAR(50),      -- SL_HIT, T1_HIT, T2_HIT, T3_HIT, MANUAL, EXPIRED
    
    -- P&L
    realized_pnl DOUBLE,
    realized_rr DOUBLE,
    max_favorable_excursion DOUBLE,
    max_adverse_excursion DOUBLE,
    
    -- Status
    trade_status VARCHAR(20),     -- OPEN, CLOSED, CANCELLED
    is_winner BOOLEAN,
    
    INDEX idx_iob_id (iob_id),
    INDEX idx_trade_status (trade_status),
    INDEX idx_entry_time (entry_time)
);
```

### 5.3 Market Structure Table

```sql
CREATE TABLE market_structure (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    instrument_token BIGINT NOT NULL,
    instrument_name VARCHAR(50),
    timeframe VARCHAR(20) NOT NULL,
    analysis_timestamp DATETIME NOT NULL,
    
    -- Trend Identification
    trend_direction VARCHAR(20),        -- UPTREND, DOWNTREND, SIDEWAYS
    trend_strength VARCHAR(20),         -- STRONG, MODERATE, WEAK
    consecutive_hh_count INT,
    consecutive_hl_count INT,
    consecutive_lh_count INT,
    consecutive_ll_count INT,
    
    -- Swing Points
    last_swing_high DOUBLE,
    last_swing_high_time DATETIME,
    last_swing_low DOUBLE,
    last_swing_low_time DATETIME,
    
    -- CHoCH Detection
    choch_detected BOOLEAN,
    choch_type VARCHAR(20),             -- BULLISH_CHOCH, BEARISH_CHOCH
    choch_level DOUBLE,
    choch_timestamp DATETIME,
    
    -- Premium/Discount Zones
    range_high DOUBLE,
    range_low DOUBLE,
    equilibrium_level DOUBLE,
    price_zone VARCHAR(20),             -- PREMIUM, DISCOUNT, EQUILIBRIUM
    
    -- Market Phase
    market_phase VARCHAR(30),           -- ACCUMULATION, DISTRIBUTION, MARKUP, MARKDOWN
    phase_confidence DOUBLE,
    
    -- Analysis Metadata
    candles_analyzed INT,
    current_price DOUBLE,
    overall_bias VARCHAR(20),
    
    INDEX idx_instrument_timeframe (instrument_token, timeframe)
);
```

### 5.4 Volume Profile Table

```sql
CREATE TABLE volume_profile (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    instrument_token BIGINT NOT NULL,
    instrument_name VARCHAR(50),
    timeframe VARCHAR(20),
    analysis_timestamp DATETIME,
    
    -- POC (Point of Control)
    poc_price DOUBLE,
    poc_volume BIGINT,
    
    -- Value Area
    value_area_high DOUBLE,
    value_area_low DOUBLE,
    value_area_volume_percent DOUBLE,
    
    -- High/Low Volume Nodes
    high_volume_nodes TEXT,       -- JSON array of price levels
    low_volume_nodes TEXT,        -- JSON array of price levels
    
    -- Volume Analysis
    total_volume BIGINT,
    average_volume BIGINT,
    volume_trend VARCHAR(20),     -- INCREASING, DECREASING, STABLE
    
    INDEX idx_instrument_analysis (instrument_token, analysis_timestamp)
);
```

### 5.5 Risk Management Table

```sql
CREATE TABLE risk_management (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    instrument_token BIGINT,
    instrument_name VARCHAR(50),
    analysis_date DATE NOT NULL,
    analysis_timestamp DATETIME,
    
    -- Account Configuration
    account_capital DOUBLE,
    risk_per_trade_percent DOUBLE,
    max_risk_per_trade DOUBLE,
    
    -- Daily Limits
    max_daily_loss_percent DOUBLE,
    max_daily_loss_amount DOUBLE,
    daily_realized_pnl DOUBLE,
    daily_limit_reached BOOLEAN,
    daily_trade_count INT,
    max_daily_trades INT,
    
    -- ATR Settings
    atr_value DOUBLE,
    atr_period INT,
    atr_sl_multiplier DOUBLE,
    dynamic_sl_distance DOUBLE,
    
    -- Position Sizing
    calculated_position_size INT,
    lot_size INT,
    calculated_lots INT,
    
    -- Portfolio Heat
    open_positions_count INT,
    total_open_risk DOUBLE,
    portfolio_heat_percent DOUBLE,
    max_portfolio_heat_percent DOUBLE,
    
    -- Trading Status
    trading_allowed BOOLEAN,
    trading_blocked_reason TEXT,
    
    INDEX idx_analysis_date (analysis_date)
);
```

---

## 6. API Reference

### 6.1 IOB Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/iob/scan/{instrumentToken}` | Scan for IOBs on specific instrument |
| `POST` | `/api/iob/scan-all` | Scan all indices (NIFTY, SENSEX) |
| `GET` | `/api/iob/fresh/{instrumentToken}` | Get unmitigated IOBs |
| `GET` | `/api/iob/bullish/{instrumentToken}` | Get bullish IOBs only |
| `GET` | `/api/iob/bearish/{instrumentToken}` | Get bearish IOBs only |
| `GET` | `/api/iob/today/{instrumentToken}` | Get today's detected IOBs |
| `GET` | `/api/iob/tradable/{instrumentToken}` | Get valid tradable IOBs |
| `GET` | `/api/iob/dashboard` | Get dashboard data for all indices |
| `GET` | `/api/iob/analysis/{instrumentToken}` | Get detailed analysis |
| `GET` | `/api/iob/all-indices` | Get data for NIFTY & SENSEX |
| `GET` | `/api/iob/trade-setup/{iobId}` | Generate trade setup |
| `POST` | `/api/iob/execute-trade/{iobId}` | Execute trade based on IOB |
| `POST` | `/api/iob/check-mitigation` | Check IOB mitigation |
| `GET` | `/api/iob/statistics/{instrumentToken}` | Get performance statistics |
| `GET` | `/api/iob/mtf-analysis/{instrumentToken}` | Multi-timeframe analysis |

### 6.2 Auto Trade Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/iob/auto-trade/enable` | Enable auto trading |
| `POST` | `/api/iob/auto-trade/disable` | Disable auto trading |
| `GET` | `/api/iob/auto-trade/status` | Get auto trade status |
| `GET` | `/api/iob/auto-trade/config` | Get auto trade configuration |
| `PUT` | `/api/iob/auto-trade/config` | Update auto trade configuration |
| `GET` | `/api/iob/auto-trade/open-trades` | Get open auto trades |

### 6.3 Performance Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/performance/metrics` | Get all-time performance metrics |
| `GET` | `/api/performance/daily/{date}` | Get daily metrics |
| `GET` | `/api/performance/trades/recent` | Get recent trades |
| `GET` | `/api/performance/trades/open` | Get open trades |
| `GET` | `/api/performance/equity-curve` | Get equity curve data |
| `POST` | `/api/performance/backtest` | Run backtest |
| `GET` | `/api/performance/backtest/results` | Get backtest results |

### 6.4 Chart Data Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/chart/complete/{instrumentToken}` | Get complete chart data with IOBs |
| `GET` | `/api/chart/candles/{instrumentToken}` | Get candlestick data |
| `GET` | `/api/chart/iob-zones/{instrumentToken}` | Get IOB zones for charting |
| `GET` | `/api/chart/swing-points/{instrumentToken}` | Get swing points |
| `GET` | `/api/chart/trade-levels/{iobId}` | Get trade levels for IOB |

---

## 7. Frontend Implementation

### 7.1 Component Structure

```
ktm-ui/src/app/components/
├── iob-analysis/
│   ├── iob-analysis.component.ts      # Main IOB dashboard
│   ├── iob-analysis.component.html
│   └── iob-analysis.component.css
├── iob-chart/
│   ├── iob-chart.component.ts         # Interactive chart with Lightweight Charts
│   ├── iob-chart.component.html
│   └── iob-chart.component.css
├── iob-performance/
│   ├── iob-performance.component.ts   # Performance tracking dashboard
│   ├── iob-performance.component.html
│   └── iob-performance.component.css
└── performance-dashboard/
    ├── performance-dashboard.component.ts  # Backtesting & metrics
    ├── performance-dashboard.component.html
    └── performance-dashboard.component.css
```

### 7.2 IOB Analysis Component Features

| Feature | Description |
|---------|-------------|
| **Dashboard View** | Tabbed view for NIFTY and SENSEX |
| **Auto-Refresh** | 60-second polling interval |
| **Manual Scan** | On-demand IOB scanning |
| **Trade Execution** | Direct trade execution from UI |
| **Signal Indicators** | Color-coded bullish/bearish signals |
| **Confidence Display** | Visual confidence level indicators |

### 7.3 IOB Chart Component Features

| Feature | Description |
|---------|-------------|
| **Candlestick Chart** | TradingView-style charting using Lightweight Charts |
| **IOB Zone Visualization** | Bullish (green) and bearish (red) zone overlays |
| **Trade Levels** | Entry, SL, T1, T2, T3 horizontal lines |
| **Swing Points** | Swing high/low markers on chart |
| **BOS Levels** | Break of structure level lines |
| **Interval Selector** | 5min, 15min, 1hour, Daily options |
| **Instrument Toggle** | Switch between NIFTY and SENSEX |

### 7.4 Display Columns

```typescript
displayedColumns = [
  'select',           // Checkbox for multi-select
  'obType',           // IOB type with icon
  'obCandleTime',     // OB candle time
  'detectionTimestamp',// Detection time
  'zone',             // Zone high - low
  'currentPrice',     // Current market price
  'distanceToZone',   // Distance % to zone
  'targetStatus',     // Target hit progress
  'hasFvg',           // FVG validation badge (Valid ✅ / Invalid ❌ / No FVG)
  'tradeDirection',   // LONG/SHORT
  'entryPrice',       // Entry price
  'stopLoss',         // Stop loss
  'targets',          // T1/T2/T3
  'riskReward',       // RR ratio
  'confidence',       // Signal confidence
  'status',           // FRESH/MITIGATED/TRADED
  'actions'           // Execute/View/Mitigate buttons
];
```

#### FVG Column Display

The FVG column shows a rich validation badge:

| State | Display | Color | Tooltip |
|-------|---------|-------|---------|
| **Valid FVG** | `✅ Valid 83%` | Green | Full 6-factor breakdown |
| **Invalid FVG** | `⚠️ Invalid 33%` | Orange | Full 6-factor breakdown |
| **No FVG** | `No FVG` | Gray | "No Fair Value Gap detected" |

Priority badges (`#1`, `#2`, etc.) appear next to the FVG badge in gold when available.

---

## 8. Enhanced Analysis Features

### 8.1 Market Structure Analysis

The `MarketStructureService` provides comprehensive market context:

| Feature | Description |
|---------|-------------|
| **Trend Identification** | HH/HL (uptrend) vs LH/LL (downtrend) pattern recognition |
| **CHoCH Detection** | Change of Character for trend reversal signals |
| **Premium/Discount Zones** | Price position relative to range (above/below equilibrium) |
| **Market Phase** | Accumulation, Distribution, Markup, Markdown identification |
| **Structure Confluence Score** | 0-100 score for market structure alignment |

### 8.2 Volume Profile Analysis

The `VolumeProfileService` provides volume-based confirmation:

| Feature | Description |
|---------|-------------|
| **POC Calculation** | Point of Control - highest volume price level |
| **Value Area** | 70% volume concentration zone (VAH/VAL) |
| **Institutional Volume Detection** | Volume > 1.5x average indicates institutional activity |
| **Displacement Volume** | Confirms strong moves with high volume |
| **Volume Delta** | Bullish vs bearish volume imbalance |

### 8.3 Risk Management

The `RiskManagementService` provides dynamic risk controls:

| Feature | Description |
|---------|-------------|
| **Position Sizing** | Based on account risk % (default 1% per trade) |
| **ATR-Based Stops** | Dynamic SL using Average True Range |
| **Daily Loss Limits** | Maximum daily drawdown protection |
| **Portfolio Heat** | Total open risk tracking |
| **Trade Validation** | Risk checks before trade execution |

---

## 9. Telegram Integration

### 9.1 Alert Types

| Alert Type | Trigger | Content |
|------------|---------|---------|
| **IOB Signal** | New IOB with confidence > 51% | Instrument, zone, entry/SL/targets, confidence |
| **Mitigation** | Price enters IOB zone | Instrument, mitigation price, zone details |
| **Trade Entry** | Auto trade executed | Entry details, position size |
| **Trade Exit** | Trade closed | Exit reason, P&L, RR achieved |
| **System** | Auto-trade enable/disable | Status change notification |

### 9.2 Alert Message Format

```
🟢 BULLISH IOB Detected

📈 Instrument: NIFTY
⏰ Timeframe: 5min
📅 OB Candle Time: 2026-01-20 10:30:00

🎯 Zone High: 23150.50
🎯 Zone Low: 23120.25
💰 Current Price: 23200.00

📍 Trade Direction: LONG
🚀 Entry Price: 23135.38
🛡️ Stop Loss: 23097.13
🎯 Target 1: 23192.75
🎯 Target 2: 23230.00
🎯 Target 3: 23288.38

📊 Risk:Reward: 2.50
💪 Confidence: 78.5%

📊 FVG: Valid ✅ (Score: 83%)
🏅 FVG Priority: #1
📐 FVG Zone: ₹23118.00 - ₹23142.50
✅ Unmitigated | ✅ Reaction | ✅ S/R | ✅ Gann | ✅ BOS
```

---

## 10. Scheduler & Automation

### 10.1 IOB Scheduler Configuration

```java
// Market Hours Schedule (Monday-Friday, IST)
@Scheduled(cron = "0 16-59 9 * * MON-FRI", zone = "Asia/Kolkata")   // 9:16 AM - 9:59 AM
@Scheduled(cron = "0 * 10-14 * * MON-FRI", zone = "Asia/Kolkata")   // 10:00 AM - 2:59 PM
@Scheduled(cron = "0 0-29 15 * * MON-FRI", zone = "Asia/Kolkata")   // 3:00 PM - 3:29 PM

// End of Day Cleanup
@Scheduled(cron = "0 35 15 * * MON-FRI", zone = "Asia/Kolkata")     // 3:35 PM
```

### 10.2 Scheduler Actions

Each minute during market hours:
1. **Scan for new IOBs** on NIFTY and SENSEX
2. **Check mitigation** of existing fresh IOBs
3. **Send Telegram alerts** for high-confidence signals (>51%)
4. **Process auto trading** if enabled
5. **Check trade outcomes** and update P&L

---

## 11. High Priority Enhancements (Implemented)

### 11.1 Historical Performance Tracking & Backtesting

**Status:** ✅ Fully Implemented

**Components:**
- `IOBTradeResult` entity for storing trade outcomes
- `PerformanceTrackingService` for metrics calculation
- `BacktestingService` for historical validation
- Performance dashboard UI with equity curve

**Features:**
- Trade entry/exit recording
- Win rate and average RR calculation
- Max drawdown tracking
- Equity curve visualization
- Historical backtesting on past data

### 11.2 Automated Trading Integration

**Status:** ✅ Framework Implemented

**Components:**
- `IOBAutoTradeService` for trade automation
- Auto-trade configuration management
- Entry condition monitoring
- Position management with trailing stops

**Configuration Options:**
```java
minConfidence: 60.0              // Minimum signal confidence
maxZoneDistancePercent: 0.5      // Max distance to zone for entry
maxOpenTrades: 3                 // Maximum concurrent positions
dailyLossLimit: 5000.0           // Daily loss limit (INR)
riskPerTrade: 1000.0             // Risk per trade (INR)
trailingSLActivation: 1.0        // Activate trailing after 1R profit
trailingSLDistance: 0.5          // Trail at 0.5R distance
entryOnZoneTouch: true           // Enter when price touches zone
requireFVG: false                // Require FVG for entry
```

### 11.3 Interactive Chart Visualization

**Status:** ✅ Fully Implemented

**Technology:** Lightweight Charts library

**Features:**
- TradingView-style candlestick charts
- IOB zone overlays (bullish green, bearish red)
- Trade level visualization (Entry, SL, T1, T2, T3)
- Swing point markers (SH, SL)
- BOS level lines
- Real-time price updates
- Interval selection (5min, 15min, 1hour, daily)
- Instrument switching (NIFTY/SENSEX)

### 11.4 Real-Time Price Integration

**Status:** ✅ Implemented via Polling

**Approach:** 
- Every-minute price polling via scheduled scanner
- Price updates trigger mitigation checks
- Auto-refresh on frontend (60-second interval)

---

## 12. FVG 6-Factor Validation System

**Status:** ✅ Fully Implemented (February 2026)

### 12.1 Overview

Not all Fair Value Gaps are valid trading signals. The system applies a comprehensive **6-Factor Validation** to every detected FVG, producing a validation score (0-100%), a pass/fail flag (`fvgValid`), a priority rank, and a detailed factor breakdown. This information feeds into the IOB confidence scoring, Telegram alerts, and the Angular frontend.

### 12.2 The 6 Factors

#### Factor 1: Unmitigated
The FVG zone must not have been tested, filled, or mitigated by price since its creation.

```java
// For each candle after FVG formation:
// - Check if candle close is inside FVG zone (fvgLow <= close <= fvgHigh)
// - Bullish FVG: Check if wick penetrated below fvgLow (fully mitigated)
// - Bearish FVG: Check if wick penetrated above fvgHigh (fully mitigated)
// If any candle meets these conditions → fvgUnmitigated = false
```

#### Factor 2: Candle Reaction
When price retraces to the FVG, the reaction candle must close inside the FVG or in the direction of the gap.

```java
// Find first candle that touches the FVG zone after formation
// Bullish FVG: Reaction candle must close >= fvgLow (inside or above)
//   → Invalid if close < fvgLow (closes through/opposite)
// Bearish FVG: Reaction candle must close <= fvgHigh (inside or below)
//   → Invalid if close > fvgHigh (closes through/opposite)
// If no retrace has occurred yet → valid by default (untested = good)
```

#### Factor 3: Confluence with Support/Resistance
The FVG should overlap with prior support (bullish) or resistance (bearish) levels.

```java
// Bullish FVG: Check overlap with prior swing lows (support)
//   Also check broken swing highs (former resistance → now support)
// Bearish FVG: Check overlap with prior swing highs (resistance)
//   Also check broken swing lows (former support → now resistance)
// Uses 50% zone buffer for near-overlap detection
```

#### Factor 4: Priority by Position
Among multiple FVGs of the same type, position-based ranking determines priority.

```java
// Bullish FVGs: Sort ascending by fvgLow → lowest zone = priority #1
//   (Near recent low = highest priority; near recent high = lowest)
// Bearish FVGs: Sort descending by fvgHigh → highest zone = priority #1
//   (Near recent high = highest priority; near recent low = lowest)
// Applied as a post-pass in scanForIOBs() after all IOBs are detected
```

#### Factor 5: Gann Box Position
Simulates the TradingView Gann Box tool (settings: price levels 0, 0.5, 1).

```java
// Compute recent move range (up to 50 candles lookback):
//   rangeHigh = max(high) over lookback period
//   rangeLow = min(low) over lookback period
// Normalize FVG midpoint: position = (fvgMidpoint - rangeLow) / (rangeHigh - rangeLow)
// Bullish FVG: Valid if position <= 0.5 (lower portion, near level 0)
// Bearish FVG: Valid if position >= 0.5 (upper portion, near level 1)
```

#### Factor 6: Break of Structure (BOS) Before Gap
The FVG must form after a Break of Structure — this is inherently satisfied since IOB detection requires a BOS.

```java
// Confirmed when: iob.getBosLevel() != null && iob.getBosLevel() > 0
// IOB detection pipeline requires BOS before OB candle identification,
// so FVG formation (at obIndex + 2) always occurs after BOS
```

### 12.3 Scoring System

```
Validation Score = (factors_passed / 6) × 100

Pass/Fail: fvgValid = (score >= 50%)  →  at least 3 of 6 factors must pass

Factor breakdown stored in fvgValidationDetails:
  "5/6: Unmitigated ✅ | Candle Reaction ❌ | S/R Confluence ✅ | Priority #1 ✅ | Gann Box ✅ | BOS ✅"
```

### 12.4 Impact on Confidence Scoring

**Base IOB Confidence (`validateIOB()`):**

| FVG State | Confidence Impact |
|-----------|-------------------|
| Valid FVG (score 100%) | +20.0 points |
| Valid FVG (score 67%) | +13.4 points |
| Valid FVG (score 50%) | +10.0 points |
| Invalid FVG (present but failed) | +5.0 points |
| No FVG detected | +0.0 points |

**Enhanced Confidence (`calculateEnhancedConfidence()`):**

| Component | Weight |
|-----------|--------|
| Base Signal Confidence | 40% |
| Market Structure Score | 20% |
| Volume Confluence Score | 20% |
| **FVG Validation Score** | **20%** |

Additional bonuses:
- Valid FVG: +5 points
- Top Priority FVG (#1): +3 points
- Invalid FVG (present but failed): -3 points

### 12.5 Database Columns

| Column | Type | Description |
|--------|------|-------------|
| `fvg_valid` | BOOLEAN | Overall validity (score ≥ 50%) |
| `fvg_validation_score` | DOUBLE | Composite score 0-100 |
| `fvg_validation_details` | VARCHAR(1000) | Factor breakdown text |
| `fvg_priority` | INT | Rank among same-type FVGs (1 = best) |
| `fvg_unmitigated` | BOOLEAN | Factor 1 result |
| `fvg_candle_reaction_valid` | BOOLEAN | Factor 2 result |
| `fvg_sr_confluence` | BOOLEAN | Factor 3 result |
| `fvg_gann_box_valid` | BOOLEAN | Factor 5 result |
| `fvg_bos_confirmed` | BOOLEAN | Factor 6 result |

### 12.6 API Response Fields

All 9 FVG validation fields are included in the IOB API response (`convertToMap()`):

```json
{
  "hasFvg": true,
  "fvgHigh": 23142.50,
  "fvgLow": 23118.00,
  "fvgValid": true,
  "fvgValidationScore": 83.33,
  "fvgValidationDetails": "5/6: Unmitigated ✅ | Candle Reaction ✅ | S/R Confluence ✅ | Priority #1 ✅ | Gann Box ✅ | BOS ✅",
  "fvgPriority": 1,
  "fvgUnmitigated": true,
  "fvgCandleReactionValid": true,
  "fvgSrConfluence": true,
  "fvgGannBoxValid": true,
  "fvgBosConfirmed": true
}
```

### 12.7 Telegram Alert Display

When an FVG is present in an IOB signal, the Telegram alert includes:

```
📊 FVG: Valid ✅ (Score: 83%)
🏅 FVG Priority: #1
📐 FVG Zone: ₹23118.00 - ₹23142.50
✅ Unmitigated | ✅ Reaction | ✅ S/R | ✅ Gann | ✅ BOS
```

For invalid FVGs:
```
📊 FVG: Invalid ❌ (Score: 33%)
📐 FVG Zone: ₹23118.00 - ₹23142.50
❌ Unmitigated | ❌ Reaction | ✅ S/R | ❌ Gann | ✅ BOS
```

### 12.8 Frontend Display

The Angular IOB Analysis component displays FVG validation with:

| Element | Description |
|---------|-------------|
| **FVG Badge** | Color-coded badge: Green (Valid), Orange (Invalid), Gray (No FVG) |
| **Score Display** | Percentage score shown in badge text |
| **Tooltip** | Hover shows full factor breakdown with ✅/❌ per factor |
| **Priority Badge** | Gold `#1`, `#2` rank badge next to FVG badge |
| **Trade Setup Dialog** | Full FVG factors panel in the trade setup popup |
| **Explanation Card** | Updated help section explaining all 6 factors |

### 12.9 Implementation Files

| File | Changes |
|------|---------|
| `InternalOrderBlock.java` | 9 new entity fields for FVG validation |
| `InternalOrderBlockServiceImpl.java` | `validateFVG()`, `assignFvgPriority()`, `updateFvgScoreWithPriority()` methods; updated `checkForFVG()`, `validateIOB()`, `calculateEnhancedConfidence()`, `convertToMap()`, Telegram alerts |
| `iob.model.ts` | 9 new fields in `InternalOrderBlock` interface |
| `iob-analysis.component.ts` | `getFvgDisplayText()`, `getFvgClass()`, `getFvgTooltip()`, `getFvgIcon()`, `getValidFvgCount()` methods; enhanced trade setup dialog |
| `iob-analysis.component.html` | Rich FVG validation column with badge, tooltip, priority |
| `iob-analysis.component.css` | `.fvg-badge`, `.fvg-valid`, `.fvg-invalid`, `.fvg-priority-badge` styles |

---

## 13. Configuration Reference

### 13.1 Application Properties

```properties
# IOB Scanner
iob.scanner.enabled=true

# Telegram Alerts
telegram.bot.token=YOUR_BOT_TOKEN
telegram.chat.id=YOUR_CHAT_ID
telegram.alerts.enabled=true

# Auto Trading
iob.auto-trade.enabled=false
iob.auto-trade.min-confidence=60.0
iob.auto-trade.max-daily-trades=5

# Risk Management
risk.account-capital=100000
risk.per-trade-percent=1.0
risk.max-daily-loss-percent=3.0
```

### 13.2 Constants (ApplicationConstants.java)

```java
public static final Long NIFTY_INSTRUMENT_TOKEN = 256265L;
public static final Long SENSEX_INSTRUMENT_TOKEN = 265L;
public static final Long BANKNIFTY_INSTRUMENT_TOKEN = 260105L;

public static final int SWING_LOOKBACK = 3;
public static final int LOOKBACK_CANDLES = 100;
public static final double MIN_DISPLACEMENT_BODY_PERCENT = 0.6;
public static final double TELEGRAM_ALERT_CONFIDENCE_THRESHOLD = 51.0;
```

---

## 14. Logging Configuration

### 14.1 Dedicated IOB Log File

All IOB-related logs are written to a dedicated log file: `logs/iob.log`

**Configuration in logback-spring.xml:**

```xml
<!-- Dedicated appender for Internal Order Block (IOB) related logs -->
<appender name="IOB_FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>logs/iob.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <fileNamePattern>logs/iob-%d{yyyy-MM-dd}.log</fileNamePattern>
        <maxHistory>60</maxHistory>
        <totalSizeCap>1GB</totalSizeCap>
    </rollingPolicy>
    <encoder>
        <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60}.%M:%L - %msg%n</pattern>
    </encoder>
</appender>
```

### 14.2 IOB Logger Configuration

The following classes log to the dedicated IOB file:

| Class | Description |
|-------|-------------|
| `IOBScheduler` | Scheduled scanning and alerts |
| `InternalOrderBlockServiceImpl` | Core IOB detection logic |
| `IOBAutoTradeServiceImpl` | Auto trading operations |
| `MarketStructureServiceImpl` | Market structure analysis |
| `VolumeProfileServiceImpl` | Volume profile analysis |
| `RiskManagementServiceImpl` | Risk management calculations |
| `PerformanceTrackingServiceImpl` | Trade performance tracking |
| `BacktestingServiceImpl` | Backtesting operations |
| `AutoTradingServiceImpl` | Automated trading |
| `InternalOrderBlockController` | IOB API endpoints |
| `IOBAutoTradeController` | Auto trade API endpoints |
| `PerformanceController` | Performance API endpoints |
| `MarketAnalysisController` | Market analysis API endpoints |

### 14.3 Log Retention

- **Max History:** 60 days of archived logs
- **Total Size Cap:** 1GB maximum
- **Rolling Policy:** Daily log rotation

---

## 15. Potential Future Enhancements

### 15.1 🟡 Medium Priority

| Enhancement | Description | Effort |
|-------------|-------------|--------|
| **Advanced Alert Customization** | Per-signal alert rules, multiple Telegram channels | Medium |
| **Sector/Stock Analysis** | Extend IOB detection to individual stocks | High |
| **ML Confidence Scoring** | Machine learning for pattern recognition | Very High |
| **Session-Based Analysis** | Opening range, session highs/lows | Medium |
| **Portfolio Analytics** | Comprehensive P&L dashboard, trade journal | High |

### 15.2 🟢 Low Priority

| Enhancement | Description | Effort |
|-------------|-------------|--------|
| **Cross-Instrument Correlation** | NIFTY-SENSEX divergence detection | Medium |
| **News Integration** | Economic calendar, avoid high-impact events | High |
| **Mobile Application** | Native mobile app with push notifications | Very High |
| **Paper Trading Mode** | Virtual trading with real market data | Medium |
| **Multi-Language Support** | Hindi, English, regional languages | Medium |

---

## 16. Appendix

### 16.1 File Structure

```
src/main/java/com/trading/kalyani/KTManager/
├── config/
│   └── IOBScheduler.java
├── constants/
│   └── ApplicationConstants.java
├── controller/
│   ├── InternalOrderBlockController.java
│   ├── IOBAutoTradeController.java
│   ├── PerformanceController.java
│   ├── MarketAnalysisController.java
│   └── ChartController.java
├── entity/
│   ├── InternalOrderBlock.java
│   ├── IOBTradeResult.java
│   ├── MarketStructure.java
│   ├── VolumeProfile.java
│   ├── RiskManagement.java
│   ├── TradeResult.java
│   └── PerformanceMetrics.java
├── repository/
│   ├── InternalOrderBlockRepository.java
│   ├── IOBTradeResultRepository.java
│   ├── MarketStructureRepository.java
│   ├── VolumeProfileRepository.java
│   └── RiskManagementRepository.java
├── service/
│   ├── InternalOrderBlockService.java
│   ├── IOBAutoTradeService.java
│   ├── MarketStructureService.java
│   ├── VolumeProfileService.java
│   ├── RiskManagementService.java
│   ├── PerformanceTrackingService.java
│   ├── BacktestingService.java
│   └── TelegramNotificationService.java
└── service/serviceImpl/
    ├── InternalOrderBlockServiceImpl.java
    ├── IOBAutoTradeServiceImpl.java
    ├── MarketStructureServiceImpl.java
    ├── VolumeProfileServiceImpl.java
    ├── RiskManagementServiceImpl.java
    ├── PerformanceTrackingServiceImpl.java
    └── BacktestingServiceImpl.java

ktm-ui/src/app/
├── components/
│   ├── iob-analysis/
│   ├── iob-chart/
│   ├── iob-performance/
│   └── performance-dashboard/
├── models/
│   ├── iob.model.ts
│   └── performance.model.ts
└── services/
    ├── data.service.ts
    └── performance.service.ts
```

### 16.2 Glossary

| Term | Definition |
|------|------------|
| **IOB** | Internal Order Block - The last opposing candle before BOS |
| **BOS** | Break of Structure - Price breaking beyond swing point |
| **FVG** | Fair Value Gap - Price imbalance zone (3-candle pattern) |
| **FVG Validation** | 6-factor scoring system for FVG quality: Unmitigated, Candle Reaction, S/R Confluence, Priority, Gann Box, BOS |
| **Gann Box** | TradingView tool dividing a price range into levels (0, 0.5, 1); used to validate FVG positioning |
| **CHoCH** | Change of Character - Trend reversal signal |
| **POC** | Point of Control - Highest volume price level |
| **ATR** | Average True Range - Volatility measure |
| **HH/HL** | Higher High / Higher Low - Uptrend pattern |
| **LH/LL** | Lower High / Lower Low - Downtrend pattern |
| **MTF** | Multi-Timeframe Analysis |
| **HTF** | Higher Timeframe |
| **SMC** | Smart Money Concepts |
| **VAH/VAL** | Value Area High / Value Area Low |

### 16.3 Related Documentation

- `INTERNAL_ORDER_BLOCK_DOCUMENTATION.md` - Original IOB documentation
- `IOB_HIGH_PRIORITY_ENHANCEMENTS.md` - Enhancement implementation details
- `TELEGRAM_NOTIFICATIONS.md` - Telegram integration guide
- `LIQUIDITY_ANALYSIS_IMPLEMENTATION.md` - Liquidity analysis features
- `BRAHMASTRA_COMPREHENSIVE_DOCUMENTATION.md` - Brahmastra Strategy documentation

---

## 17. Brahmastra Tab Chart & Live Tick Integration

**Status:** ✅ Fixed (February 2026)

### 17.1 Issue: Blank Charts

The Brahmastra tab's indicator charts (Price & Supertrend, MACD, VWAP) and backtest charts (Equity Curve, Drawdown) were rendering as blank because they relied on Chart.js (`declare var Chart: any`) which was never installed or loaded in the project.

### 17.2 Solution: Migration to Lightweight Charts

All Chart.js usage in the Brahmastra component was replaced with **Lightweight Charts** (TradingView's library), which is already used by the IOB Chart component and the Brahmastra Interactive Signal Chart. This ensures a consistent charting library across the entire application.

| Chart | Before (Chart.js) | After (Lightweight Charts) |
|-------|-------------------|---------------------------|
| Price & Supertrend | `<canvas>` line chart | Candlestick series + colored line series |
| MACD (12, 26, 9) | `<canvas>` bar+line combo | Histogram series + 2 line series |
| VWAP | `<canvas>` line chart | 4 line series (Price, VWAP, Upper Band, Lower Band) |
| Equity Curve | `<canvas>` line chart | Line series + Area series fill |
| Drawdown Curve | `<canvas>` line chart | Line series + inverted Area series fill |

### 17.3 Live Tick Component Added

The `<app-live-tick>` component was added to the Brahmastra tab header, providing real-time market data display consistent with other tabs (IOB Analysis, IOB Trades, Liquidity Analysis, Analytics).

**Displayed data:** NIFTY 50 price, India VIX, ATM strike CE/PE prices, Straddle value, and live connection status.

### 17.4 Files Changed

| File | Changes |
|------|---------|
| `brahmastra.component.ts` | Removed `declare var Chart: any`; Added `LiveTickComponent` import; Replaced all `new Chart(ctx, ...)` calls with `createChart(container, ...)` using Lightweight Charts API; Added `createLightweightChartOptions()` and `toChartTime()` helpers; New ViewChild refs for chart containers |
| `brahmastra.component.html` | Added `<app-live-tick></app-live-tick>` in header; Replaced `<canvas id="...">` elements with `<div #...Container>` template refs for Lightweight Charts |
| `brahmastra.component.css` | Added `.backtest-chart-container` style for backtest chart div containers |

---

*Document Version: 4.0*  
*Created: January 19, 2026*  
*Last Updated: February 23, 2026*  
*Author: KTManager Development Team*  
*Latest Features: FVG 6-Factor Validation System (Section 12), Brahmastra Chart Fix & Live Tick (Section 17)*
