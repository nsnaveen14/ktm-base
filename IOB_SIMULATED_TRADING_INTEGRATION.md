# IOB-Based Simulated Options Trading Integration

## Technical & Functional Documentation

**Document Version:** 1.0  
**Date:** February 8, 2026  
**Author:** KTManager Development Team

---

## Table of Contents

1. [Overview](#1-overview)
2. [Functional Requirements](#2-functional-requirements)
3. [Architecture](#3-architecture)
4. [Technical Implementation](#4-technical-implementation)
5. [API Endpoints](#5-api-endpoints)
6. [Data Flow](#6-data-flow)
7. [Trading Logic](#7-trading-logic)
8. [Configuration](#8-configuration)
9. [Error Handling](#9-error-handling)
10. [Testing](#10-testing)

---

## 1. Overview

### 1.1 Purpose

This document describes the integration between the Internal Order Block (IOB) detection system and the Simulated Options Trading module. The integration enables automatic placement of simulated options trades based on fresh IOB signals, with dynamic target selection based on signal confidence levels.

### 1.2 Scope

The integration covers:
- Automatic IOB signal detection and validation
- Dynamic trade placement with ATM (At-The-Money) strike selection
- Confidence-based target selection (T1/T2/T3)
- Trade lifecycle management (entry, monitoring, exit)
- Re-entry prevention after IOB mitigation
- Performance tracking and reporting

### 1.3 Key Features

| Feature | Description |
|---------|-------------|
| **Automatic Detection** | Fresh IOBs with ≥50% confidence automatically generate trade signals |
| **Smart Entry** | Trades placed when price enters or approaches IOB zone |
| **Dynamic Targets** | Target selection based on IOB confidence level |
| **Mitigation Protection** | No trades placed on mitigated IOBs |
| **Multi-Source Trading** | IOB signals work alongside other signal sources |

---

## 2. Functional Requirements

### 2.1 Trade Signal Generation

#### 2.1.1 Signal Conditions

A valid IOB trade signal is generated when:

1. **IOB Status**: Must be `FRESH` (not mitigated, expired, or completed)
2. **Confidence Threshold**: Signal confidence ≥ 50%
3. **Price Proximity**: Current price within or near the IOB zone
4. **No Duplicate**: No existing open trade from IOB_SIGNAL source with same direction

#### 2.1.2 Signal Types

| IOB Type | Signal Type | Option Type | Trade Direction |
|----------|-------------|-------------|-----------------|
| BULLISH_IOB | BUY | CE (Call) | LONG |
| BEARISH_IOB | SELL | PE (Put) | SHORT |

### 2.2 Target Selection Based on Confidence

The target price for each trade is dynamically selected based on the IOB's signal confidence:

| Confidence Range | Target Level | Description |
|------------------|--------------|-------------|
| **85% - 100%** | Target 3 (T3) | High confidence - aim for maximum profit |
| **70% - 84%** | Target 2 (T2) | Medium confidence - balanced approach |
| **50% - 69%** | Target 1 (T1) | Lower confidence - quick profit booking |

### 2.3 Trade Exit Conditions

A trade is exited when any of the following conditions are met:

1. **Target Hit**: Option price reaches calculated target price
2. **Stop Loss Hit**: Option price falls to stop loss level
3. **Trailing Stop Loss**: Price retraces after moving favorably
4. **IOB Mitigated**: The associated IOB status changes to MITIGATED
5. **IOB Expired**: The associated IOB status changes to EXPIRED
6. **Reverse Signal**: A new IOB signal in opposite direction is detected
7. **Market Close**: Auto-exit 5 minutes before market close (3:25 PM)
8. **Manual Exit**: User-initiated exit

### 2.4 Re-entry Rules

| Scenario | Re-entry Allowed? | Notes |
|----------|-------------------|-------|
| Price returns to FRESH IOB zone | ✅ Yes | Can place new trade |
| IOB mitigated, price returns | ❌ No | IOB is no longer valid |
| Previous trade exited at target | ✅ Yes | If IOB still FRESH |
| Previous trade hit stop loss | ✅ Yes | If IOB still FRESH |

---

## 3. Architecture

### 3.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Frontend (Angular)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │  Trading        │  │  IOB Analysis   │  │  Performance   │  │
│  │  Component      │  │  Component      │  │  Charts        │  │
│  └────────┬────────┘  └────────┬────────┘  └───────┬────────┘  │
└───────────┼─────────────────────┼──────────────────┼────────────┘
            │                     │                  │
            ▼                     ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     REST API Layer                               │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐   │
│  │ SimulatedTradingController│  │ InternalOrderBlockController│   │
│  │ /api/trading/*           │  │ /api/iob/*                  │   │
│  └────────────┬─────────────┘  └──────────────┬──────────────┘   │
└───────────────┼────────────────────────────────┼─────────────────┘
                │                                │
                ▼                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Service Layer                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              SimulatedTradingServiceImpl                 │    │
│  │  ┌─────────────────┐  ┌─────────────────────────────┐   │    │
│  │  │ checkIOBSignal()│  │ placeIOBTrade()             │   │    │
│  │  └────────┬────────┘  └──────────────┬──────────────┘   │    │
│  └───────────┼──────────────────────────┼──────────────────┘    │
│              │                          │                        │
│              ▼                          ▼                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           InternalOrderBlockServiceImpl                  │    │
│  │  • getFreshIOBs()  • markAsTraded()  • checkMitigation() │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Data Layer                                   │
│  ┌──────────────────────┐  ┌────────────────────────────────┐   │
│  │ SimulatedTradeRepository│  │ InternalOrderBlockRepository │   │
│  └──────────────────────┘  └────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Class Dependencies

```
SimulatedTradingServiceImpl
├── SimulatedTradeRepository
├── TradingLedgerRepository
├── CandlePredictionService (live market data)
├── GammaExposureService (GEX validation)
├── LiquiditySweepService (whale activity)
├── InternalOrderBlockService ← NEW
└── InternalOrderBlockRepository ← NEW
```

---

## 4. Technical Implementation

### 4.1 New Service Interface Methods

**File:** `SimulatedTradingService.java`

```java
/**
 * Check for IOB (Internal Order Block) based trade signals.
 * Automatically detects fresh IOBs and creates trade signals when:
 * - IOB has confidence >= 50%
 * - Price is within or near the IOB zone
 * - IOB is not yet mitigated
 *
 * Target selection based on confidence:
 * - 85%+ confidence: Target 3
 * - 70-84% confidence: Target 2
 * - 50-69% confidence: Target 1
 */
Map<String, Object> checkIOBSignal();

/**
 * Place a trade based on IOB signal with appropriate targets.
 * @param iob The Internal Order Block to trade
 * @return The created trade or null if placement failed
 */
SimulatedTrade placeIOBTrade(InternalOrderBlock iob);
```

### 4.2 IOB Signal Detection Implementation

**File:** `SimulatedTradingServiceImpl.java`

#### 4.2.1 Signal Checking Flow

```java
private Map<String, Object> checkIOBSignalWithReverseCheck() {
    // 1. Get current market data
    Map<String, Object> liveData = candlePredictionService.getLiveTickData();
    Double niftyLTP = (Double) liveData.get("niftyLTP");
    
    // 2. Check for existing open IOB trade
    Optional<SimulatedTrade> openIOBTrade = 
        tradeRepository.findOpenTradeBySignalSource("IOB_SIGNAL");
    
    // 3. Get fresh IOBs for NIFTY
    List<InternalOrderBlock> freshIOBs = 
        internalOrderBlockService.getFreshIOBs(niftyToken);
    
    // 4. If existing trade, check for reverse signals
    if (openIOBTrade.isPresent()) {
        // Check if IOB is mitigated → exit signal
        // Check for opposite direction IOB → reverse signal
    }
    
    // 5. If no existing trade, look for new entry opportunities
    for (InternalOrderBlock iob : freshIOBs) {
        if (iob.getSignalConfidence() >= 50 && isPriceNearIOBZone(niftyLTP, iob)) {
            // Generate trade signal
        }
    }
}
```

#### 4.2.2 Price Proximity Check

```java
private boolean isPriceNearIOBZone(Double currentPrice, InternalOrderBlock iob) {
    double zoneHigh = iob.getZoneHigh();
    double zoneLow = iob.getZoneLow();
    double zoneRange = zoneHigh - zoneLow;
    
    // Allow entry if price is within zone or within 0.5% tolerance
    double tolerance = Math.max(zoneRange * 0.5, currentPrice * 0.005);
    
    return currentPrice >= (zoneLow - tolerance) && 
           currentPrice <= (zoneHigh + tolerance);
}
```

#### 4.2.3 Target Price Calculation

```java
private Double getIOBTargetPrice(InternalOrderBlock iob) {
    Double confidence = iob.getSignalConfidence();
    
    if (confidence >= 85 && iob.getTarget3() != null) {
        return iob.getTarget3();  // High confidence → T3
    } else if (confidence >= 70 && iob.getTarget2() != null) {
        return iob.getTarget2();  // Medium confidence → T2
    } else {
        return iob.getTarget1();  // Lower confidence → T1
    }
}
```

### 4.3 Trade Placement Implementation

```java
@Override
public SimulatedTrade placeIOBTrade(InternalOrderBlock iob) {
    // 1. Validate IOB is still fresh
    if (!"FRESH".equals(iob.getStatus())) {
        return null;
    }
    
    // 2. Get market data
    Map<String, Object> liveData = candlePredictionService.getLiveTickData();
    Double niftyLTP = (Double) liveData.get("niftyLTP");
    Integer atmStrike = (Integer) liveData.get("atmStrike");
    
    // 3. Determine option type
    String signalType = "BULLISH_IOB".equals(iob.getObType()) ? "BUY" : "SELL";
    String optionType = signalType.equals("BUY") ? "CE" : "PE";
    
    // 4. Get ATM option price
    Double optionLTP = optionType.equals("CE") 
        ? (Double) liveData.get("atmCELTP")
        : (Double) liveData.get("atmPELTP");
    
    // 5. Calculate targets based on IOB levels
    Double iobTargetPrice = getIOBTargetPrice(iob);
    Double iobStopLoss = iob.getStopLoss();
    
    // Convert index targets to option targets
    double indexRisk = Math.abs(iob.getEntryPrice() - iobStopLoss);
    double indexReward = Math.abs(iobTargetPrice - iob.getEntryPrice());
    double optionMultiplier = 0.5; // ATM options move ~50% of index
    
    double targetPrice = optionLTP + (indexReward * optionMultiplier);
    double stopLossPrice = optionLTP - (indexRisk * optionMultiplier);
    
    // 6. Create and save trade
    SimulatedTrade trade = SimulatedTrade.builder()
        .signalSource("IOB_SIGNAL")
        .signalType(signalType)
        .optionType(optionType)
        .strikePrice(atmStrike.doubleValue())
        .entryPrice(optionLTP)
        .targetPrice(targetPrice)
        .stopLossPrice(stopLossPrice)
        .status("OPEN")
        .build();
    
    trade = tradeRepository.save(trade);
    
    // 7. Track active IOB trade
    activeIOBTrades.put(iob.getId(), trade.getTradeId());
    
    // 8. Mark IOB as traded
    internalOrderBlockService.markAsTraded(iob.getId(), trade.getTradeId());
    
    return trade;
}
```

### 4.4 Integration with Auto-Trade Flow

The IOB signal is integrated into the main signal checking flow:

```java
@Override
public Map<String, Object> checkForTradeSignals() {
    // ... existing signal checks ...
    
    // Check IOB (Internal Order Block) signal
    Map<String, Object> iobSignal = checkIOBSignalWithReverseCheck();
    if ((Boolean) iobSignal.getOrDefault("hasSignal", false) ||
        (Boolean) iobSignal.getOrDefault("hasReverseSignal", false)) {
        allSignals.add(iobSignal);
    }
    
    // ... process signals ...
}
```

### 4.5 Trade Exit Cleanup

When an IOB trade is exited, cleanup is performed:

```java
@Override
public SimulatedTrade exitTrade(String tradeId, String exitReason, Double exitPrice) {
    // ... exit trade logic ...
    
    // Clean up IOB trade tracking
    if ("IOB_SIGNAL".equals(trade.getSignalSource())) {
        Long iobId = extractIOBIdFromEntryReason(trade.getEntryReason());
        if (iobId != null) {
            activeIOBTrades.remove(iobId);
        }
    }
    
    return trade;
}
```

---

## 5. API Endpoints

### 5.1 IOB Signal Endpoint

**Endpoint:** `GET /api/trading/signals/iob`

**Description:** Check current IOB signal status

**Response:**
```json
{
    "hasSignal": true,
    "hasReverseSignal": false,
    "signalSource": "IOB_SIGNAL",
    "signalType": "BUY",
    "signalStrength": "STRONG",
    "confidence": 87.5,
    "iobId": 12345,
    "entryPrice": 23150.50,
    "stopLoss": 23100.00,
    "target1": 23200.00,
    "target2": 23250.00,
    "target3": 23300.00,
    "zoneHigh": 23175.00,
    "zoneLow": 23125.00
}
```

### 5.2 Existing Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/trading/signals/check` | Check all trade signals including IOB |
| POST | `/api/trading/auto-trade` | Trigger auto trade (includes IOB) |
| GET | `/api/trading/open` | Get open trades (includes IOB trades) |
| GET | `/api/trading/today` | Get today's trades (includes IOB trades) |
| POST | `/api/trading/exit/{tradeId}` | Exit specific trade |
| GET | `/api/iob/fresh/{instrumentToken}` | Get fresh IOBs |
| GET | `/api/iob/analysis/{instrumentToken}` | Get IOB analysis |

---

## 6. Data Flow

### 6.1 Trade Entry Flow

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Market Data  │────▶│ IOB Signal Check │────▶│ Signal Valid?   │
│ (NIFTY LTP)  │     │                  │     │                 │
└──────────────┘     └──────────────────┘     └────────┬────────┘
                                                       │
                     ┌─────────────────────────────────┘
                     │ Yes
                     ▼
              ┌──────────────┐     ┌──────────────────┐
              │ Get ATM      │────▶│ Calculate        │
              │ Option       │     │ Targets/SL       │
              └──────────────┘     └────────┬─────────┘
                                            │
                                            ▼
              ┌──────────────┐     ┌──────────────────┐
              │ Save Trade   │◀────│ Create Trade     │
              │ to DB        │     │ Entity           │
              └──────────────┘     └──────────────────┘
                     │
                     ▼
              ┌──────────────┐     ┌──────────────────┐
              │ Mark IOB as  │────▶│ Track in         │
              │ Traded       │     │ activeIOBTrades  │
              └──────────────┘     └──────────────────┘
```

### 6.2 Trade Exit Flow

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Monitor Open │────▶│ Check Exit       │────▶│ Exit Condition  │
│ Trades       │     │ Conditions       │     │ Met?            │
└──────────────┘     └──────────────────┘     └────────┬────────┘
                                                       │
         ┌─────────────────────────────────────────────┘
         │ Yes (Target/SL/Reverse/Mitigated)
         ▼
  ┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
  │ Calculate    │────▶│ Update Trade     │────▶│ Remove from     │
  │ P&L          │     │ Status=CLOSED    │     │ activeIOBTrades │
  └──────────────┘     └──────────────────┘     └─────────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │ Update       │
                       │ Ledger       │
                       └──────────────┘
```

---

## 7. Trading Logic

### 7.1 Signal Strength Mapping

```java
private String getSignalStrengthFromConfidence(Double confidence) {
    if (confidence >= 85) return "STRONG";
    if (confidence >= 70) return "MODERATE";
    return "WEAK";
}
```

### 7.2 Option Price Estimation

The option target/stop-loss is calculated from IOB index levels:

```
Option Target = Option Entry + (Index Reward × Option Multiplier)
Option SL = Option Entry - (Index Risk × Option Multiplier)

Where:
- Index Reward = |IOB Target - IOB Entry|
- Index Risk = |IOB Entry - IOB Stop Loss|
- Option Multiplier = 0.5 (ATM options move ~50% of index)
```

### 7.3 Trade Quantity

| Parameter | Value |
|-----------|-------|
| Lot Size | 75 (NIFTY) |
| Default Lots | 4 |
| Default Quantity | 300 units |

---

## 8. Configuration

### 8.1 Trading Parameters

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `autoTradingEnabled` | true | Enable/disable auto trading |
| `NIFTY_LOT_SIZE` | 75 | NIFTY lot size |
| `DEFAULT_NUM_LOTS` | 4 | Number of lots per trade |
| `SIGNAL_COOLDOWN_MINUTES` | 5 | Minimum gap between signals |
| `maxDailyLoss` | -50,000 | Max daily loss limit |
| `maxDailyTrades` | 100 | Max trades per day |

### 8.2 Trading Hours

| Parameter | Time |
|-----------|------|
| Market Open | 09:00 AM |
| No New Trades After | 03:15 PM |
| Market Close | 03:30 PM |

### 8.3 IOB Confidence Thresholds

| Threshold | Value | Use |
|-----------|-------|-----|
| Minimum for Signal | 50% | Generate signal |
| T2 Threshold | 70% | Use Target 2 |
| T3 Threshold | 85% | Use Target 3 |

---

## 9. Error Handling

### 9.1 Validation Errors

| Error Condition | Handling |
|-----------------|----------|
| IOB not found | Return null, log warning |
| IOB not FRESH | Skip trade, log info |
| No live data | Return null, log warning |
| No ATM option LTP | Return null, log warning |
| Confidence < 50% | Skip signal |

### 9.2 Trade Placement Errors

```java
try {
    trade = tradeRepository.save(trade);
} catch (Exception e) {
    logger.error("Error placing IOB trade: {}", e.getMessage(), e);
    return null;
}
```

### 9.3 Exit Handling Errors

```java
// If exitPrice is null, fallback to entry price
if (exitPrice == null) {
    logger.info("Exit price was null - using entryPrice as fallback");
    exitPrice = trade.getEntryPrice();
}
```

---

## 10. Testing

### 10.1 Unit Test Cases

| Test Case | Description | Expected Result |
|-----------|-------------|-----------------|
| TC001 | IOB with 90% confidence | Trade placed with T3 target |
| TC002 | IOB with 75% confidence | Trade placed with T2 target |
| TC003 | IOB with 55% confidence | Trade placed with T1 target |
| TC004 | IOB with 45% confidence | No trade (below threshold) |
| TC005 | Mitigated IOB | No trade placed |
| TC006 | Price outside zone | No trade placed |
| TC007 | Existing open trade | No duplicate trade |
| TC008 | Reverse signal | Existing trade exited |

### 10.2 Integration Test Scenarios

1. **End-to-End Trade Flow**
   - Detect fresh IOB
   - Place trade automatically
   - Monitor for target/SL
   - Exit and update ledger

2. **Multi-Signal Handling**
   - IOB signal alongside EMA signal
   - Verify both trades placed independently

3. **Re-entry Test**
   - Exit trade at target
   - Price returns to IOB zone
   - Verify re-entry allowed (if IOB still FRESH)

### 10.3 API Testing

```bash
# Check IOB signal
curl -X GET http://localhost:8084/api/trading/signals/iob

# Trigger auto trade
curl -X POST http://localhost:8084/api/trading/auto-trade

# Get open trades
curl -X GET http://localhost:8084/api/trading/open
```

---

## Appendix A: Database Schema

### SimulatedTrade Table

| Column | Type | Description |
|--------|------|-------------|
| trade_id | VARCHAR | Unique trade identifier |
| signal_source | VARCHAR | "IOB_SIGNAL" for IOB trades |
| signal_type | VARCHAR | "BUY" or "SELL" |
| signal_strength | VARCHAR | "STRONG", "MODERATE", "WEAK" |
| option_type | VARCHAR | "CE" or "PE" |
| strike_price | DOUBLE | ATM strike price |
| entry_price | DOUBLE | Option entry price |
| target_price | DOUBLE | Calculated target |
| stop_loss_price | DOUBLE | Calculated stop loss |
| entry_reason | VARCHAR | Contains IOB ID reference |
| status | VARCHAR | "OPEN", "CLOSED", etc. |

### InternalOrderBlock Table

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | IOB unique ID |
| ob_type | VARCHAR | "BULLISH_IOB", "BEARISH_IOB" |
| signal_confidence | DOUBLE | Confidence percentage |
| zone_high | DOUBLE | Upper zone boundary |
| zone_low | DOUBLE | Lower zone boundary |
| target_1 | DOUBLE | First target level |
| target_2 | DOUBLE | Second target level |
| target_3 | DOUBLE | Third target level |
| stop_loss | DOUBLE | Stop loss level |
| status | VARCHAR | "FRESH", "MITIGATED", etc. |
| trade_taken | BOOLEAN | Trade placed flag |
| trade_id | VARCHAR | Associated trade ID |

---

## Appendix B: Logging

### Log Format

```
📊 IOB Signal detected: BULLISH_IOB 12345 with 87.5% confidence at zone 23125.00-23175.00
📊 IOB Trade placed successfully: TRD_20260208_001 | CE BUY @ 245.50 | Strike: 23150 | Target: 280.75 (IOB T3) | SL: 220.25 | IOB ID: 12345 | Confidence: 87.5%
```

### Log Levels

| Level | Usage |
|-------|-------|
| INFO | Trade placements, exits, signals |
| DEBUG | Signal checks, price comparisons |
| WARN | Validation failures, missing data |
| ERROR | Exceptions, trade failures |

---

## Appendix C: Frontend Integration

### Signal Source Color

**File:** `performance-charts.component.ts`

```typescript
getSourceColor(source: string): string {
    switch (source) {
        case 'IOB_SIGNAL': return '#4CAF50';  // Green
        // ... other sources
    }
}
```

### UI Display

IOB trades appear in the Trading component with:
- Green badge for IOB_SIGNAL source
- Confidence percentage displayed
- Target level (T1/T2/T3) indicator
- IOB ID reference in trade details

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Feb 8, 2026 | KTM Dev Team | Initial document |


