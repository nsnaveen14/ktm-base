# IOB High-Priority Enhancements - Implementation Documentation

## Document Information
- **Version:** 1.0
- **Date:** January 18, 2026
- **Author:** KTManager Development Team
- **Status:** Implemented

---

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Enhancement 1: Real-Time IOB Scanning via WebSocket](#enhancement-1-real-time-iob-scanning-via-websocket)
3. [Enhancement 2: Historical Performance Tracking & Backtesting](#enhancement-2-historical-performance-tracking--backtesting)
4. [Enhancement 3: Multi-Timeframe Analysis (MTF)](#enhancement-3-multi-timeframe-analysis-mtf)
5. [Enhancement 4: Automated Trading Integration](#enhancement-4-automated-trading-integration)
6. [API Reference](#api-reference)
7. [Frontend Components](#frontend-components)
8. [Database Schema](#database-schema)
9. [Configuration Guide](#configuration-guide)
10. [Usage Examples](#usage-examples)

---

## Executive Summary

This document describes the implementation of four high-priority enhancements to the Internal Order Block (IOB) trading system:

| # | Enhancement | Status | Key Benefit |
|---|-------------|--------|-------------|
| 1 | Real-Time WebSocket Scanning | ✅ Complete | Instant IOB signal delivery |
| 2 | Performance Tracking & Backtesting | ✅ Complete | Strategy validation |
| 3 | Multi-Timeframe Analysis | ✅ Complete | Higher probability trades |
| 4 | Automated Trading | ✅ Complete | Hands-free execution |

### Files Created
| File | Purpose |
|------|---------|
| `IOBTradeResult.java` | Entity for trade outcome tracking |
| `IOBTradeResultRepository.java` | Database queries for performance |
| `IOBAutoTradeService.java` | Auto-trade service interface |
| `IOBAutoTradeServiceImpl.java` | Auto-trade implementation |
| `IOBAutoTradeController.java` | REST API endpoints |
| `iob-performance.component.ts/html/css` | Frontend dashboard |

### Files Modified
| File | Changes |
|------|---------|
| `ApplicationConstants.java` | Added WebSocket topics |
| `WebSocketController.java` | Added IOB signal endpoints |
| `InternalOrderBlockService.java` | Added MTF/RT interface methods |
| `InternalOrderBlockServiceImpl.java` | Added MTF/RT implementations |
| `data.service.ts` | Added API methods |
| `iob.model.ts` | Added new TypeScript models |
| `app.component.ts/html` | Added new tab |

---

## Enhancement 1: Real-Time IOB Scanning via Scheduled Polling

### Overview
IOB signals are detected through a scheduled polling mechanism that runs every minute during market hours. The `IOBScheduler` scans for new IOBs on NIFTY and SENSEX, checks for mitigation of existing IOBs, and sends Telegram alerts for high-confidence signals (>51%).

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           IOBScheduler (Every Minute)                        │
│                                                                              │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────┐ │
│  │ Scan for New IOBs   │ -> │ Check Mitigation    │ -> │ Send Alerts     │ │
│  │ (NIFTY, SENSEX)     │    │ (Existing IOBs)     │    │ (Telegram)      │ │
│  └─────────────────────┘    └─────────────────────┘    └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       │ If confidence > 51%
                                       ▼
                    ┌───────────────────────────────────────┐
                    │          Telegram Alert               │
                    │  - IOB Type (Bullish/Bearish)         │
                    │  - Entry, Stop Loss, Targets 1-3      │
                    │  - Risk:Reward, Confidence            │
                    └───────────────────────────────────────┘
```

### Implementation Details

#### Scheduler Configuration
```java
// IOBScheduler.java - Runs every minute during market hours
@Scheduled(cron = "0 16-59 9 * * MON-FRI", zone = "Asia/Kolkata")  // First hour
@Scheduled(cron = "0 * 10-14 * * MON-FRI", zone = "Asia/Kolkata")  // Mid-day
@Scheduled(cron = "0 0-29 15 * * MON-FRI", zone = "Asia/Kolkata")  // Last hour
```

#### Configuration Property
```yaml
# application.properties
iob.scanner.enabled=true  # Enable/disable the IOB scanner
```

### Telegram Alert Format

**New IOB Signal:**
```
📊 IOB SIGNAL ALERT

🟢 BULLISH IOB Detected
📈 Instrument: NIFTY
⏰ Timeframe: 5min
📅 OB Candle Time: 2026-01-18T10:30:00

🎯 Zone High: 23150.50
🎯 Zone Low: 23120.25
💰 Current Price: 23100.00

📍 Trade Direction: LONG
🚀 Entry Price: 23120.25
🛡️ Stop Loss: 23080.00
🎯 Target 1: 23180.50
🎯 Target 2: 23220.75
🎯 Target 3: 23280.00

📊 Risk:Reward: 1.50
💪 Confidence: 85.0%
✅ Has FVG: Yes
```

**Mitigation Alert:**
```
⚠️ IOB MITIGATION ALERT

🟢 BULLISH IOB Mitigated
📈 Instrument: NIFTY
💰 Mitigation Price: 23135.00
🎯 Zone: 23120.25 - 23150.50
```

### Frontend Integration

The frontend uses a 60-second polling interval to refresh IOB data:

```typescript
// iob-analysis.component.ts
autoRefresh = true;
refreshInterval = 60000; // 60 seconds

ngOnInit(): void {
  this.loadAllData();
  this.startAutoRefresh();
}

startAutoRefresh(): void {
  if (this.autoRefresh) {
    this.refreshSub = interval(this.refreshInterval).subscribe(() => {
      this.loadAllData();
    });
  }
}
```

### Files Created/Modified
| File | Purpose |
|------|---------|
| `IOBScheduler.java` | Scheduled job for IOB scanning every minute |

---

## Enhancement 2: Historical Performance Tracking & Backtesting.realtime-status.connected .status-dot {
  background: #4caf50;
  animation: pulse-green 1.5s infinite;
}

@keyframes pulse-green {
  0% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0.7); }
  70% { box-shadow: 0 0 0 8px rgba(76, 175, 80, 0); }
  100% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0); }
}

.new-signal-badge {
  background: #ff9800;
  color: #000;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 11px;
  animation: pulse-orange 1s infinite;
}
```

### Backend Integration with Kite Ticker

The `processPriceTick` method is automatically called from `KiteTickerProvider` when price ticks are received:

```java
// KiteTickerProvider.java - onTicks handler
if (tick.getInstrumentToken() == NIFTY_INSTRUMENT_TOKEN) {
    // Process IOB price tick for NIFTY
    if (iobService != null) {
        iobService.processPriceTick(NIFTY_INSTRUMENT_TOKEN, tick.getLastTradedPrice());
    }
}

if (tick.getInstrumentToken() == SENSEX_INSTRUMENT_TOKEN) {
    // Process IOB price tick for SENSEX
    if (iobService != null) {
        iobService.processPriceTick(SENSEX_INSTRUMENT_TOKEN, tick.getLastTradedPrice());
    }
}
```

The IOB service is injected when the ticker starts:
```java
// DailyJobServiceImpl.java - startKiteTicker()
kiteTickerProvider = new KiteTickerProvider(...);
if (internalOrderBlockService != null) {
    kiteTickerProvider.setIobService(internalOrderBlockService);
}
```

---

## Enhancement 2: Historical Performance Tracking & Backtesting

### Overview
This enhancement adds the ability to track trade outcomes and run historical backtests to validate the IOB strategy before live trading.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Backtesting Flow                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ User Request │───>│ Fetch Hist.  │───>│ Detect IOBs  │  │
│  │ Date Range   │    │ Candles      │    │ in Sequence  │  │
│  └──────────────┘    └──────────────┘    └──────┬───────┘  │
│                                                  │          │
│                                         ┌────────▼───────┐  │
│                                         │ Simulate Trades│  │
│                                         │ - Entry/Exit   │  │
│                                         │ - SL/Targets   │  │
│                                         └────────┬───────┘  │
│                                                  │          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────▼───────┐  │
│  │ Return Stats │<───│ Calculate    │<───│ Save Results │  │
│  │ & Trades     │    │ Win Rate, RR │    │ to Database  │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Database Entity: IOBTradeResult

```java
@Entity
@Table(name = "iob_trade_results")
public class IOBTradeResult {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Link to IOB
    private Long iobId;
    private String tradeId;

    // Instrument details
    private Long instrumentToken;
    private String instrumentName;
    private String timeframe;

    // IOB details
    private String iobType;           // BULLISH_IOB, BEARISH_IOB
    private String tradeDirection;    // LONG, SHORT
    private Double signalConfidence;
    private Boolean hasFvg;

    // Entry details
    private Double zoneHigh;
    private Double zoneLow;
    private Double plannedEntry;
    private Double actualEntry;
    private LocalDateTime entryTime;
    private String entryTrigger;      // ZONE_TOUCH, ZONE_MIDPOINT, MANUAL

    // Stop loss & targets
    private Double plannedStopLoss;
    private Double actualStopLoss;
    private Double target1;
    private Double target2;
    private Double target3;

    // Exit details
    private Double exitPrice;
    private LocalDateTime exitTime;
    private String exitReason;        // STOP_LOSS, TARGET_1, TARGET_2, TARGET_3, MANUAL

    // Trade outcome
    private Double pointsCaptured;
    private Double achievedRR;
    private Boolean isWinner;
    private Integer targetHit;        // 0=SL, 1=T1, 2=T2, 3=T3

    // P&L
    private Integer quantity;
    private Double grossPnl;
    private Double netPnl;

    // Trade mode
    private String tradeMode;         // SIMULATED, LIVE, BACKTEST
    private String status;            // PENDING, OPEN, CLOSED

    // MTF context
    private Boolean htfAligned;
    private Double mtfConfluenceScore;

    // Peak tracking
    private Double peakFavorable;     // Max favorable excursion
    private Double peakAdverse;       // Max adverse excursion
}
```

### Backtest Implementation

```java
@Override
public Map<String, Object> runBacktest(Long instrumentToken, LocalDate startDate, 
                                        LocalDate endDate, String timeframe) {
    
    // 1. Fetch historical candles
    HistoricalDataRequest request = HistoricalDataRequest.builder()
        .instrumentToken(String.valueOf(instrumentToken))
        .fromDate(startDate.atStartOfDay())
        .toDate(endDate.atTime(15, 30))
        .interval(convertTimeframe(timeframe))
        .build();
    
    HistoricalDataResponse response = instrumentService.getHistoricalData(request);
    List<HistoricalCandle> candles = response.getCandles();

    // 2. Clear previous backtest results
    clearBacktestResults(instrumentToken);

    // 3. Process candles sequentially
    int wins = 0, losses = 0;
    double totalPnl = 0, totalRR = 0;

    for (int i = 50; i < candles.size() - 10; i++) {
        // Get lookback window for IOB detection
        List<HistoricalCandle> lookback = candles.subList(i - 50, i + 1);
        
        // Detect IOBs
        List<InternalOrderBlock> detectedIOBs = detectIOBsInCandles(
            instrumentToken, timeframe, lookback);

        for (InternalOrderBlock iob : detectedIOBs) {
            // Check if price enters zone in subsequent candles
            for (int j = i + 1; j < Math.min(i + 10, candles.size()); j++) {
                if (isPriceInZone(iob, candles.get(j).getClose())) {
                    // Simulate trade
                    IOBTradeResult trade = simulateBacktestTrade(
                        iob, candles.get(j), candles, j);
                    
                    if (trade != null) {
                        trade.setTradeMode("BACKTEST");
                        tradeResultRepository.save(trade);
                        
                        if (trade.getIsWinner()) wins++;
                        else losses++;
                        
                        totalPnl += trade.getNetPnl();
                        totalRR += trade.getAchievedRR();
                    }
                    break;
                }
            }
        }
    }

    // 4. Return results
    int totalTrades = wins + losses;
    return Map.of(
        "success", true,
        "totalTrades", totalTrades,
        "wins", wins,
        "losses", losses,
        "winRate", totalTrades > 0 ? (wins * 100.0 / totalTrades) : 0,
        "totalPnl", totalPnl,
        "averageRR", totalTrades > 0 ? (totalRR / totalTrades) : 0
    );
}
```

### Trade Simulation Logic

```java
private IOBTradeResult simulateBacktestTrade(InternalOrderBlock iob,
        HistoricalCandle entryCandle, List<HistoricalCandle> candles, int entryIndex) {
    
    double entryPrice = iob.getZoneMidpoint();
    double stopLoss = iob.getStopLoss();
    double target1 = iob.getTarget1();
    double target2 = iob.getTarget2();
    boolean isLong = "LONG".equals(iob.getTradeDirection());

    String exitReason = null;
    double exitPrice = 0;

    // Simulate price movement after entry
    for (int i = entryIndex + 1; i < Math.min(entryIndex + 50, candles.size()); i++) {
        HistoricalCandle candle = candles.get(i);

        if (isLong) {
            // Check stop loss
            if (candle.getLow() <= stopLoss) {
                exitReason = "STOP_LOSS";
                exitPrice = stopLoss;
                break;
            }
            // Check targets
            if (candle.getHigh() >= target2) {
                exitReason = "TARGET_2";
                exitPrice = target2;
                break;
            }
            if (candle.getHigh() >= target1) {
                exitReason = "TARGET_1";
                exitPrice = target1;
                break;
            }
        } else {
            // SHORT trade logic (inverse)
            if (candle.getHigh() >= stopLoss) {
                exitReason = "STOP_LOSS";
                exitPrice = stopLoss;
                break;
            }
            if (candle.getLow() <= target2) {
                exitReason = "TARGET_2";
                exitPrice = target2;
                break;
            }
        }
    }

    // Create and return trade result
    IOBTradeResult trade = IOBTradeResult.builder()
        .iobId(iob.getId())
        .actualEntry(entryPrice)
        .exitPrice(exitPrice)
        .exitReason(exitReason)
        .build();
    
    trade.calculateMetrics();
    return trade;
}
```

### Performance Statistics

```java
@Override
public Map<String, Object> getPerformanceStatistics(LocalDateTime startDate, 
                                                     LocalDateTime endDate) {
    Map<String, Object> stats = new HashMap<>();

    Long totalTrades = tradeResultRepository.countClosedTrades(startDate, endDate);
    Long winningTrades = tradeResultRepository.countWinningTrades(startDate, endDate);
    Double avgRR = tradeResultRepository.getAverageRR(startDate, endDate);
    Double totalPnl = tradeResultRepository.getTotalPnl(startDate, endDate);

    stats.put("totalTrades", totalTrades);
    stats.put("winningTrades", winningTrades);
    stats.put("losingTrades", totalTrades - winningTrades);
    stats.put("winRate", totalTrades > 0 ? (winningTrades * 100.0 / totalTrades) : 0);
    stats.put("averageRR", avgRR != null ? avgRR : 0.0);
    stats.put("totalPnl", totalPnl != null ? totalPnl : 0.0);

    // Performance by IOB type
    stats.put("performanceByType", tradeResultRepository.getPerformanceByIOBType());
    
    // Performance by timeframe
    stats.put("performanceByTimeframe", tradeResultRepository.getPerformanceByTimeframe());

    return stats;
}
```

---

## Enhancement 3: Multi-Timeframe Analysis (MTF)

### Overview
This enhancement scans for IOBs across multiple timeframes and provides confluence scoring to identify higher-probability trade setups.

### Timeframe Hierarchy

| Timeframe | Weight | Significance |
|-----------|--------|--------------|
| 5min | 1.0x | Entry timeframe |
| 15min | 1.5x | Short-term bias |
| 1hour | 2.0x | Intraday bias |
| Daily | 3.0x | Primary trend |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Multi-Timeframe Analysis                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │  Daily  │  │  1hour  │  │  15min  │  │  5min   │            │
│  │  IOBs   │  │  IOBs   │  │  IOBs   │  │  IOBs   │            │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘            │
│       │            │            │            │                   │
│       │ Weight:3x  │ Weight:2x  │ Weight:1.5x│ Weight:1x        │
│       │            │            │            │                   │
│       └────────────┴────────────┴────────────┘                   │
│                           │                                      │
│                  ┌────────▼────────┐                            │
│                  │ Determine HTF   │                            │
│                  │ Bias            │                            │
│                  │ BULLISH/BEARISH │                            │
│                  │ /NEUTRAL        │                            │
│                  └────────┬────────┘                            │
│                           │                                      │
│              ┌────────────┴────────────┐                        │
│              │                         │                        │
│     ┌────────▼────────┐     ┌─────────▼─────────┐              │
│     │ Filter 5min     │     │ Calculate MTF     │              │
│     │ IOBs by HTF     │     │ Confluence Score  │              │
│     │ Direction       │     │ (+5 per aligned   │              │
│     │                 │     │  HTF IOB)         │              │
│     └─────────────────┘     └───────────────────┘              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation

#### Scan Multiple Timeframes
```java
@Override
public Map<String, List<InternalOrderBlock>> scanMultipleTimeframes(
        Long instrumentToken, List<String> timeframes) {
    
    Map<String, List<InternalOrderBlock>> result = new LinkedHashMap<>();

    for (String timeframe : timeframes) {
        List<InternalOrderBlock> iobs = scanForIOBs(instrumentToken, timeframe);
        result.put(timeframe, iobs);
    }

    return result;
}
```

#### Determine HTF Bias
```java
private String determineHTFBias(Map<String, List<InternalOrderBlock>> mtfIOBs) {
    double bullishScore = 0;
    double bearishScore = 0;

    for (Map.Entry<String, List<InternalOrderBlock>> entry : mtfIOBs.entrySet()) {
        String timeframe = entry.getKey();
        Double weight = TIMEFRAME_WEIGHTS.getOrDefault(timeframe, 1.0);

        for (InternalOrderBlock iob : entry.getValue()) {
            if ("BULLISH_IOB".equals(iob.getObType())) {
                bullishScore += weight;
            } else if ("BEARISH_IOB".equals(iob.getObType())) {
                bearishScore += weight;
            }
        }
    }

    // Require 20% difference for directional bias
    if (bullishScore > bearishScore * 1.2) {
        return "BULLISH";
    } else if (bearishScore > bullishScore * 1.2) {
        return "BEARISH";
    }
    return "NEUTRAL";
}
```

#### Calculate MTF Confluence Score
```java
@Override
public Double calculateMTFConfluenceScore(InternalOrderBlock iob, Long instrumentToken) {
    double score = 0.0;
    String iobDirection = "BULLISH_IOB".equals(iob.getObType()) ? "BULLISH" : "BEARISH";
    String iobTimeframe = iob.getTimeframe();

    // Check alignment with higher timeframes
    for (String htf : TIMEFRAME_HIERARCHY) {
        if (isHigherTimeframe(htf, iobTimeframe)) {
            List<InternalOrderBlock> htfIOBs = iobRepository.findFreshIOBsByType(
                instrumentToken,
                iobDirection.equals("BULLISH") ? "BULLISH_IOB" : "BEARISH_IOB"
            ).stream()
             .filter(i -> htf.equals(i.getTimeframe()))
             .collect(Collectors.toList());

            for (InternalOrderBlock htfIOB : htfIOBs) {
                if (zonesOverlapOrNearby(iob, htfIOB)) {
                    Double weight = TIMEFRAME_WEIGHTS.get(htf);
                    score += 5.0 * weight;
                }
            }
        }
    }

    return Math.min(score, 25.0); // Cap at 25 points
}
```

#### Get HTF-Aligned IOBs
```java
@Override
public List<InternalOrderBlock> getHTFAlignedIOBs(Long instrumentToken) {
    List<InternalOrderBlock> ltfIOBs = iobRepository.findFreshIOBs(instrumentToken);
    String htfBias = getHTFBias(instrumentToken);

    if ("NEUTRAL".equals(htfBias)) {
        return ltfIOBs; // Return all if no clear bias
    }

    return ltfIOBs.stream()
        .filter(iob -> {
            String direction = "BULLISH_IOB".equals(iob.getObType()) ? "BULLISH" : "BEARISH";
            return direction.equals(htfBias);
        })
        .collect(Collectors.toList());
}
```

### MTF Analysis Response

```json
{
  "instrumentToken": 256265,
  "instrumentName": "NIFTY",
  "htfBias": "BULLISH",
  "totalAligned": 3,
  "summaryByTimeframe": {
    "5min": { "bullish": 2, "bearish": 1, "total": 3 },
    "15min": { "bullish": 1, "bearish": 0, "total": 1 },
    "1hour": { "bullish": 1, "bearish": 0, "total": 1 },
    "daily": { "bullish": 0, "bearish": 0, "total": 0 }
  },
  "alignedIOBs": [
    {
      "id": 45,
      "obType": "BULLISH_IOB",
      "timeframe": "5min",
      "signalConfidence": 92.5,
      "mtfConfluenceScore": 17.5
    }
  ],
  "timestamp": "2026-01-18T10:30:00"
}
```

---

## Enhancement 4: Automated Trading Integration

### Overview
This enhancement enables fully automated trade execution based on IOB signals, including entry, stop-loss management, trailing stops, and target-based exits.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Auto-Trade Execution Flow                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐                                                   │
│  │ Price Tick   │                                                   │
│  └──────┬───────┘                                                   │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │                  Risk Checks                              │       │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │       │
│  │  │Daily Loss   │  │Max Open     │  │Portfolio    │       │       │
│  │  │Limit Check  │  │Trades Check │  │Heat Check   │       │       │
│  │  └─────────────┘  └─────────────┘  └─────────────┘       │       │
│  └──────────────────────────────────────────────────────────┘       │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │              Process Open Trades                          │       │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │       │
│  │  │Check SL Hit │  │Check Target │  │Update Trail │       │       │
│  │  │             │  │Hit          │  │Stop Loss    │       │       │
│  │  └─────────────┘  └─────────────┘  └─────────────┘       │       │
│  └──────────────────────────────────────────────────────────┘       │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │              Check for New Entries                        │       │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │       │
│  │  │Get Fresh    │  │Check Entry  │  │Enter Trade  │       │       │
│  │  │IOBs         │  │Conditions   │  │             │       │       │
│  │  └─────────────┘  └─────────────┘  └─────────────┘       │       │
│  └──────────────────────────────────────────────────────────┘       │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │              Notifications                                │       │
│  │  ┌─────────────┐  ┌─────────────┐                        │       │
│  │  │Telegram     │  │WebSocket    │                        │       │
│  │  │Alert        │  │Push         │                        │       │
│  │  └─────────────┘  └─────────────┘                        │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Configuration Parameters

```java
// Default configuration values
private static final double DEFAULT_MIN_CONFIDENCE = 60.0;
private static final double DEFAULT_MAX_ZONE_DISTANCE_PERCENT = 0.5;
private static final int DEFAULT_MAX_OPEN_TRADES = 3;
private static final double DEFAULT_DAILY_LOSS_LIMIT = 5000.0;
private static final double DEFAULT_RISK_PER_TRADE = 1000.0;
private static final double DEFAULT_TRAILING_SL_ACTIVATION = 1.0; // 1R
private static final double DEFAULT_TRAILING_SL_DISTANCE = 0.5;  // 0.5R
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `minConfidence` | 60% | Minimum IOB confidence for entry |
| `maxZoneDistancePercent` | 0.5% | Max distance from zone to trigger |
| `maxOpenTrades` | 3 | Maximum simultaneous positions |
| `dailyLossLimit` | ₹5,000 | Stop trading after this loss |
| `riskPerTrade` | ₹1,000 | Risk amount for position sizing |
| `trailingSLActivation` | 1.0R | Activate trailing after 1R profit |
| `trailingSLDistance` | 0.5R | Trail distance from current price |
| `entryOnZoneTouch` | true | Enter when price touches zone |
| `requireFVG` | false | Require FVG confluence |
| `requireHTFAlignment` | false | Require HTF direction match |

### Entry Logic

```java
@Override
public boolean shouldEnterTrade(InternalOrderBlock iob, Double currentPrice) {
    if (!autoTradingEnabled) return false;

    // Check confidence threshold
    double minConfidence = getConfigDouble("minConfidence", DEFAULT_MIN_CONFIDENCE);
    if (iob.getSignalConfidence() < minConfidence) return false;

    // Check if already traded
    if (Boolean.TRUE.equals(iob.getTradeTaken())) return false;

    // Risk management checks
    if (isMaxOpenTradesReached()) return false;
    if (isDailyLossLimitReached()) return false;

    // Check FVG requirement
    if (getConfigBoolean("requireFVG", false) && !iob.getHasFvg()) return false;

    // Check HTF alignment requirement
    if (getConfigBoolean("requireHTFAlignment", false)) {
        if (!isHTFAligned(iob.getInstrumentToken(), iob.getTradeDirection())) {
            return false;
        }
    }

    // Check if price is in zone
    return isPriceInZone(iob, currentPrice);
}
```

### Trade Entry

```java
@Override
public IOBTradeResult enterTrade(InternalOrderBlock iob, Double currentPrice, 
                                  String entryTrigger) {
    String tradeId = "IOB_" + iob.getId() + "_" + System.currentTimeMillis();

    IOBTradeResult trade = IOBTradeResult.builder()
        .iobId(iob.getId())
        .tradeId(tradeId)
        .instrumentToken(iob.getInstrumentToken())
        .instrumentName(iob.getInstrumentName())
        .timeframe(iob.getTimeframe())
        .iobType(iob.getObType())
        .tradeDirection(iob.getTradeDirection())
        .signalConfidence(iob.getSignalConfidence())
        .hasFvg(iob.getHasFvg())
        .zoneHigh(iob.getZoneHigh())
        .zoneLow(iob.getZoneLow())
        .plannedEntry(iob.getEntryPrice())
        .actualEntry(currentPrice)
        .entryTime(LocalDateTime.now())
        .entryTrigger(entryTrigger)
        .plannedStopLoss(iob.getStopLoss())
        .actualStopLoss(iob.getStopLoss())
        .target1(iob.getTarget1())
        .target2(iob.getTarget2())
        .target3(iob.getTarget3())
        .tradeMode("SIMULATED")
        .status("OPEN")
        .build();

    // Calculate position size
    double riskAmount = getConfigDouble("riskPerTrade", DEFAULT_RISK_PER_TRADE);
    trade.setQuantity(calculatePositionSize(currentPrice, iob.getStopLoss(), riskAmount));
    trade.setRiskPoints(Math.abs(currentPrice - iob.getStopLoss()));

    // Save trade and update IOB
    tradeResultRepository.save(trade);
    iob.setTradeTaken(true);
    iob.setTradeId(tradeId);
    iobRepository.save(iob);

    // Send notifications
    sendTradeEntryNotification(trade, iob);
    pushTradeUpdate(trade, "ENTRY");

    return trade;
}
```

### Trailing Stop Loss

```java
@Override
public void updateTrailingStopLosses(Double currentPrice) {
    List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();

    double activationR = getConfigDouble("trailingSLActivation", 1.0);
    double trailDistance = getConfigDouble("trailingSLDistance", 0.5);

    for (IOBTradeResult trade : openTrades) {
        if (trade.getRiskPoints() == null || trade.getRiskPoints() <= 0) continue;

        double currentPnlR = calculateCurrentRR(trade, currentPrice);

        // Check if trailing SL should be activated
        if (currentPnlR >= activationR) {
            double newSL;
            boolean isLong = "LONG".equals(trade.getTradeDirection());

            if (isLong) {
                // For long: trail SL up
                newSL = currentPrice - (trailDistance * trade.getRiskPoints());
                if (newSL > trade.getActualStopLoss()) {
                    trade.setActualStopLoss(newSL);
                    tradeResultRepository.save(trade);
                }
            } else {
                // For short: trail SL down
                newSL = currentPrice + (trailDistance * trade.getRiskPoints());
                if (newSL < trade.getActualStopLoss()) {
                    trade.setActualStopLoss(newSL);
                    tradeResultRepository.save(trade);
                }
            }
        }

        // Update peak tracking
        if (currentPnlR > 0) {
            double peakR = trade.getPeakFavorable() / trade.getRiskPoints();
            if (currentPnlR > peakR) {
                trade.setPeakFavorable(currentPnlR * trade.getRiskPoints());
            }
        }
    }
}
```

### Exit Processing

```java
private IOBTradeResult checkExitConditions(IOBTradeResult trade, Double currentPrice) {
    String exitReason = null;
    boolean isLong = "LONG".equals(trade.getTradeDirection());

    // Check stop loss
    if (trade.getActualStopLoss() != null) {
        if (isLong && currentPrice <= trade.getActualStopLoss()) {
            exitReason = "STOP_LOSS";
        } else if (!isLong && currentPrice >= trade.getActualStopLoss()) {
            exitReason = "STOP_LOSS";
        }
    }

    // Check targets (T3 first, then T2, then T1)
    if (exitReason == null) {
        if (trade.getTarget3() != null) {
            if ((isLong && currentPrice >= trade.getTarget3()) ||
                (!isLong && currentPrice <= trade.getTarget3())) {
                exitReason = "TARGET_3";
            }
        }
        if (exitReason == null && trade.getTarget2() != null) {
            if ((isLong && currentPrice >= trade.getTarget2()) ||
                (!isLong && currentPrice <= trade.getTarget2())) {
                exitReason = "TARGET_2";
            }
        }
        if (exitReason == null && trade.getTarget1() != null) {
            if ((isLong && currentPrice >= trade.getTarget1()) ||
                (!isLong && currentPrice <= trade.getTarget1())) {
                exitReason = "TARGET_1";
            }
        }
    }

    if (exitReason != null) {
        return exitTrade(trade.getId(), exitReason, currentPrice);
    }

    return trade;
}
```

### Position Sizing

```java
@Override
public Integer calculatePositionSize(Double entryPrice, Double stopLoss, Double riskAmount) {
    double riskPerUnit = Math.abs(entryPrice - stopLoss);
    if (riskPerUnit <= 0) return 75; // Default to 1 lot

    int quantity = (int) (riskAmount / riskPerUnit);
    
    // Round to nearest lot size (75 for NIFTY)
    int lots = Math.max(1, quantity / 75);
    return lots * 75;
}
```

---

## API Reference

### Auto-Trade Control Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/iob/auto-trade/status` | Get status & config |
| POST | `/api/iob/auto-trade/enable` | Enable auto-trading |
| POST | `/api/iob/auto-trade/disable` | Disable auto-trading |
| POST | `/api/iob/auto-trade/config` | Update configuration |

### Trade Management Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/iob/auto-trade/enter/{iobId}` | Manual trade entry |
| POST | `/api/iob/auto-trade/exit/{tradeId}` | Exit specific trade |
| POST | `/api/iob/auto-trade/exit-all` | Exit all open trades |
| GET | `/api/iob/auto-trade/open-trades` | Get open trades |
| GET | `/api/iob/auto-trade/today` | Get today's summary |
| GET | `/api/iob/auto-trade/history` | Get trade history |

### Performance & Backtesting Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/iob/auto-trade/performance` | Get 30-day stats |
| GET | `/api/iob/auto-trade/risk` | Get risk metrics |
| POST | `/api/iob/auto-trade/backtest` | Run backtest |
| GET | `/api/iob/auto-trade/backtest/results/{token}` | Get results |
| DELETE | `/api/iob/auto-trade/backtest/results/{token}` | Clear results |

### MTF Analysis Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/iob/auto-trade/mtf/{token}` | Get MTF analysis |
| GET | `/api/iob/auto-trade/mtf/all` | Get all indices MTF |
| GET | `/api/iob/auto-trade/htf-aligned/{token}` | Get aligned IOBs |

---

## Frontend Components

### IOB Performance Component

**Location:** `ktm-ui/src/app/components/iob-performance/`

**Tabs:**
1. **Dashboard** - Auto-trade toggle, risk indicators, today's summary, MTF bias
2. **Performance** - 30-day stats, win rate, P&L, breakdown by type/timeframe
3. **Backtest** - Date picker, instrument selector, run simulation, view results
4. **Configuration** - All auto-trade parameters

### Data Service Methods

```typescript
// Auto-Trade Control
getIOBAutoTradeStatus(): Observable<any>
enableIOBAutoTrade(): Observable<any>
disableIOBAutoTrade(): Observable<any>
updateIOBAutoTradeConfig(config: any): Observable<any>

// Trade Management
enterIOBTrade(iobId: number): Observable<any>
exitIOBTrade(tradeId: number, reason: string, exitPrice: number): Observable<any>
exitAllIOBTrades(reason: string): Observable<any>
getOpenIOBTrades(): Observable<any>
getTodaysIOBTrades(): Observable<any>
getIOBTradeHistory(startDate: string, endDate: string): Observable<any>

// Performance & Backtesting
getIOBPerformance(): Observable<any>
getIOBRiskMetrics(): Observable<any>
runIOBBacktest(token: number, start: string, end: string, tf: string): Observable<any>

// MTF Analysis
getIOBMTFAnalysis(token: number): Observable<any>
getIOBMTFAnalysisAll(): Observable<any>
getHTFAlignedIOBs(token: number): Observable<any>
```

---

## Database Schema

### Table: iob_trade_results

```sql
CREATE TABLE iob_trade_results (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    iob_id BIGINT NOT NULL,
    trade_id VARCHAR(100) UNIQUE,
    
    -- Instrument
    instrument_token BIGINT,
    instrument_name VARCHAR(50),
    timeframe VARCHAR(20),
    
    -- IOB Details
    iob_type VARCHAR(20),
    trade_direction VARCHAR(10),
    signal_confidence DOUBLE,
    has_fvg BOOLEAN,
    
    -- Entry
    zone_high DOUBLE,
    zone_low DOUBLE,
    planned_entry DOUBLE,
    actual_entry DOUBLE,
    entry_time DATETIME,
    entry_trigger VARCHAR(50),
    
    -- Stop Loss & Targets
    planned_stop_loss DOUBLE,
    actual_stop_loss DOUBLE,
    target_1 DOUBLE,
    target_2 DOUBLE,
    target_3 DOUBLE,
    planned_rr DOUBLE,
    
    -- Exit
    exit_price DOUBLE,
    exit_time DATETIME,
    exit_reason VARCHAR(50),
    
    -- Outcome
    points_captured DOUBLE,
    risk_points DOUBLE,
    achieved_rr DOUBLE,
    is_winner BOOLEAN,
    target_hit INT,
    
    -- P&L
    quantity INT,
    gross_pnl DOUBLE,
    net_pnl DOUBLE,
    
    -- Mode & Status
    trade_mode VARCHAR(20),
    status VARCHAR(20),
    
    -- MTF Context
    htf_aligned BOOLEAN,
    mtf_confluence_score DOUBLE,
    
    -- Peak Tracking
    peak_favorable DOUBLE,
    peak_adverse DOUBLE,
    
    -- Timestamps
    created_at DATETIME,
    updated_at DATETIME,
    
    INDEX idx_iob_id (iob_id),
    INDEX idx_status (status),
    INDEX idx_entry_time (entry_time),
    INDEX idx_trade_mode (trade_mode)
);
```

---

## Configuration Guide

### Enabling Auto-Trading

1. Navigate to **IOB Performance** tab
2. Go to **Configuration** tab
3. Set parameters:
   - Minimum Confidence: 60-70% recommended
   - Max Zone Distance: 0.3-0.5% recommended
   - Max Open Trades: 2-3 for risk management
   - Daily Loss Limit: Based on account size
   - Risk Per Trade: 1-2% of account
4. Click **Save Configuration**
5. Go to **Dashboard** tab
6. Toggle **Auto-Trading** to ON

### Running a Backtest

1. Navigate to **IOB Performance** → **Backtest** tab
2. Select instrument (NIFTY or SENSEX)
3. Set date range (30-90 days recommended)
4. Select timeframe (5min for intraday)
5. Click **Run Backtest**
6. Review results:
   - Win rate should be > 50%
   - Average RR should be > 1.0
   - Total P&L indicates profitability

---

## Usage Examples

### Example 1: Enable Conservative Auto-Trading

```json
// POST /api/iob/auto-trade/config
{
  "minConfidence": 70,
  "maxZoneDistancePercent": 0.3,
  "maxOpenTrades": 2,
  "dailyLossLimit": 3000,
  "riskPerTrade": 500,
  "requireFVG": true,
  "requireHTFAlignment": true
}
```

### Example 2: Run 30-Day Backtest

```
POST /api/iob/auto-trade/backtest
  ?instrumentToken=256265
  &startDate=2025-12-18
  &endDate=2026-01-17
  &timeframe=5min
```

### Example 3: Get MTF Analysis

```
GET /api/iob/auto-trade/mtf/256265

Response:
{
  "htfBias": "BULLISH",
  "totalAligned": 4,
  "summaryByTimeframe": {
    "5min": { "bullish": 3, "bearish": 1 },
    "15min": { "bullish": 2, "bearish": 0 },
    "1hour": { "bullish": 1, "bearish": 0 }
  }
}
```

---

## Conclusion

These four high-priority enhancements transform the IOB trading system from a signal detection tool into a comprehensive automated trading platform with:

- **Real-Time Signals** - Instant WebSocket notifications
- **Performance Tracking** - Historical analysis and backtesting
- **MTF Analysis** - Higher probability through timeframe confluence
- **Automated Execution** - Hands-free trading with risk management

The system is now capable of detecting, validating, executing, and tracking IOB-based trades with minimal manual intervention.

---

*Document Version: 1.0*
*Last Updated: January 18, 2026*
