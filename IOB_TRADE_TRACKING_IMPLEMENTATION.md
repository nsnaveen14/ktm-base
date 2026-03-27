# IOB Trade Tracking Implementation

## Overview

This document describes the IOB (Internal Order Block) Trade Tracking feature that enables real-time monitoring of IOB trade signals from detection to completion, with full Telegram alert integration.

## Features Implemented

### 1. Trade Timeline Tracking (Backend)

New fields added to `InternalOrderBlock` entity to track the complete lifecycle of a trade:

| Field | Description |
|-------|-------------|
| `entryTriggeredTime` | Time when price entered the IOB zone (entry triggered) |
| `actualEntryPrice` | Actual entry price when zone was touched |
| `stopLossHitTime` | Time when stop loss was hit |
| `stopLossHitPrice` | Price at which stop loss was hit |
| `target1HitTime` | Time when Target 1 (1.5R) was hit |
| `target1HitPrice` | Price at which Target 1 was hit |
| `target2HitTime` | Time when Target 2 (2.5R) was hit |
| `target2HitPrice` | Price at which Target 2 was hit |
| `target3HitTime` | Time when Target 3 (4R) was hit |
| `target3HitPrice` | Price at which Target 3 was hit |
| `maxFavorableExcursion` | Maximum profit point reached during trade |
| `maxAdverseExcursion` | Maximum drawdown experienced during trade |
| `tradeOutcome` | Final outcome: WIN, LOSS, BREAKEVEN, ACTIVE |
| `pointsCaptured` | Points captured from entry to exit |

### 2. Telegram Alerts

Enhanced Telegram notifications are sent at each key milestone:

#### Entry Triggered Alert 🚀
- Sent when price enters the IOB zone
- Includes: Trade direction, entry price, zone details, stop loss, targets, confidence score, risk points

#### Target Hit Alerts 🎯
- Target 1 (1.5R): Sent when first target is reached
- Target 2 (2.5R): Sent when second target is reached  
- Target 3 (4R): Sent when third target is reached - FULL WIN
- Includes: Profit points, entry time, hit time, target price, actual entry price

#### Stop Loss Hit Alert 🛑
- Sent when stop loss is triggered
- Includes: Loss points, max profit reached before SL, entry/exit times, actual entry price

### 3. IOB Trades UI Tab

New "IOB Trades" tab added to the Order Blocks container (between "IOB Analysis" and "IOB Chart") with:

#### Header Section
- Title and description
- Live status indicator (10s refresh)
- Live tick display
- Refresh and auto-refresh toggle buttons

#### Instrument & Status Filters (Chip-based)
- **Instrument**: All | NIFTY 50 | SENSEX
- **Status**: All Trades | Active | Completed

#### Statistics Cards
- **Active Trades**: Count of currently open positions
- **Today's Wins**: Green highlighted win count
- **Today's Losses**: Red highlighted loss count
- **Today's P&L**: Points captured (positive=green, negative=red)
- **Win Rate**: Percentage of winning trades

#### Trade Timeline Cards
Each trade displays:
- **Header**: Instrument badge, Type badge (BULLISH/BEARISH), Direction (LONG/SHORT), Outcome badge, P&L
- **Trade Details Grid**: Zone, Entry Price, Stop Loss, Targets (with hit indicators), Duration, Confidence
- **Progress Bar**: Visual indicator of trade progress (25% entry → 50% T1 → 75% T2 → 100% T3/SL)
- **Timeline Events**: Chronological list of trade events with icons and timestamps
- **Max Excursion Info**: Maximum profit and drawdown reached

## How It Works

### Trade Lifecycle

1. **Signal Detection**: IOB signal is detected and stored with status "FRESH"
   - Telegram detection alert sent (if enabled)

2. **Entry Trigger**: When live price enters the IOB zone:
   - Status changes to "MITIGATED"
   - `entryTriggeredTime` and `actualEntryPrice` are recorded
   - `tradeOutcome` set to "ACTIVE"
   - **Telegram entry alert sent** 🚀

3. **Trade Monitoring**: As price moves:
   - `maxFavorableExcursion` and `maxAdverseExcursion` are updated
   - Target hits are checked and recorded
   - Stop loss is monitored

4. **Target Hits**: When targets are reached:
   - `targetXHitTime` and `targetXHitPrice` recorded
   - **Telegram target hit alert sent** 🎯
   - For T3: `tradeOutcome` = "WIN", status = "COMPLETED"

5. **Stop Loss Hit**: When stop loss is triggered:
   - `stopLossHitTime` and `stopLossHitPrice` recorded
   - `tradeOutcome` = "LOSS", status = "STOPPED"
   - `pointsCaptured` calculated (negative)
   - **Telegram stop loss alert sent** 🛑

### Real-Time Price Integration

The tracking is integrated with `KiteTickerProvider`:
```java
// In processIOBAutoTrade() method
if (iobService != null && currentPrice != null) {
    iobService.checkMitigation(instrumentToken, currentPrice);
    iobService.checkTargetHits(instrumentToken, currentPrice);
}
```

## Files Modified

### Backend (Java)
1. `InternalOrderBlock.java` - Added 14 new tracking fields
2. `InternalOrderBlockServiceImpl.java`:
   - Enhanced `checkMitigation()` - records entry time/price, sends entry alert
   - Enhanced `checkTargetHits()` - records hit times/prices, tracks excursions, sends alerts
   - Added `sendIOBEntryTelegramAlert()` method
   - Enhanced `sendTargetHitAlert()` and `sendStopLossHitAlert()` with profit/loss calculations
   - Updated `convertToMap()` to include all new fields
3. `InternalOrderBlockController.java` - Updated `convertToSummary()` with new fields
4. `TelegramNotificationServiceImpl.java` - Added IOB_ENTRY alert type

### Frontend (Angular)
1. `iob.model.ts` - Added 14 new tracking fields to interface
2. **New Component** `iob-trades/`:
   - `iob-trades.component.ts` - Full component logic
   - `iob-trades.component.html` - Timeline-based UI template
   - `iob-trades.component.css` - Styling matching IOB Analysis
3. `order-blocks-container.component.ts` - Added IobTradesComponent import
4. `order-blocks-container.component.html` - Added "IOB Trades" tab

## UI Navigation

1. Open "Order Blocks" tab in main navigation
2. Select "IOB Trades" sub-tab (second tab)
3. Use chip filters to select instrument (All/NIFTY/SENSEX)
4. Use status filter to view (All/Active/Completed)
5. View trade timelines with full event history

## Configuration

Telegram alerts can be enabled/disabled in Settings:
- IOB Entry alerts use the IOB Mitigation alerts setting
- Target Hit alerts use the Prediction → Target Hit setting
- All alerts require Telegram to be configured and enabled

## Technical Notes

- All timestamps are stored in `LocalDateTime` format
- Prices are stored as `Double` values
- Trade outcome is stored as String: "WIN", "LOSS", "BREAKEVEN", "ACTIVE"
- Database columns added with null handling for backward compatibility
- Auto-refresh interval: 10 seconds for trade monitoring
- UI uses consistent styling with IOB Analysis tab (dark theme, cyan accents)

