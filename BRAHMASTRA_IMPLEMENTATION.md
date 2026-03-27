# Brahmastra Triple Confirmation Intraday Trading Strategy

## Overview

Brahmastra is an automated signal generator and backtester that implements the Triple Confirmation Intraday Trading Strategy. The strategy generates buy/sell signals only when **all three indicators confirm**:

1. **Supertrend** (ATR-based, default: period 20, multiplier 2) - For trend direction
2. **MACD** (12, 26, 9) - For momentum confirmation  
3. **VWAP** (Volume Weighted Average Price) - For value area confirmation

Optional **PCR (Put-Call Ratio)** filtering is available for market bias confirmation.

## Features

### 1. Signal Generation
- Generates BUY/SELL signals when all three indicators align
- Provides entry price, stop loss, and multiple target levels (1:1, 1:2, 1:3 RR)
- Calculates confidence score based on indicator strength
- PCR-based filtering (PCR > 1.2 = Bullish only, PCR < 0.8 = Bearish only)
- Sideways market detection (skips signals when Supertrend flips > 3x in 10 candles)

### 2. Backtesting Engine
- Comprehensive backtest with equity curve and drawdown analysis
- Metrics: Win rate, profit factor, Sharpe ratio, Sortino ratio, Calmar ratio
- Trade-by-trade log with entry/exit details
- CSV export functionality

### 3. Live Scanning
- Real-time scanning for NIFTY, BANKNIFTY, SENSEX
- WebSocket push for new signals
- Integration with DailyJobServiceImpl for appJobConfigNum 1 and 4
- Telegram notifications for new signals

### 4. Dashboard
- Aggregated performance across all assets
- Current PCR values and market bias
- Active signals display
- Strategy health indicator

## API Endpoints

### Signal Generation
```
POST /api/brahmastra/signals/generate
{
  "symbol": "NIFTY",
  "timeframe": "5m",
  "fromDate": "2026-02-01",
  "toDate": "2026-02-11",
  "usePCR": true
}
```

### Backtesting
```
POST /api/brahmastra/backtest/run
{
  "symbol": "BANKNIFTY",
  "timeframe": "5m",
  "fromDate": "2026-02-01",
  "toDate": "2026-02-11",
  "usePCR": true,
  "initialCapital": 100000,
  "riskPerTrade": 1
}
```

### Live Scanner
```
GET /api/brahmastra/scan/live?symbols=NIFTY,BANKNIFTY,SENSEX
```

### Dashboard
```
GET /api/brahmastra/dashboard/summary
```

### PCR Data
```
GET /api/brahmastra/pcr/current
```

## Technical Implementation

### Backend (Spring Boot)
- **Entity**: `BrahmastraSignal`, `BrahmastraBacktestResult`
- **Repository**: `BrahmastraSignalRepository`, `BrahmastraBacktestResultRepository`
- **Service**: `BrahmastraService` / `BrahmastraServiceImpl`
- **Controller**: `BrahmastraController`

### Frontend (Angular)
- **Component**: `BrahmastraComponent` (standalone)
- **Service**: `BrahmastraService` (uses `DataService` for API calls)
- **Model**: `brahmastra.model.ts`
- **DataService**: Added Brahmastra API methods following existing patterns

### Integration Points
- **KiteTickerProvider**: Live data feed
- **DailyJobServiceImpl**: Scheduled scanning for appJobConfigNum 1 and 4
- **MessagingService**: WebSocket signal push
- **TelegramNotificationService**: Alert notifications
- **IndexLTPRepository**: PCR data via `meanStrikePCR`

## Signal Logic

### BUY Signal
```
Supertrend = BULLISH (Green)
AND MACD Line > Signal Line (Crossover)
AND Price <= VWAP * 1.002 (Near or below VWAP)
AND (if PCR enabled) PCR >= 0.8 (Not strongly bearish)
```

### SELL Signal
```
Supertrend = BEARISH (Red)
AND MACD Line < Signal Line (Crossover)
AND Price >= VWAP * 0.998 (Near or above VWAP)
AND (if PCR enabled) PCR <= 1.2 (Not strongly bullish)
```

### Trade Management
- **Entry**: On signal confirmation
- **Stop Loss**: Previous candle low (BUY) / high (SELL)
- **Target**: 2:1 Risk-Reward ratio default
- **Exit**: Stop loss hit, target hit, MACD reversal, or EOD (3:25 PM IST)

## Files Created

### Backend
- `src/main/java/com/trading/kalyani/KTManager/dto/brahmastra/` (DTOs)
- `src/main/java/com/trading/kalyani/KTManager/entity/BrahmastraSignal.java`
- `src/main/java/com/trading/kalyani/KTManager/entity/BrahmastraBacktestResult.java`
- `src/main/java/com/trading/kalyani/KTManager/repository/BrahmastraSignalRepository.java`
- `src/main/java/com/trading/kalyani/KTManager/repository/BrahmastraBacktestResultRepository.java`
- `src/main/java/com/trading/kalyani/KTManager/service/BrahmastraService.java`
- `src/main/java/com/trading/kalyani/KTManager/service/serviceImpl/BrahmastraServiceImpl.java`
- `src/main/java/com/trading/kalyani/KTManager/controller/BrahmastraController.java`

### Frontend
- `ktm-ui/src/app/models/brahmastra.model.ts`
- `ktm-ui/src/app/services/brahmastra.service.ts`
- `ktm-ui/src/app/components/brahmastra/brahmastra.component.ts`
- `ktm-ui/src/app/components/brahmastra/brahmastra.component.html`
- `ktm-ui/src/app/components/brahmastra/brahmastra.component.css`

### Modified Files
- `ApplicationConstants.java` - Added `BRAHMASTRA_SIGNAL_TOPIC`
- `MessagingService.java` - Added `sendBrahmastraSignal()`
- `MessagingServiceImpl.java` - Implemented WebSocket method
- `WebSocketController.java` - Added Brahmastra WebSocket endpoint
- `DailyJobServiceImpl.java` - Integrated Brahmastra scanning
- `IndexLTPRepository.java` - Added PCR lookup query
- `CandleStickRepository.java` - Added candle fetch query
- `app.component.ts` - Added BrahmastraComponent import
- `app.component.html` - Added Brahmastra tab

## Usage

1. Navigate to the **Brahmastra** tab in the UI
2. View the **Dashboard** for overall strategy performance
3. Use **Signal Generator** to generate historical signals with custom parameters
4. Use **Backtester** to test the strategy with different capital and risk settings
5. Live signals are automatically pushed via WebSocket during market hours

## Market Timing

The strategy is optimized for NSE market hours:
- **Start**: 9:15 AM IST
- **End**: 3:30 PM IST
- **EOD Exit**: 3:25 PM IST (5 minutes before close)

## Notes

- Default timeframe is 5 minutes, but supports 1m, 3m, 15m, 30m, 1h
- PCR data is fetched from the existing `IndexLTP.meanStrikePCR` field
- Candle data is stored in the `CandleStick` table
- Signals are persisted in `brahmastra_signals` table
- Backtest results are stored in `brahmastra_backtest_results` table

