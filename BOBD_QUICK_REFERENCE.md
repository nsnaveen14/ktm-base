# BOBD Failures - Quick Reference Card

## 🎯 Strategy at a Glance

**Name**: Breakout/Breakdown Failure (BOBD/Fakeout Strategy)  
**Market**: Nifty 50 Weekly Options (ATM/OTM)  
**Style**: Intraday Reversal Trading  
**Risk**: Conservative (1% per trade)  
**Win Rate Target**: 55-65%  

## 📍 Key Levels

| Level | Description | Usage |
|-------|-------------|-------|
| **PDH** | Prior Day High | Resistance level |
| **PDL** | Prior Day Low | Support level |
| **ORH_15** | Opening Range High (15 min) | Early resistance |
| **ORL_15** | Opening Range Low (15 min) | Early support |
| **ORH_30** | Opening Range High (30 min) | Stronger resistance |
| **ORL_30** | Opening Range Low (30 min) | Stronger support |

## 🎯 Signal Types

### Bullish Fakeout (Buy PUT)
```
Price breaks above PDH/ORH
→ Fails to sustain (0.2-0.5%)
→ Reclaims level within 2-5 candles
→ Bearish reversal → BUY PUT
```

### Bearish Fakeout (Buy CALL)
```
Price breaks below PDL/ORL
→ Fails to sustain (0.2-0.5%)
→ Reclaims level within 2-5 candles
→ Bullish reversal → BUY CALL
```

## ⏰ Trading Hours

| Time | Activity |
|------|----------|
| 9:00 AM | System calculates PDH/PDL |
| 9:15 AM | Market opens, OR calculation starts |
| 9:30 AM | 15-min OR complete |
| 9:45 AM | **Trading window opens** |
| 10:00 AM - 1:00 PM | **Best trading hours** |
| 2:30 PM | **Last entry time** |
| 3:15 PM | **Force close all positions** |
| 3:30 PM | Market closes, daily summary |

## 💰 Trade Setup (Conservative)

```
Entry:       ATM/OTM Option
Risk:        1% of account
Stop Loss:   25 points on premium
Target 1:    50 points (1:2 RR)
Target 2:    75 points (1:3 RR)
Position:    1-2 lots
Max Trades:  1 per day
```

## 🎛️ Filters (Default)

✅ Trading Window: 9:45 AM - 2:30 PM  
✅ ADX > 25 (trend strength)  
✅ VIX < 20 (low volatility)  
✅ Volume divergence present  
✅ Avoid expiry days (Thursday)  
✅ Avoid RBI policy days  
✅ Candle pattern confirmation  

## 📱 UI Navigation

### Dashboard Tab
- Key levels display
- Today's signals summary
- Performance stats
- Active configuration

### Signals Tab
- All detected signals
- Filters (type, level, status)
- Signal details modal
- Confirm/Cancel actions

### Active Trades Tab
- Live position monitoring
- P&L tracking
- Close trade button

### Performance Tab
- Win rate
- Profit factor
- Expectancy
- Trade statistics

### Backtest Tab
- Historical testing
- Date range selection
- Configuration comparison
- Results analysis

### Configuration Tab
- Strategy parameters
- Risk management
- Filter settings
- Multiple profiles

## 🔄 Workflow

```mermaid
1. SIGNAL DETECTED
   ↓
2. REVIEW & CONFIRM
   ↓
3. GENERATE TRADE SETUP
   ↓
4. EXECUTE TRADE
   ↓
5. MARK AS TRADED
   ↓
6. MONITOR (SL/Targets)
   ↓
7. CLOSE TRADE
   ↓
8. RECORD OUTCOME
```

## 🚦 Signal Status

| Status | Meaning | Action |
|--------|---------|--------|
| 🟡 DETECTED | Signal found | Review & confirm |
| 🟢 CONFIRMED | Ready for trade | Execute trade |
| 🔵 ENTRY_TRIGGERED | Entry price hit | Enter position |
| 🟣 ACTIVE | Position open | Monitor |
| ✅ TARGET_HIT | Target reached | Close position |
| ❌ SL_HIT | Stop loss hit | Exit position |
| ⚪ EXPIRED | Signal expired | Ignore |
| ⚫ CANCELLED | Signal cancelled | Ignore |

## 📞 API Quick Reference

### Scan for Signals
```bash
GET /api/bobd/scan/256265
```

### Get Today's Signals
```bash
GET /api/bobd/signals/today?instrumentToken=256265
```

### Confirm Signal
```bash
POST /api/bobd/signals/123/confirm
```

### Mark as Traded
```bash
POST /api/bobd/signals/123/mark-traded
Body: { "entryPremium": 150.50 }
```

### Close Trade
```bash
POST /api/bobd/signals/123/close-trade
Body: { "exitPremium": 200.75, "exitReason": "TARGET_1" }
```

### Get Performance
```bash
GET /api/bobd/performance/stats
```

## 📊 Performance Metrics

```
Win Rate:       55-65%
Profit Factor:  1.5-2.0
Average RR:     1:1.8
Max Drawdown:   10-15%
Monthly Return: 3-5%
```

## ⚡ Quick Actions

### Pre-Market Checklist
- [ ] Check VIX level
- [ ] Review event calendar
- [ ] Set active configuration
- [ ] Verify telegram notifications

### During Market
- [ ] Monitor key levels
- [ ] Scan for signals (auto)
- [ ] Confirm valid signals
- [ ] Execute trades promptly
- [ ] Monitor active positions

### Post-Market
- [ ] Review daily summary
- [ ] Analyze closed trades
- [ ] Update trade journal
- [ ] Plan for next day

## 🔧 Configuration Presets

### Conservative (Default)
```
Risk: 1%, SL: 25 pts
Targets: 1:2, 1:3
Max Trades: 1/day
Filters: Strict
```

### Moderate
```
Risk: 1.5%, SL: 20-30 pts
Targets: 1:2, 1:3, trailing
Max Trades: 2/day
Filters: Medium
```

### Aggressive
```
Risk: 2%, SL: 20 pts
Targets: 1:3+
Max Trades: 3/day
Filters: Minimal
```

## ⚠️ Risk Rules

1. Never exceed 1-2% risk per trade
2. Always use stop loss
3. Book partial profits at T1
4. Force close by 3:15 PM
5. Max 1-2 trades per day
6. Avoid revenge trading
7. No trading on event days
8. Respect VIX limits

## 📞 Telegram Notifications

- 🔔 Signal detected
- ✅ Entry triggered
- 🎯 Target hit
- 🛑 Stop loss hit
- 📊 Daily summary (3:30 PM)

## 🆘 Troubleshooting

**No signals?**
→ Check trading window, VIX, event day

**Trade not executing?**
→ Verify signal confirmed, check market status

**Performance poor?**
→ Review losing trades, adjust configuration

**Telegram not working?**
→ Check bot settings, enable notifications in config

## 📚 Resources

- **Full Guide**: `BOBD_IMPLEMENTATION_GUIDE.md`
- **Summary**: `BOBD_IMPLEMENTATION_SUMMARY.md`
- **Code**: `src/.../BOBDFailureServiceImpl.java`
- **UI**: `ktm-ui/src/app/components/bobd-failures/`

## 🎓 Remember

✅ Patience is key - wait for quality setups  
✅ Risk management first - protect capital  
✅ Follow the system - don't deviate  
✅ Keep learning - review and improve  
✅ Stay disciplined - emotions are enemy  

---

**Quick Access URL**: `http://localhost:4200/bobd-failures`  
**API Base**: `http://localhost:8080/api/bobd`  
**Version**: 1.0.0  
**Updated**: January 2026  

**Print this card and keep it handy! 📋**
