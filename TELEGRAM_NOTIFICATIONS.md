# Telegram Notification Framework

This document explains how to set up and use the Telegram notification framework in KTManager.

## Overview

The Telegram notification framework allows the application to send real-time alerts to your Telegram account. It supports various notification categories and can be easily integrated with any service or job in the application.

## Setup Instructions

### Step 1: Create a Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` to create a new bot
3. Follow the instructions to name your bot
4. Copy the **Bot Token** provided by BotFather (e.g., `123456789:ABCdefGHIjklMNOpqrSTUvwxYZ`)

### Step 2: Get Your Chat ID

Option A - Using @userinfobot:
1. Search for `@userinfobot` on Telegram
2. Start a chat with it
3. It will display your user ID (this is your chat ID)

Option B - Using the Bot API:
1. Start a chat with your newly created bot
2. Send any message to the bot
3. Open this URL in your browser (replace YOUR_BOT_TOKEN):
   ```
   https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates
   ```
4. Look for `"chat":{"id":123456789}` - this number is your chat ID

### Step 3: Configure the Application

Edit `src/main/resources/application.yaml` and update the Telegram section:

```yaml
telegram:
  enabled: true
  botToken: YOUR_BOT_TOKEN_HERE
  defaultChatId: YOUR_CHAT_ID_HERE
  parseMode: HTML
  disableWebPagePreview: true
  disableNotification: false
  categories:
    tradeAlerts: true
    predictionAlerts: true
    deviationAlerts: true
    systemAlerts: true
    patternAlerts: true
    marketAlerts: true
```

## API Endpoints

### Configuration & Testing

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/telegram/status` | GET | Get current configuration status |
| `/api/telegram/validate` | GET | Validate bot configuration with Telegram |
| `/api/telegram/test` | POST | Send a test notification |

### Send Notifications

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/telegram/send` | POST | Send a custom notification (JSON body) |
| `/api/telegram/send/simple` | POST | Send simple message (params: message, title) |
| `/api/telegram/send/trade-alert` | POST | Send trade alert |
| `/api/telegram/send/prediction-alert` | POST | Send prediction alert |
| `/api/telegram/send/deviation-alert` | POST | Send deviation alert |
| `/api/telegram/send/pattern-alert` | POST | Send candlestick pattern alert |
| `/api/telegram/send/system-alert` | POST | Send system alert |
| `/api/telegram/send/market-alert` | POST | Send market alert |

### History & Statistics

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/telegram/notifications/recent` | GET | Get last 50 notifications |
| `/api/telegram/notifications/today` | GET | Get today's notifications |
| `/api/telegram/notifications/category/{category}` | GET | Get notifications by category |
| `/api/telegram/stats/today` | GET | Get today's notification statistics |

## Integration Examples

### Java - Inject and Use TelegramNotificationService

```java
@Service
public class MyService {

    @Autowired(required = false)
    private TelegramNotificationService telegramService;

    public void someMethod() {
        // Send a simple message
        if (telegramService != null && telegramService.isConfigured()) {
            telegramService.sendMessage("Alert Title", "This is the message");
        }

        // Send a trade alert with data
        Map<String, Object> tradeData = Map.of(
            "Symbol", "NIFTY",
            "Entry", "₹24500",
            "Target", "₹24600"
        );
        telegramService.sendTradeAlert("BULLISH Trade Setup", "Entry confirmed", tradeData);

        // Send asynchronously (non-blocking)
        telegramService.sendTradeAlertAsync("Trade Alert", "Message", tradeData);
    }
}
```

### Java - Using the Request Builder

```java
TelegramNotificationRequest request = TelegramNotificationRequest.builder()
    .category(TelegramNotificationRequest.NotificationCategory.TRADE_ALERT)
    .priority(TelegramNotificationRequest.NotificationPriority.HIGH)
    .title("Trade Alert")
    .message("NIFTY broke resistance at 24500")
    .data(Map.of("Price", 24500, "Direction", "BULLISH"))
    .source("TradingEngine")
    .eventTime(LocalDateTime.now())
    .build();

telegramService.send(request);
```

### Angular - Using TelegramService

```typescript
import { TelegramService } from './services/telegram.service';

@Component({...})
export class MyComponent {
    constructor(private telegramService: TelegramService) {}

    sendAlert() {
        // Simple message
        this.telegramService.sendMessage('Alert message', 'Title').subscribe();

        // Trade alert
        this.telegramService.sendTradeAlert(
            'BULLISH Setup',
            'Entry at ₹24500',
            { entry: 24500, target: 24600, stopLoss: 24400 }
        ).subscribe(response => {
            if (response.success) {
                console.log('Alert sent!');
            }
        });

        // Quick helper methods
        this.telegramService.alertTradeSetup('BULLISH', 24500, 24600, 24400, 2.0).subscribe();
        this.telegramService.alertDeviation(15.5, 24520, 24504.5, '10:30').subscribe();
        this.telegramService.alertPattern('Bullish Engulfing', 'BULLISH', 24500).subscribe();
    }
}
```

## Notification Categories

| Category | Description | Emoji |
|----------|-------------|-------|
| `TRADE_ALERT` | Trade setup and execution alerts | 💹 |
| `PREDICTION_ALERT` | Candle prediction notifications | 🔮 |
| `DEVIATION_ALERT` | Prediction deviation warnings | 📉 |
| `SYSTEM_ALERT` | System status notifications | 🔧 |
| `PATTERN_ALERT` | Candlestick pattern detections | 🕯️ |
| `MARKET_ALERT` | Market condition alerts | 📈 |
| `TEST` | Test notifications | 🧪 |
| `CUSTOM` | Custom notifications | 📝 |

## Priority Levels

| Priority | Description | Emoji |
|----------|-------------|-------|
| `CRITICAL` | Urgent alerts requiring immediate attention | 🚨 |
| `HIGH` | Important alerts | ⚠️ |
| `NORMAL` | Standard notifications | 📊 |
| `LOW` | Informational messages | ℹ️ |

## Message Format

Messages are formatted in HTML with the following structure:

```
🚨 💹 Trade Alert Title

Alert message content

• Entry: ₹24500
• Target: ₹24600
• Stop-Loss: ₹24400

📌 Source | 🕐 2026-01-17T10:30:00
```

## Rate Limiting

The service includes built-in rate limiting:
- Default: 1 message per second
- Configurable via `telegram.rateLimitMs`
- Automatic retry with exponential backoff

## Error Handling

- All notifications are persisted to the database for auditing
- Failed notifications are logged with error details
- Retry mechanism with configurable attempts
- Graceful degradation when Telegram is not configured

## Database Schema

Notifications are stored in the `telegram_notification` table:

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Primary key |
| telegram_message_id | BIGINT | Message ID from Telegram |
| chat_id | VARCHAR | Target chat ID |
| category | VARCHAR | Notification category |
| priority | VARCHAR | Priority level |
| title | VARCHAR | Message title |
| message | TEXT | Original message |
| formatted_message | TEXT | HTML formatted message |
| source | VARCHAR | Source service |
| success | BOOLEAN | Send status |
| error_message | TEXT | Error details if failed |
| sent_at | TIMESTAMP | When message was sent |

## Current Integrations

The Telegram notification service is currently integrated with:

1. **Analytics Component (Frontend)** - Sends deviation alerts when prediction deviation exceeds threshold
2. **CandlePredictionService** - Sends trade setup alerts for high-confidence setups
3. **ReversalPatternService** - Sends pattern detection alerts for candlestick patterns
4. **InternalOrderBlockService** - Sends IOB signals for NIFTY/SENSEX with confidence > 51%

### Internal Order Block (IOB) Alerts

IOB alerts are automatically sent when:
- A new Internal Order Block is detected
- The instrument is NIFTY or SENSEX
- Signal confidence is greater than 51%

The alert includes:
- IOB type (Bullish/Bearish)
- Zone levels (High/Low/Midpoint)
- Current price and distance to zone
- Trade setup (Entry, Stop-Loss, Target)
- Risk:Reward ratio
- FVG (Fair Value Gap) presence
- Timeframe

## Disabling Notifications

### Disable All Notifications
```yaml
telegram:
  enabled: false
```

### Disable Specific Categories
```yaml
telegram:
  categories:
    tradeAlerts: false
    predictionAlerts: true
    # ... etc
```

## Troubleshooting

### "Telegram notifications are disabled or not configured"
- Ensure `telegram.enabled: true` in application.yaml
- Verify bot token and chat ID are set

### Messages not being received
1. Test using `/api/telegram/test` endpoint
2. Check `/api/telegram/validate` to verify bot token
3. Ensure you've started a chat with the bot
4. Check application logs for errors

### Rate limit issues
- Telegram has a limit of ~30 messages per second per bot
- Increase `telegram.rateLimitMs` if you're hitting limits
