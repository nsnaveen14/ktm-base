/**
 * Models for Telegram notification integration
 */

// ==================== Settings Models ====================

export interface TelegramSettings {
  enabled: boolean;
  botToken?: string;
  defaultChatId?: string;
  parseMode: string;
  disableWebPagePreview: boolean;
  disableNotification: boolean;
  connectionTimeout: number;
  readTimeout: number;
  maxRetries: number;
  retryDelayMs: number;
  rateLimitMs: number;
  tradeAlerts: TradeAlertSettings;
  predictionAlerts: PredictionAlertSettings;
  deviationAlerts: DeviationAlertSettings;
  systemAlerts: SystemAlertSettings;
  patternAlerts: PatternAlertSettings;
  marketAlerts: MarketAlertSettings;
}

export interface TradeAlertSettings {
  enabled: boolean;
  iobAlerts: boolean;
  iobMitigationAlerts: boolean;
  tradeSetupAlerts: boolean;
  tradeDecisionAlerts: boolean;
  liquidityZoneAlerts: boolean;
  brahmastraAlerts: boolean;
  minConfidenceThreshold: number;
}

export interface PredictionAlertSettings {
  enabled: boolean;
  candlePredictionAlerts: boolean;
  trendChangeAlerts: boolean;
  targetHitAlerts: boolean;
}

export interface DeviationAlertSettings {
  enabled: boolean;
  deviationThresholdPoints: number;
  largeDeviationOnly: boolean;
}

export interface SystemAlertSettings {
  enabled: boolean;
  tickerConnectionAlerts: boolean;
  jobStatusAlerts: boolean;
  errorAlerts: boolean;
}

export interface PatternAlertSettings {
  enabled: boolean;
  bullishPatternAlerts: boolean;
  bearishPatternAlerts: boolean;
  minConfidenceThreshold: number;
}

export interface MarketAlertSettings {
  enabled: boolean;
  marketOpenCloseAlerts: boolean;
  significantMoveAlerts: boolean;
  significantMoveThresholdPercent: number;
}

// ==================== Notification Models ====================

export interface TelegramNotificationRequest {
  category?: NotificationCategory;
  priority?: NotificationPriority;
  title?: string;
  message?: string;
  chatId?: string;
  parseMode?: string;
  disableWebPagePreview?: boolean;
  disableNotification?: boolean;
  data?: Record<string, any>;
  source?: string;
  eventTime?: string;
}

export interface TelegramNotificationResponse {
  success: boolean;
  messageId?: number;
  chatId?: string;
  error?: string;
  httpStatus?: number;
  timestamp?: string;
  category?: NotificationCategory;
}

export interface TelegramNotification {
  id: number;
  telegramMessageId?: number;
  chatId?: string;
  category?: string;
  priority?: string;
  title?: string;
  message?: string;
  formattedMessage?: string;
  source?: string;
  success: boolean;
  errorMessage?: string;
  httpStatus?: number;
  retryCount: number;
  sentAt?: string;
  eventTime?: string;
  createdAt?: string;
}

export interface TelegramConfigStatus {
  enabled: boolean;
  configured: boolean;
  botTokenSet: boolean;
  chatIdSet: boolean;
  parseMode: string;
  categories: TelegramCategorySettings;
}

export interface TelegramCategorySettings {
  tradeAlerts: boolean;
  predictionAlerts: boolean;
  deviationAlerts: boolean;
  systemAlerts: boolean;
  patternAlerts: boolean;
  marketAlerts: boolean;
}

export interface TelegramValidationResult {
  configured: boolean;
  valid: boolean;
  error?: string;
  botUsername?: string;
  botFirstName?: string;
  botId?: number;
}

export interface TelegramStats {
  totalSent: number;
  successful: number;
  failed: number;
  byCategory: Record<string, number>;
}

export enum NotificationCategory {
  TRADE_ALERT = 'TRADE_ALERT',
  PREDICTION_ALERT = 'PREDICTION_ALERT',
  DEVIATION_ALERT = 'DEVIATION_ALERT',
  SYSTEM_ALERT = 'SYSTEM_ALERT',
  PATTERN_ALERT = 'PATTERN_ALERT',
  MARKET_ALERT = 'MARKET_ALERT',
  TEST = 'TEST',
  CUSTOM = 'CUSTOM'
}

export enum NotificationPriority {
  LOW = 'LOW',
  NORMAL = 'NORMAL',
  HIGH = 'HIGH',
  CRITICAL = 'CRITICAL'
}

/**
 * Helper class to build notification requests easily
 */
export class TelegramNotificationBuilder {
  private request: TelegramNotificationRequest = {};

  static create(): TelegramNotificationBuilder {
    return new TelegramNotificationBuilder();
  }

  category(category: NotificationCategory): TelegramNotificationBuilder {
    this.request.category = category;
    return this;
  }

  priority(priority: NotificationPriority): TelegramNotificationBuilder {
    this.request.priority = priority;
    return this;
  }

  title(title: string): TelegramNotificationBuilder {
    this.request.title = title;
    return this;
  }

  message(message: string): TelegramNotificationBuilder {
    this.request.message = message;
    return this;
  }

  data(data: Record<string, any>): TelegramNotificationBuilder {
    this.request.data = data;
    return this;
  }

  source(source: string): TelegramNotificationBuilder {
    this.request.source = source;
    return this;
  }

  silent(silent: boolean = true): TelegramNotificationBuilder {
    this.request.disableNotification = silent;
    return this;
  }

  build(): TelegramNotificationRequest {
    this.request.eventTime = new Date().toISOString();
    return this.request;
  }

  // Quick builders
  static tradeAlert(title: string, message: string, data?: Record<string, any>): TelegramNotificationRequest {
    return TelegramNotificationBuilder.create()
      .category(NotificationCategory.TRADE_ALERT)
      .priority(NotificationPriority.HIGH)
      .title(title)
      .message(message)
      .data(data || {})
      .source('UI')
      .build();
  }

  static predictionAlert(title: string, message: string, data?: Record<string, any>): TelegramNotificationRequest {
    return TelegramNotificationBuilder.create()
      .category(NotificationCategory.PREDICTION_ALERT)
      .priority(NotificationPriority.NORMAL)
      .title(title)
      .message(message)
      .data(data || {})
      .source('UI')
      .build();
  }

  static deviationAlert(title: string, message: string, deviation: number): TelegramNotificationRequest {
    const priority = deviation >= 20 ? NotificationPriority.CRITICAL :
                     deviation >= 10 ? NotificationPriority.HIGH :
                     NotificationPriority.NORMAL;
    return TelegramNotificationBuilder.create()
      .category(NotificationCategory.DEVIATION_ALERT)
      .priority(priority)
      .title(title)
      .message(message)
      .data({ deviation: `${deviation} points` })
      .source('UI')
      .build();
  }

  static patternAlert(patternName: string, direction: string, price: number): TelegramNotificationRequest {
    return TelegramNotificationBuilder.create()
      .category(NotificationCategory.PATTERN_ALERT)
      .priority(NotificationPriority.HIGH)
      .title(`Pattern Detected: ${patternName}`)
      .message(`${direction} pattern detected at ₹${price.toFixed(2)}`)
      .data({ pattern: patternName, direction, price })
      .source('UI')
      .build();
  }

  static systemAlert(title: string, message: string): TelegramNotificationRequest {
    return TelegramNotificationBuilder.create()
      .category(NotificationCategory.SYSTEM_ALERT)
      .priority(NotificationPriority.NORMAL)
      .title(title)
      .message(message)
      .source('UI')
      .build();
  }
}
