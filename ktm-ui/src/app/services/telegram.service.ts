import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, BehaviorSubject, catchError, of, tap, take } from 'rxjs';
import {
  TelegramNotificationRequest,
  TelegramNotificationResponse,
  TelegramNotification,
  TelegramConfigStatus,
  TelegramValidationResult,
  TelegramStats,
  TelegramSettings
} from '../models/telegram.model';

/**
 * Service for managing Telegram notifications.
 * Provides methods to send various types of alerts and manage notification settings.
 */
@Injectable({
  providedIn: 'root'
})
export class TelegramService {
  private baseUrl = '/api/telegram';

  // Observable state for configuration status
  private configStatusSubject = new BehaviorSubject<TelegramConfigStatus | null>(null);
  public configStatus$ = this.configStatusSubject.asObservable();

  constructor(private http: HttpClient) {
    // Load configuration status on service initialization
    this.refreshConfigStatus();
  }

  // ==================== Configuration ====================

  /**
   * Get current Telegram configuration status
   */
  getConfigStatus(): Observable<TelegramConfigStatus> {
    return this.http.get<TelegramConfigStatus>(`${this.baseUrl}/status`).pipe(
      tap(status => this.configStatusSubject.next(status)),
      catchError(err => {
        console.error('Error getting Telegram config status:', err);
        return of({
          enabled: false,
          configured: false,
          botTokenSet: false,
          chatIdSet: false,
          parseMode: 'HTML',
          categories: {
            tradeAlerts: false,
            predictionAlerts: false,
            deviationAlerts: false,
            systemAlerts: false,
            patternAlerts: false,
            marketAlerts: false
          }
        });
      })
    );
  }

  /**
   * Refresh the cached configuration status
   */
  refreshConfigStatus(): void {
    this.getConfigStatus().pipe(take(1)).subscribe();
  }

  /**
   * Check if Telegram is configured and enabled
   */
  isConfigured(): boolean {
    const status = this.configStatusSubject.value;
    return status?.configured ?? false;
  }

  /**
   * Validate bot configuration with Telegram API
   */
  validateConfiguration(): Observable<TelegramValidationResult> {
    return this.http.get<TelegramValidationResult>(`${this.baseUrl}/validate`);
  }

  // ==================== Settings Management ====================

  /**
   * Get current Telegram notification settings
   */
  getSettings(): Observable<TelegramSettings> {
    return this.http.get<TelegramSettings>(`${this.baseUrl}/settings`);
  }

  /**
   * Update Telegram notification settings
   */
  updateSettings(settings: TelegramSettings): Observable<TelegramSettings> {
    return this.http.put<TelegramSettings>(`${this.baseUrl}/settings`, settings);
  }

  /**
   * Check if a specific alert type is enabled
   */
  isAlertTypeEnabled(category: string, alertType: string): Observable<{ category: string; alertType: string; enabled: boolean }> {
    const params = new HttpParams()
      .set('category', category)
      .set('alertType', alertType);
    return this.http.get<{ category: string; alertType: string; enabled: boolean }>(`${this.baseUrl}/settings/alert-enabled`, { params });
  }

  // ==================== Send Notifications ====================

  /**
   * Send a custom notification
   */
  send(request: TelegramNotificationRequest): Observable<TelegramNotificationResponse> {
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send`, request);
  }

  /**
   * Send a simple text message
   */
  sendMessage(message: string, title?: string): Observable<TelegramNotificationResponse> {
    let params = new HttpParams().set('message', message);
    if (title) {
      params = params.set('title', title);
    }
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/simple`, null, { params });
  }

  /**
   * Send a trade alert
   */
  sendTradeAlert(title: string, message: string, tradeData?: Record<string, any>): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('title', title)
      .set('message', message);
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/trade-alert`, tradeData || {}, { params });
  }

  /**
   * Send a prediction alert
   */
  sendPredictionAlert(title: string, message: string, predictionData?: Record<string, any>): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('title', title)
      .set('message', message);
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/prediction-alert`, predictionData || {}, { params });
  }

  /**
   * Send a deviation alert
   */
  sendDeviationAlert(title: string, message: string, deviation: number): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('title', title)
      .set('message', message)
      .set('deviation', deviation.toString());
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/deviation-alert`, null, { params });
  }

  /**
   * Send a pattern alert
   */
  sendPatternAlert(patternName: string, direction: string, price: number, patternData?: Record<string, any>): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('patternName', patternName)
      .set('direction', direction)
      .set('price', price.toString());
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/pattern-alert`, patternData || {}, { params });
  }

  /**
   * Send a system alert
   */
  sendSystemAlert(title: string, message: string): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('title', title)
      .set('message', message);
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/system-alert`, null, { params });
  }

  /**
   * Send a market alert
   */
  sendMarketAlert(title: string, message: string, marketData?: Record<string, any>): Observable<TelegramNotificationResponse> {
    const params = new HttpParams()
      .set('title', title)
      .set('message', message);
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/send/market-alert`, marketData || {}, { params });
  }

  // ==================== Testing ====================

  /**
   * Send a test notification to verify configuration
   */
  sendTestNotification(): Observable<TelegramNotificationResponse> {
    return this.http.post<TelegramNotificationResponse>(`${this.baseUrl}/test`, null);
  }

  // ==================== History & Stats ====================

  /**
   * Get recent notifications (last 50)
   */
  getRecentNotifications(): Observable<TelegramNotification[]> {
    return this.http.get<TelegramNotification[]>(`${this.baseUrl}/notifications/recent`);
  }

  /**
   * Get today's notifications
   */
  getTodayNotifications(): Observable<TelegramNotification[]> {
    return this.http.get<TelegramNotification[]>(`${this.baseUrl}/notifications/today`);
  }

  /**
   * Get notifications by category
   */
  getNotificationsByCategory(category: string): Observable<TelegramNotification[]> {
    return this.http.get<TelegramNotification[]>(`${this.baseUrl}/notifications/category/${category}`);
  }

  /**
   * Get today's notification statistics
   */
  getTodayStats(): Observable<TelegramStats> {
    return this.http.get<TelegramStats>(`${this.baseUrl}/stats/today`);
  }

  // ==================== Helper Methods ====================

  /**
   * Quick method to send a deviation alert from analytics component
   */
  alertDeviation(deviation: number, actual: number, predicted: number, time: string): Observable<TelegramNotificationResponse> {
    const direction = actual > predicted ? 'ABOVE' : 'BELOW';
    const title = `Large Deviation Detected at ${time}`;
    const message = `Actual: ${actual.toFixed(2)} | Predicted: ${predicted.toFixed(2)}\nDeviation: ${deviation.toFixed(2)} points ${direction} prediction`;
    return this.sendDeviationAlert(title, message, deviation);
  }

  /**
   * Quick method to send a pattern detection alert
   */
  alertPattern(patternName: string, direction: string, price: number, confidence?: number): Observable<TelegramNotificationResponse> {
    const data: Record<string, any> = { pattern: patternName, direction, price };
    if (confidence !== undefined) {
      data['confidence'] = `${(confidence * 100).toFixed(1)}%`;
    }
    return this.sendPatternAlert(patternName, direction, price, data);
  }

  /**
   * Quick method to send a trade setup alert
   */
  alertTradeSetup(direction: string, entry: number, target: number, stopLoss: number, riskReward: number): Observable<TelegramNotificationResponse> {
    const title = `${direction} Trade Setup`;
    const message = `Entry: ₹${entry.toFixed(2)} | Target: ₹${target.toFixed(2)} | SL: ₹${stopLoss.toFixed(2)}`;
    const data = {
      direction,
      entry: `₹${entry.toFixed(2)}`,
      target: `₹${target.toFixed(2)}`,
      stopLoss: `₹${stopLoss.toFixed(2)}`,
      riskReward: riskReward.toFixed(2)
    };
    return this.sendTradeAlert(title, message, data);
  }

  /**
   * Quick method to send a market status alert
   */
  alertMarketStatus(niftyLTP: number, change: number, changePercent: number, vix: number): Observable<TelegramNotificationResponse> {
    const direction = change >= 0 ? '📈' : '📉';
    const title = `Market Update ${direction}`;
    const message = `Nifty: ₹${niftyLTP.toFixed(2)} (${change >= 0 ? '+' : ''}${change.toFixed(2)}, ${changePercent.toFixed(2)}%)`;
    const data = {
      nifty: `₹${niftyLTP.toFixed(2)}`,
      change: `${change >= 0 ? '+' : ''}${change.toFixed(2)}`,
      changePercent: `${changePercent.toFixed(2)}%`,
      vix: vix.toFixed(2)
    };
    return this.sendMarketAlert(title, message, data);
  }

  /**
   * Get category display info
   */
  getCategoryInfo(category: string): { icon: string; color: string; label: string } {
    const categoryMap: Record<string, { icon: string; color: string; label: string }> = {
      'TRADE_ALERT': { icon: '💹', color: '#4CAF50', label: 'Trade Alert' },
      'PREDICTION_ALERT': { icon: '🔮', color: '#9C27B0', label: 'Prediction Alert' },
      'DEVIATION_ALERT': { icon: '📉', color: '#FF9800', label: 'Deviation Alert' },
      'SYSTEM_ALERT': { icon: '🔧', color: '#607D8B', label: 'System Alert' },
      'PATTERN_ALERT': { icon: '🕯️', color: '#E91E63', label: 'Pattern Alert' },
      'MARKET_ALERT': { icon: '📈', color: '#2196F3', label: 'Market Alert' },
      'TEST': { icon: '🧪', color: '#795548', label: 'Test' },
      'CUSTOM': { icon: '📝', color: '#9E9E9E', label: 'Custom' }
    };
    return categoryMap[category] || categoryMap['CUSTOM'];
  }

  /**
   * Get priority display info
   */
  getPriorityInfo(priority: string): { icon: string; color: string; label: string } {
    const priorityMap: Record<string, { icon: string; color: string; label: string }> = {
      'CRITICAL': { icon: '🚨', color: '#F44336', label: 'Critical' },
      'HIGH': { icon: '⚠️', color: '#FF9800', label: 'High' },
      'NORMAL': { icon: '📊', color: '#2196F3', label: 'Normal' },
      'LOW': { icon: 'ℹ️', color: '#9E9E9E', label: 'Low' }
    };
    return priorityMap[priority] || priorityMap['NORMAL'];
  }
}
