import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatDividerModule } from '@angular/material/divider';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { Subscription, interval } from 'rxjs';
import { PerformanceService } from '../../services/performance.service';
//import { WebSocketService } from '../../services/websocket.service';
import { WebsocketService } from '../../services/web-socket.service';
import { AutoTradingConfig, AutoTradingStats, OpenPosition, AutoTradeOrder } from '../../models/performance.model';

@Component({
  selector: 'app-auto-trading-config',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatSlideToggleModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatExpansionModule,
    MatDividerModule,
    MatSnackBarModule
  ],
  templateUrl: './auto-trading-config.component.html',
  styleUrls: ['./auto-trading-config.component.css']
})
export class AutoTradingConfigComponent implements OnInit, OnDestroy {
  isLoading = false;

  // Configuration
  config: AutoTradingConfig = {
    autoTradingEnabled: false,
    paperTradingMode: true,
    entryType: 'ZONE_TOUCH',
    minConfidence: 65,
    requireFvg: false,
    requireTrendAlignment: true,
    requireInstitutionalVolume: false,
    maxPositionSize: 50,
    maxLotsPerTrade: 1,
    maxOpenPositions: 2,
    maxDailyTrades: 5,
    maxDailyLoss: 10000,
    useDynamicSL: true,
    slAtrMultiplier: 1.5,
    enableTrailingSL: true,
    trailingSLTrigger: 'TARGET_1',
    trailingSLDistancePoints: 20,
    bookPartialProfits: true,
    partialProfitPercent: 50,
    partialProfitAt: 'TARGET_1',
    defaultExitTarget: 'TARGET_2',
    exitAtMarketClose: true,
    marketCloseTime: '15:20',
    tradeStartTime: '09:20',
    tradeEndTime: '15:00',
    avoidFirstCandle: true,
    enabledInstruments: '256265,265',
    defaultProductType: 'MIS'
  };

  // Status
  status: AutoTradingStats | null = null;

  // Positions and Orders
  openPositions: OpenPosition[] = [];
  pendingOrders: AutoTradeOrder[] = [];
  todaysTrades: AutoTradeOrder[] = [];
  activityLog: any[] = [];

  private subscriptions: Subscription[] = [];
  private refreshInterval: any;

  readonly entryTypes = [
    { value: 'ZONE_TOUCH', label: 'Zone Touch' },
    { value: 'ZONE_MIDPOINT', label: 'Zone Midpoint' },
    { value: 'CONFIRMATION_CANDLE', label: 'Confirmation Candle' }
  ];

  readonly trailTriggers = [
    { value: 'TARGET_1', label: 'At Target 1' },
    { value: 'BREAKEVEN', label: 'At Breakeven' },
    { value: 'POINTS', label: 'Fixed Points' }
  ];

  readonly exitTargets = [
    { value: 'TARGET_1', label: 'Target 1' },
    { value: 'TARGET_2', label: 'Target 2' },
    { value: 'TARGET_3', label: 'Target 3' }
  ];

  constructor(
    private performanceService: PerformanceService,
    private wsService: WebsocketService,
    private snackBar: MatSnackBar
  ) {}

  ngOnInit(): void {
    this.loadConfig();
    this.loadStatus();
    this.loadPositionsAndOrders();

    // Subscribe to auto trade updates
    this.subscriptions.push(
      this.wsService.getAutoTradeUpdates().subscribe(update => {
        this.handleAutoTradeUpdate(update);
      })
    );

    // Refresh status every 30 seconds
    this.refreshInterval = setInterval(() => {
      this.loadStatus();
      this.loadPositionsAndOrders();
    }, 30000);
  }

  ngOnDestroy(): void {
    this.subscriptions.forEach(sub => sub.unsubscribe());
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
  }

  // ==================== Data Loading ====================

  loadConfig(): void {
    this.performanceService.getAutoTradingConfig().subscribe({
      next: (config) => {
        this.config = { ...this.config, ...config };
      },
      error: (err) => {
        console.error('Error loading config:', err);
      }
    });
  }

  loadStatus(): void {
    this.performanceService.getAutoTradingStatus().subscribe({
      next: (status) => {
        this.status = status;
      },
      error: (err) => {
        console.error('Error loading status:', err);
      }
    });
  }

  loadPositionsAndOrders(): void {
    this.performanceService.getOpenPositions().subscribe({
      next: (positions) => {
        this.openPositions = positions;
      },
      error: (err) => console.error('Error loading positions:', err)
    });

    this.performanceService.getPendingOrders().subscribe({
      next: (orders) => {
        this.pendingOrders = orders;
      },
      error: (err) => console.error('Error loading orders:', err)
    });

    this.performanceService.getTodaysAutoTrades().subscribe({
      next: (trades) => {
        this.todaysTrades = trades;
      },
      error: (err) => console.error('Error loading trades:', err)
    });

    this.performanceService.getActivityLog(20).subscribe({
      next: (log) => {
        this.activityLog = log;
      },
      error: (err) => console.error('Error loading activity log:', err)
    });
  }

  // ==================== Configuration Actions ====================

  saveConfig(): void {
    this.isLoading = true;
    this.performanceService.updateAutoTradingConfig(this.config).subscribe({
      next: () => {
        this.showNotification('Configuration saved', 'success');
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error saving config:', err);
        this.showNotification('Failed to save configuration', 'error');
        this.isLoading = false;
      }
    });
  }

  toggleAutoTrading(): void {
    if (this.config.autoTradingEnabled) {
      this.performanceService.enableAutoTrading().subscribe({
        next: () => {
          this.showNotification('Auto trading enabled', 'success');
          this.loadStatus();
        },
        error: (err) => {
          console.error('Error enabling auto trading:', err);
          this.config.autoTradingEnabled = false;
        }
      });
    } else {
      this.performanceService.disableAutoTrading().subscribe({
        next: () => {
          this.showNotification('Auto trading disabled', 'success');
          this.loadStatus();
        },
        error: (err) => {
          console.error('Error disabling auto trading:', err);
          this.config.autoTradingEnabled = true;
        }
      });
    }
  }

  // ==================== Position Actions ====================

  closePosition(position: OpenPosition): void {
    const exitPrice = prompt('Enter exit price:');
    if (!exitPrice) return;

    this.performanceService.closePosition(position.positionId, parseFloat(exitPrice), 'MANUAL').subscribe({
      next: () => {
        this.showNotification('Position closed', 'success');
        this.loadPositionsAndOrders();
      },
      error: (err) => {
        console.error('Error closing position:', err);
        this.showNotification('Failed to close position', 'error');
      }
    });
  }

  closeAllPositions(): void {
    if (!confirm('Are you sure you want to close all positions?')) return;

    this.performanceService.closeAllPositions().subscribe({
      next: () => {
        this.showNotification('All positions closed', 'success');
        this.loadPositionsAndOrders();
      },
      error: (err) => {
        console.error('Error closing all positions:', err);
        this.showNotification('Failed to close positions', 'error');
      }
    });
  }

  cancelOrder(orderId: string): void {
    this.performanceService.cancelAutoOrder(orderId).subscribe({
      next: () => {
        this.showNotification('Order cancelled', 'success');
        this.loadPositionsAndOrders();
      },
      error: (err) => {
        console.error('Error cancelling order:', err);
        this.showNotification('Failed to cancel order', 'error');
      }
    });
  }

  updateTrailingStop(position: OpenPosition): void {
    const newSL = prompt('Enter new stop loss:');
    if (!newSL) return;

    this.performanceService.updateTrailingStop(position.positionId, parseFloat(newSL)).subscribe({
      next: () => {
        this.showNotification('Trailing stop updated', 'success');
        this.loadPositionsAndOrders();
      },
      error: (err) => {
        console.error('Error updating trailing stop:', err);
        this.showNotification('Failed to update trailing stop', 'error');
      }
    });
  }

  // ==================== Helpers ====================

  handleAutoTradeUpdate(update: any): void {
    console.log('Auto trade update:', update);
    this.loadPositionsAndOrders();
  }

  calculateUnrealizedPnl(position: OpenPosition): number {
    if (!position.currentPrice || !position.entryPrice) return 0;
    const isLong = position.direction === 'LONG';
    const pnlPoints = isLong ?
      position.currentPrice - position.entryPrice :
      position.entryPrice - position.currentPrice;
    return pnlPoints * (position.quantity || 1);
  }

  formatCurrency(value: number | undefined | null): string {
    if (value === undefined || value === null) return '--';
    return '₹' + value.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  formatTime(timestamp: string | undefined): string {
    if (!timestamp) return '--';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' });
  }

  getPnlClass(pnl: number): string {
    return pnl >= 0 ? 'pnl-positive' : 'pnl-negative';
  }

  getDirectionClass(direction: string): string {
    return direction === 'LONG' ? 'direction-long' : 'direction-short';
  }

  private showNotification(message: string, type: 'success' | 'error'): void {
    this.snackBar.open(message, 'Close', {
      duration: 3000,
      horizontalPosition: 'right',
      verticalPosition: 'top',
      panelClass: type === 'error' ? ['error-snackbar'] : ['success-snackbar']
    });
  }

  trackByValue(_: number, item: { value: any }): any { return item.value; }
  trackByPosition(_: number, pos: OpenPosition): string { return pos.positionId; }
  trackByOrder(_: number, order: AutoTradeOrder): string { return order.orderId; }
  trackByActivity(i: number, item: any): any { return item.timestamp ?? i; }
}
