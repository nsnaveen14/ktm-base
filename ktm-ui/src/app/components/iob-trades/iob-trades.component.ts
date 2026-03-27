import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatBadgeModule } from '@angular/material/badge';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { FormsModule } from '@angular/forms';
import { DataService } from '../../services/data.service';
import { interval, Subscription } from 'rxjs';
import { InternalOrderBlock } from '../../models/iob.model';
import { LiveTickComponent } from '../live-tick/live-tick.component';

interface TradeTimeline {
  iob: InternalOrderBlock;
  events: TimelineEvent[];
}

interface TimelineEvent {
  type: 'DETECTION' | 'ENTRY' | 'TARGET_1' | 'TARGET_2' | 'TARGET_3' | 'STOP_LOSS';
  time?: string;
  price?: number;
  description: string;
  icon: string;
  color: string;
}

@Component({
  selector: 'app-iob-trades',
  standalone: true,
  imports: [
    CommonModule,
    MatCardModule,
    MatTableModule,
    MatButtonModule,
    MatIconModule,
    MatTabsModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatDividerModule,
    MatBadgeModule,
    MatSnackBarModule,
    MatSlideToggleModule,
    FormsModule,
    LiveTickComponent
  ],
  templateUrl: './iob-trades.component.html',
  styleUrls: ['./iob-trades.component.css']
})
export class IobTradesComponent implements OnInit, OnDestroy {

  isLoading = false;
  autoRefresh = true;
  refreshInterval = 10000; // 10 seconds for trade monitoring

  selectedInstrument: 'NIFTY' | 'ALL' = 'ALL';
  selectedFilter: 'ACTIVE' | 'COMPLETED' | 'ALL' = 'ALL';

  // Toggle to show/hide older trades (default: hide, only show today's)
  showOlderTrades = false;

  niftyToken = 256265;

  // All IOBs from API
  allIOBs: InternalOrderBlock[] = [];

  // Live trades (active positions)
  liveTrades: InternalOrderBlock[] = [];

  // Historical/Completed trades
  completedTrades: InternalOrderBlock[] = [];

  // Trade timelines for detailed view
  tradeTimelines: TradeTimeline[] = [];

  // Statistics
  stats = {
    activeTrades: 0,
    todayWins: 0,
    todayLosses: 0,
    todayPnL: 0,
    winRate: 0
  };

  private refreshSub?: Subscription;

  constructor(
    private dataService: DataService,
    private snackBar: MatSnackBar
  ) {}

  ngOnInit(): void {
    this.loadAllData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
  }

  loadAllData(): void {
    this.isLoading = true;

    // Load IOB data from all indices
    this.dataService.getAllIndicesIOBData().subscribe({
      next: (data: any) => {
        console.log('IOB Trades - Raw data received:', {
          NIFTY: {
            activeIOBs: data.NIFTY?.activeIOBs?.length || 0,
            mitigatedIOBs: data.NIFTY?.mitigatedIOBs?.length || 0,
            completedIOBs: data.NIFTY?.completedIOBs?.length || 0
          }
        });

        // Extract all categories of IOBs
        const niftyActive = data.NIFTY?.activeIOBs || [];
        const niftyMitigated = data.NIFTY?.mitigatedIOBs || [];
        const niftyCompleted = data.NIFTY?.completedIOBs || [];

        // Active Trades = FRESH IOBs (not yet mitigated, waiting for entry)
        // These come from activeIOBs which contains FRESH status IOBs
        this.liveTrades = this.deduplicateIOBs([...niftyActive])
          .filter(iob => iob.status === 'FRESH');

        // Completed trades = IOBs where trade ended (SL or T3 hit)
        this.completedTrades = this.deduplicateIOBs([...niftyCompleted]);

        // All IOBs for reference (mitigated + completed, for timeline display)
        this.allIOBs = this.deduplicateIOBs([
          ...niftyMitigated,
          ...niftyCompleted
        ]);

        console.log('IOB Trades - Active (Fresh) trades:', this.liveTrades.length);
        console.log('IOB Trades - Completed trades:', this.completedTrades.length);

        // Calculate statistics
        this.calculateStats();

        // Apply filters and build timelines
        this.applyFilters();

        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading IOB trade data:', err);
        this.isLoading = false;
      }
    });
  }

  /**
   * Remove duplicate IOBs by ID
   */
  private deduplicateIOBs(iobs: InternalOrderBlock[]): InternalOrderBlock[] {
    const uniqueMap = new Map<number, InternalOrderBlock>();
    iobs.forEach(iob => {
      if (iob.id) {
        uniqueMap.set(iob.id, iob);
      }
    });
    return Array.from(uniqueMap.values());
  }

  private buildTradeTimelines(iobs: InternalOrderBlock[]): void {
    this.tradeTimelines = iobs
      .map(iob => ({
        iob,
        events: this.buildTimelineEvents(iob)
      }))
      .sort((a, b) => {
        // Sort by detection time for FRESH, or entry time for mitigated/completed
        const timeA = a.iob.entryTriggeredTime ? new Date(a.iob.entryTriggeredTime).getTime() :
                      a.iob.mitigationTime ? new Date(a.iob.mitigationTime).getTime() :
                      a.iob.detectionTimestamp ? new Date(a.iob.detectionTimestamp).getTime() : 0;
        const timeB = b.iob.entryTriggeredTime ? new Date(b.iob.entryTriggeredTime).getTime() :
                      b.iob.mitigationTime ? new Date(b.iob.mitigationTime).getTime() :
                      b.iob.detectionTimestamp ? new Date(b.iob.detectionTimestamp).getTime() : 0;
        return timeB - timeA; // Most recent first
      });
  }

  /**
   * Check if IOB has triggered an entry - used to determine if it should be shown in trades.
   * Trade tracking should only show IOBs where:
   * - Entry was triggered when IOB was FRESH (entryTriggeredTime is set), OR
   * - IOB was mitigated (mitigationTime is set), OR
   * - IOB has completed (target hit or stop loss hit)
   */
  private hasTriggeredEntry(iob: InternalOrderBlock): boolean {
    // Must have actual entry time set - this indicates price hit the zone
    // and trade tracking started
    return !!(
      iob.entryTriggeredTime ||
      iob.mitigationTime ||
      iob.stopLossHitTime ||
      iob.target1HitTime ||
      iob.target2HitTime ||
      iob.target3HitTime
    );
  }

  /**
   * Check if trade has completed (either SL hit or T3 hit)
   */
  private isTradeCompleted(iob: InternalOrderBlock): boolean {
    return !!(
      iob.stopLossHitTime ||
      iob.target3AlertSent ||
      iob.status === 'STOPPED' ||
      iob.status === 'COMPLETED' ||
      iob.tradeOutcome === 'WIN' ||
      iob.tradeOutcome === 'LOSS'
    );
  }

  private buildTimelineEvents(iob: InternalOrderBlock): TimelineEvent[] {
    const events: TimelineEvent[] = [];

    // Detection event
    if (iob.detectionTimestamp) {
      events.push({
        type: 'DETECTION',
        time: iob.detectionTimestamp,
        price: iob.zoneMidpoint,
        description: `IOB Signal Detected - ${iob.obType?.replace('_IOB', '')} zone identified`,
        icon: 'radar',
        color: 'blue'
      });
    }

    // Entry event
    if (iob.entryTriggeredTime || iob.mitigationTime) {
      events.push({
        type: 'ENTRY',
        time: iob.entryTriggeredTime || iob.mitigationTime,
        price: iob.actualEntryPrice || iob.entryPrice,
        description: `Entry Triggered - ${iob.tradeDirection} position opened at ₹${this.formatNumber(iob.actualEntryPrice || iob.entryPrice)}`,
        icon: 'login',
        color: iob.tradeDirection === 'LONG' ? 'green' : 'red'
      });
    }

    // Target 1 hit
    if (iob.target1HitTime || iob.target1AlertSent) {
      events.push({
        type: 'TARGET_1',
        time: iob.target1HitTime,
        price: iob.target1HitPrice || iob.target1,
        description: `Target 1 Hit - ₹${this.formatNumber(iob.target1HitPrice || iob.target1)} (1.5R)`,
        icon: 'check_circle',
        color: 'teal'
      });
    }

    // Target 2 hit
    if (iob.target2HitTime || iob.target2AlertSent) {
      events.push({
        type: 'TARGET_2',
        time: iob.target2HitTime,
        price: iob.target2HitPrice || iob.target2,
        description: `Target 2 Hit - ₹${this.formatNumber(iob.target2HitPrice || iob.target2)} (2.5R)`,
        icon: 'check_circle',
        color: 'green'
      });
    }

    // Target 3 hit
    if (iob.target3HitTime || iob.target3AlertSent) {
      events.push({
        type: 'TARGET_3',
        time: iob.target3HitTime,
        price: iob.target3HitPrice || iob.target3,
        description: `Target 3 Hit - ₹${this.formatNumber(iob.target3HitPrice || iob.target3)} (4R) - FULL WIN!`,
        icon: 'emoji_events',
        color: 'gold'
      });
    }

    // Stop Loss hit
    if (iob.stopLossHitTime || iob.status === 'STOPPED') {
      events.push({
        type: 'STOP_LOSS',
        time: iob.stopLossHitTime,
        price: iob.stopLossHitPrice || iob.stopLoss,
        description: `Stop Loss Hit - ₹${this.formatNumber(iob.stopLossHitPrice || iob.stopLoss)} - Trade closed`,
        icon: 'cancel',
        color: 'red'
      });
    }

    // Sort by time
    return events.sort((a, b) => {
      const timeA = a.time ? new Date(a.time).getTime() : 0;
      const timeB = b.time ? new Date(b.time).getTime() : 0;
      return timeA - timeB;
    });
  }

  private calculateStats(): void {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    // Active trades = FRESH IOBs (waiting for entry to be triggered)
    this.stats.activeTrades = this.liveTrades.length;

    // Filter for today's completed trades
    const todayCompleted = this.completedTrades.filter(t => {
      const completionTime = t.stopLossHitTime || t.target3HitTime || t.target2HitTime || t.target1HitTime || t.mitigationTime;
      if (!completionTime) return false;
      return new Date(completionTime) >= todayStart;
    });

    // Wins = completed trades that hit at least T1 (not SL)
    this.stats.todayWins = todayCompleted.filter(t =>
      !t.stopLossHitTime && t.status !== 'STOPPED' &&
      (t.target1AlertSent || t.target2AlertSent || t.target3AlertSent || t.tradeOutcome === 'WIN')
    ).length;

    // Losses = completed trades that hit SL
    this.stats.todayLosses = todayCompleted.filter(t =>
      t.stopLossHitTime || t.status === 'STOPPED' || t.tradeOutcome === 'LOSS'
    ).length;

    this.stats.todayPnL = todayCompleted.reduce((sum, t) => sum + (t.pointsCaptured || 0), 0);

    const totalCompleted = this.stats.todayWins + this.stats.todayLosses;
    this.stats.winRate = totalCompleted > 0 ? (this.stats.todayWins / totalCompleted) * 100 : 0;
  }

  applyFilters(): void {
    let filtered: InternalOrderBlock[] = [];

    // Apply status filter first
    if (this.selectedFilter === 'ACTIVE') {
      // Active = FRESH IOBs waiting for entry
      filtered = [...this.liveTrades];
    } else if (this.selectedFilter === 'COMPLETED') {
      // Completed = trade ended (SL or targets hit)
      filtered = [...this.completedTrades];
    } else {
      // ALL = show mitigated (in progress) + completed trades
      // Note: We show allIOBs which contains mitigated + completed
      filtered = [...this.allIOBs];
    }

    // Apply instrument filter
    if (this.selectedInstrument !== 'ALL') {
      filtered = filtered.filter(iob =>
        iob.instrumentName?.includes(this.selectedInstrument)
      );
    }

    // Filter by date - only show today's trades unless showOlderTrades is enabled
    if (!this.showOlderTrades) {
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);

      filtered = filtered.filter(iob => {
        const tradeTime = iob.entryTriggeredTime || iob.mitigationTime || iob.detectionTimestamp;
        if (!tradeTime) return false;
        return new Date(tradeTime) >= todayStart;
      });
    }

    this.buildTradeTimelines(filtered);
  }

  // Toggle show/hide older trades
  onShowOlderTradesChange(): void {
    this.applyFilters();
  }

  /**
   * Get count of older trades (trades from before today)
   */
  getOlderTradesCount(): number {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    return this.allIOBs.filter(iob => {
      const tradeTime = iob.entryTriggeredTime || iob.mitigationTime || iob.detectionTimestamp;
      if (!tradeTime) return false;
      return new Date(tradeTime) < todayStart;
    }).length;
  }

  selectInstrument(instrument: 'NIFTY' | 'ALL'): void {
    this.selectedInstrument = instrument;
    this.applyFilters();
  }

  selectFilter(filter: 'ACTIVE' | 'COMPLETED' | 'ALL'): void {
    this.selectedFilter = filter;
    this.applyFilters();
  }

  refreshData(): void {
    this.loadAllData();
  }

  startAutoRefresh(): void {
    if (this.autoRefresh) {
      this.refreshSub = interval(this.refreshInterval).subscribe(() => {
        this.loadAllData();
      });
    }
  }

  stopAutoRefresh(): void {
    if (this.refreshSub) {
      this.refreshSub.unsubscribe();
    }
  }

  toggleAutoRefresh(): void {
    this.autoRefresh = !this.autoRefresh;
    if (this.autoRefresh) {
      this.startAutoRefresh();
    } else {
      this.stopAutoRefresh();
    }
  }

  // Helper methods

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null) return '--';
    return value.toFixed(decimals);
  }

  formatDateTime(value: string | undefined | null): string {
    if (!value) return '--';
    try {
      const date = new Date(value);
      return date.toLocaleString('en-IN', {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
    } catch {
      return value;
    }
  }

  formatTime(value: string | undefined | null): string {
    if (!value) return '--';
    try {
      const date = new Date(value);
      return date.toLocaleString('en-IN', {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
    } catch {
      return value;
    }
  }

  getOutcomeClass(outcome: string | undefined): string {
    switch (outcome) {
      case 'WIN': return 'outcome-win';
      case 'LOSS': return 'outcome-loss';
      case 'ACTIVE': return 'outcome-active';
      default: return '';
    }
  }

  getProgressPercent(iob: InternalOrderBlock): number {
    if (iob.status === 'FRESH') return 0; // Not yet entered
    if (!iob.actualEntryPrice && !iob.entryTriggeredTime && iob.status !== 'MITIGATED') return 0;
    if (iob.target3AlertSent || iob.tradeOutcome === 'WIN') return 100;
    if (iob.status === 'STOPPED' || iob.tradeOutcome === 'LOSS') return 100;
    if (iob.target2AlertSent) return 75;
    if (iob.target1AlertSent) return 50;
    if (iob.entryTriggeredTime || iob.status === 'MITIGATED') return 25;
    return 0;
  }

  getProgressLabel(iob: InternalOrderBlock): string {
    if (iob.status === 'FRESH') return 'Pending Entry';
    if (iob.status === 'STOPPED' || iob.tradeOutcome === 'LOSS') return 'SL Hit';
    if (iob.target3AlertSent || iob.tradeOutcome === 'WIN') return 'T3 ✓';
    if (iob.target2AlertSent) return 'T2 ✓';
    if (iob.target1AlertSent) return 'T1 ✓';
    if (iob.entryTriggeredTime || iob.status === 'MITIGATED') return 'Active';
    return 'Pending';
  }

  getCurrentPnL(iob: InternalOrderBlock): string {
    if (!iob.actualEntryPrice || !iob.currentPrice) return '--';
    const isBullish = iob.obType === 'BULLISH_IOB';
    const pnl = isBullish
      ? iob.currentPrice - iob.actualEntryPrice
      : iob.actualEntryPrice - iob.currentPrice;
    const sign = pnl >= 0 ? '+' : '';
    return `${sign}${pnl.toFixed(2)} pts`;
  }

  getPnLClass(iob: InternalOrderBlock): string {
    if (iob.pointsCaptured === undefined || iob.pointsCaptured === null) return '';
    return iob.pointsCaptured >= 0 ? 'pnl-positive' : 'pnl-negative';
  }

  getDuration(iob: InternalOrderBlock): string {
    const entryTimeStr = iob.entryTriggeredTime || iob.mitigationTime;
    if (!entryTimeStr) return '--';

    const entryTime = new Date(entryTimeStr);
    const endTime = iob.stopLossHitTime ? new Date(iob.stopLossHitTime) :
                    iob.target3HitTime ? new Date(iob.target3HitTime) :
                    new Date();

    const durationMs = endTime.getTime() - entryTime.getTime();
    const minutes = Math.floor(durationMs / 60000);
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;

    if (hours > 0) {
      return `${hours}h ${remainingMinutes}m`;
    }
    return `${minutes}m`;
  }

  getTypeClass(obType: string | undefined): string {
    return obType === 'BULLISH_IOB' ? 'type-bullish' : 'type-bearish';
  }

  getDirectionIcon(direction: string | undefined): string {
    return direction === 'LONG' ? 'trending_up' : 'trending_down';
  }

  getDirectionClass(direction: string | undefined): string {
    return direction === 'LONG' ? 'direction-long' : 'direction-short';
  }
}

