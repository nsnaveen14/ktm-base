import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { interval, Subscription } from 'rxjs';
import { DataService } from '../../../services/data.service';

interface LiquiditySweepDashboard {
  hasData: boolean;
  message?: string;
  marketStructure?: {
    bslLevel1?: number;
    bslLevel2?: number;
    bslLevel3?: number;
    sslLevel1?: number;
    sslLevel2?: number;
    sslLevel3?: number;
    swingHigh1?: number;
    swingHigh2?: number;
    swingLow1?: number;
    swingLow2?: number;
  };
  quantEngine?: {
    volumeZScore?: number;
    kaufmanEfficiency?: number;
    whaleType?: string;
    hasWhaleActivity?: boolean;
    isAbsorption?: boolean;
    isPropulsion?: boolean;
    averageVolume?: number;
    currentVolume?: number;
    whaleThreshold?: number;
  };
  trendMomentum?: {
    ema200?: number;
    isAboveEma200?: boolean;
    trendDirection?: string;
    rsiValue?: number;
    isRsiOversold?: boolean;
    isRsiOverbought?: boolean;
  };
  sweepDetection?: {
    bslSwept?: boolean;
    sslSwept?: boolean;
    sweepType?: string;
    sweptLevel?: number;
    priceClosedBack?: boolean;
    hasInstitutionalConfirmation?: boolean;
    isTrendAligned?: boolean;
    isMomentumAligned?: boolean;
  };
  tradeSignal?: {
    signalType?: string;
    signalStrength?: string;
    signalConfidence?: number;
    isValidSetup?: boolean;
    entryPrice?: number;
    stopLossPrice?: number;
    takeProfit1?: number;
    takeProfit2?: number;
    takeProfit3?: number;
    riskRewardRatio?: number;
    riskPoints?: number;
    atrValue?: number;
    suggestedOptionType?: string;
    optionStrategy?: string;
  };
  priceData?: {
    spotPrice?: number;
    open?: number;
    high?: number;
    low?: number;
    close?: number;
    volume?: number;
  };
  todayStats?: {
    totalAnalyses?: number;
    validSetups?: number;
    whaleEvents?: number;
    bslSweeps?: number;
    sslSweeps?: number;
  };
  analysisTimestamp?: string;
  timeframe?: string;
  configuration?: any;
}

@Component({
  selector: 'app-liquidity-sweep',
  standalone: true,
  imports: [
    CommonModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatExpansionModule,
    MatTooltipModule,
    MatDividerModule
  ],
  templateUrl: './liquidity-sweep.component.html',
  styleUrl: './liquidity-sweep.component.css'
})
export class LiquiditySweepComponent implements OnInit, OnDestroy {
  dashboard: LiquiditySweepDashboard | null = null;
  isLoading = false;
  lastRefresh: Date | null = null;
  error: string | null = null;

  private refreshSubscription: Subscription | null = null;
  private appJobConfigNum = 1; // Default to NIFTY

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.loadDashboard();
    // Auto-refresh every 5 minutes
    this.refreshSubscription = interval(300000).subscribe(() => {
      this.loadDashboard();
    });
  }

  ngOnDestroy(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
    }
  }

  loadDashboard(): void {
    this.isLoading = true;
    this.error = null;

    this.dataService.getLiquiditySweepDashboard(this.appJobConfigNum).subscribe({
      next: (data) => {
        this.dashboard = data;
        this.lastRefresh = new Date();
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading liquidity sweep dashboard:', err);
        this.error = 'Failed to load liquidity sweep data';
        this.isLoading = false;
      }
    });
  }

  runAnalysis(): void {
    this.isLoading = true;
    this.error = null;

    this.dataService.runLiquiditySweepAnalysis(this.appJobConfigNum).subscribe({
      next: () => {
        // Reload dashboard after analysis
        this.loadDashboard();
      },
      error: (err) => {
        console.error('Error running liquidity sweep analysis:', err);
        this.error = 'Failed to run analysis';
        this.isLoading = false;
      }
    });
  }

  refresh(): void {
    this.loadDashboard();
  }

  // Helper methods
  formatNumber(value: number | undefined): string {
    if (value === undefined || value === null) return '-';
    return value.toFixed(2);
  }

  formatPercent(value: number | undefined): string {
    if (value === undefined || value === null) return '-';
    return value.toFixed(1) + '%';
  }

  formatTime(timestamp: string | undefined): string {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' });
  }

  getSignalClass(signal: string | undefined): string {
    if (!signal) return '';
    switch (signal.toUpperCase()) {
      case 'BUY': return 'bullish';
      case 'SELL': return 'bearish';
      case 'STRONG': return 'strong';
      case 'MODERATE': return 'moderate';
      case 'WEAK': return 'weak';
      default: return 'neutral';
    }
  }

  getWhaleIcon(whaleType: string | undefined): string {
    if (!whaleType) return 'waves';
    switch (whaleType.toUpperCase()) {
      case 'ABSORPTION': return 'ac_unit'; // Iceberg/absorption
      case 'PROPULSION': return 'rocket_launch'; // Drive/propulsion
      case 'ACCUMULATION': return 'savings';
      default: return 'waves';
    }
  }

  getTrendIcon(trend: string | undefined): string {
    if (!trend) return 'swap_vert';
    switch (trend.toUpperCase()) {
      case 'BULLISH': return 'trending_up';
      case 'BEARISH': return 'trending_down';
      default: return 'swap_vert';
    }
  }

  getSweepIcon(sweepType: string | undefined): string {
    if (!sweepType || sweepType === 'NONE') return 'remove';
    return sweepType === 'BSL_SWEEP' ? 'arrow_upward' : 'arrow_downward';
  }
}

