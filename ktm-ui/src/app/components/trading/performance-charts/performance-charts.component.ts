import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { HighchartsChartModule } from 'highcharts-angular';
import * as Highcharts from 'highcharts';
import { DataService } from '../../../services/data.service';
import { interval, Subscription } from 'rxjs';

interface SourceStats {
  signalSource: string;
  displayName: string;
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  pnl: number;
  winRate: number;
  profitFactor: number;
}

interface DailyTrend {
  date: string;
  totalTrades: number;
  winningTrades: number;
  pnl: number;
  winRate: number;
}

@Component({
  selector: 'app-performance-charts',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatChipsModule,
    HighchartsChartModule
  ],
  templateUrl: './performance-charts.component.html',
  styleUrls: ['./performance-charts.component.css']
})
export class PerformanceChartsComponent implements OnInit, OnDestroy {
  Highcharts: typeof Highcharts = Highcharts;

  // Data
  performanceData: any = null;
  isLoading = false;
  error: string | null = null;

  // Period selection
  selectedPeriod: 'daily' | 'weekly' | 'monthly' = 'daily';

  // Chart configurations
  winRateChartOptions: Highcharts.Options = {};
  pnlChartOptions: Highcharts.Options = {};
  trendChartOptions: Highcharts.Options = {};
  sourceBreakdownOptions: Highcharts.Options = {};

  // Auto-refresh
  private refreshSubscription: Subscription | null = null;
  private readonly REFRESH_INTERVAL = 60000; // 1 minute

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.loadPerformanceData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
  }

  startAutoRefresh(): void {
    this.refreshSubscription = interval(this.REFRESH_INTERVAL).subscribe(() => {
      this.loadPerformanceData();
    });
  }

  stopAutoRefresh(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
      this.refreshSubscription = null;
    }
  }

  loadPerformanceData(): void {
    this.isLoading = true;
    this.error = null;

    this.dataService.getPerformanceChartData().subscribe({
      next: (data) => {
        this.performanceData = data;
        this.updateCharts();
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading performance data:', err);
        this.error = 'Failed to load performance data';
        this.isLoading = false;
      }
    });
  }

  onPeriodChange(period: 'daily' | 'weekly' | 'monthly'): void {
    this.selectedPeriod = period;
    this.updateCharts();
  }

  updateCharts(): void {
    if (!this.performanceData) return;

    const periodData = this.performanceData[this.selectedPeriod];
    if (!periodData) return;

    this.updateWinRateChart(periodData.bySource || []);
    this.updatePnLChart(periodData.bySource || []);
    this.updateTrendChart(this.performanceData.dailyTrend || []);
    this.updateSourceBreakdown(this.performanceData.sourceBreakdown || []);
  }

  updateWinRateChart(sourceStats: SourceStats[]): void {
    const categories = sourceStats.map(s => s.displayName);
    const winRates = sourceStats.map(s => Math.round(s.winRate * 10) / 10);
    const colors = this.getSourceColors(sourceStats);

    this.winRateChartOptions = {
      chart: {
        type: 'bar',
        backgroundColor: 'transparent',
        height: 200
      },
      title: {
        text: 'Win Rate by Signal Source',
        style: { color: '#fff', fontSize: '14px' }
      },
      xAxis: {
        categories: categories,
        labels: { style: { color: '#aaa' } }
      },
      yAxis: {
        min: 0,
        max: 100,
        title: { text: 'Win Rate (%)', style: { color: '#aaa' } },
        labels: { style: { color: '#aaa' } },
        gridLineColor: 'rgba(255,255,255,0.1)'
      },
      legend: { enabled: false },
      credits: { enabled: false },
      plotOptions: {
        bar: {
          dataLabels: {
            enabled: true,
            format: '{y}%',
            style: { color: '#fff', textOutline: 'none' }
          }
        }
      },
      series: [{
        type: 'bar',
        name: 'Win Rate',
        data: winRates.map((v, i) => ({ y: v, color: colors[i] })),
        colorByPoint: true
      }]
    };
  }

  updatePnLChart(sourceStats: SourceStats[]): void {
    const categories = sourceStats.map(s => s.displayName);
    const pnlValues = sourceStats.map(s => Math.round(s.pnl));

    this.pnlChartOptions = {
      chart: {
        type: 'column',
        backgroundColor: 'transparent',
        height: 200
      },
      title: {
        text: 'P&L by Signal Source',
        style: { color: '#fff', fontSize: '14px' }
      },
      xAxis: {
        categories: categories,
        labels: { style: { color: '#aaa' } }
      },
      yAxis: {
        title: { text: 'P&L (₹)', style: { color: '#aaa' } },
        labels: { style: { color: '#aaa' } },
        gridLineColor: 'rgba(255,255,255,0.1)',
        plotLines: [{
          value: 0,
          width: 1,
          color: 'rgba(255,255,255,0.3)'
        }]
      },
      legend: { enabled: false },
      credits: { enabled: false },
      plotOptions: {
        column: {
          dataLabels: {
            enabled: true,
            format: '₹{y}',
            style: { color: '#fff', textOutline: 'none', fontSize: '10px' }
          }
        }
      },
      series: [{
        type: 'column',
        name: 'P&L',
        data: pnlValues.map(v => ({
          y: v,
          color: v >= 0 ? '#4CAF50' : '#f44336'
        }))
      }]
    };
  }

  updateTrendChart(dailyTrend: DailyTrend[]): void {
    const categories = dailyTrend.map(d => this.formatDate(d.date));
    const pnlValues = dailyTrend.map(d => Math.round(d.pnl));
    const winRates = dailyTrend.map(d => Math.round(d.winRate * 10) / 10);

    this.trendChartOptions = {
      chart: {
        type: 'line',
        backgroundColor: 'transparent',
        height: 200
      },
      title: {
        text: '7-Day Performance Trend',
        style: { color: '#fff', fontSize: '14px' }
      },
      xAxis: {
        categories: categories,
        labels: { style: { color: '#aaa' } }
      },
      yAxis: [{
        title: { text: 'P&L (₹)', style: { color: '#4CAF50' } },
        labels: { style: { color: '#aaa' } },
        gridLineColor: 'rgba(255,255,255,0.1)'
      }, {
        title: { text: 'Win Rate (%)', style: { color: '#2196F3' } },
        labels: { style: { color: '#aaa' } },
        opposite: true,
        min: 0,
        max: 100
      }],
      legend: {
        enabled: true,
        itemStyle: { color: '#aaa' }
      },
      credits: { enabled: false },
      series: [{
        type: 'area',
        name: 'P&L',
        data: pnlValues,
        color: '#4CAF50',
        fillOpacity: 0.2,
        yAxis: 0
      }, {
        type: 'line',
        name: 'Win Rate',
        data: winRates,
        color: '#2196F3',
        yAxis: 1
      }]
    };
  }

  updateSourceBreakdown(sourceBreakdown: SourceStats[]): void {
    const data = sourceBreakdown
      .filter(s => s.totalTrades > 0)
      .map((s, i) => ({
        name: s.displayName,
        y: s.totalTrades,
        color: this.getSourceColor(s.signalSource)
      }));

    this.sourceBreakdownOptions = {
      chart: {
        type: 'pie',
        backgroundColor: 'transparent',
        height: 200
      },
      title: {
        text: 'Trade Distribution',
        style: { color: '#fff', fontSize: '14px' }
      },
      credits: { enabled: false },
      plotOptions: {
        pie: {
          innerSize: '50%',
          dataLabels: {
            enabled: true,
            format: '<b>{point.name}</b>: {point.y}',
            style: { color: '#aaa', textOutline: 'none', fontSize: '10px' }
          }
        }
      },
      series: [{
        type: 'pie',
        name: 'Trades',
        data: data.length > 0 ? data : [{ name: 'No trades', y: 1, color: '#555' }]
      }]
    };
  }

  getSourceColor(source: string): string {
    switch (source) {
      case 'TRADE_SETUP': return '#FF9800';
      case 'EMA_CROSSOVER': return '#2196F3';
      case 'LIQUIDITY_SWEEP': return '#00BCD4';
      case 'IOB_SIGNAL': return '#4CAF50';  // Green for IOB signals
      case 'MANUAL': return '#607D8B';
      default: return '#9E9E9E';
    }
  }

  getSourceColors(stats: SourceStats[]): string[] {
    return stats.map(s => this.getSourceColor(s.signalSource));
  }

  formatDate(dateStr: string): string {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
  }

  formatCurrency(value: number): string {
    if (value === undefined || value === null) return '₹0';
    const prefix = value >= 0 ? '+' : '';
    return prefix + '₹' + Math.abs(value).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  }

  formatPercent(value: number): string {
    if (value === undefined || value === null) return '0%';
    return value.toFixed(1) + '%';
  }

  getCurrentPeriodStats(): SourceStats[] {
    if (!this.performanceData || !this.performanceData[this.selectedPeriod]) return [];
    return this.performanceData[this.selectedPeriod].bySource || [];
  }

  getTotalStats(): { totalTrades: number; winRate: number; pnl: number } {
    if (!this.performanceData || !this.performanceData[this.selectedPeriod]) {
      return { totalTrades: 0, winRate: 0, pnl: 0 };
    }
    const data = this.performanceData[this.selectedPeriod];
    return {
      totalTrades: data.totalTrades || 0,
      winRate: data.winRate || 0,
      pnl: data.totalPnl || 0
    };
  }
}

