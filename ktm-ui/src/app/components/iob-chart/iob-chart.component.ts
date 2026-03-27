import { Component, OnInit, OnDestroy, ElementRef, ViewChild, AfterViewInit, Input, ChangeDetectorRef, NgZone } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { Subscription } from 'rxjs';
import { PerformanceService } from '../../services/performance.service';
// import { WebSocketService } from '../../services/websocket.service';
import { WebsocketService } from '../../services/web-socket.service';
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  LineStyle,
  CrosshairMode
} from 'lightweight-charts';
import { ChartCandle, IOBZone, CompleteChartData } from '../../models/performance.model';

@Component({
  selector: 'app-iob-chart',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatSelectModule,
    MatFormFieldModule,
    MatProgressSpinnerModule,
    MatChipsModule,
    MatTooltipModule,
    MatCheckboxModule
  ],
  templateUrl: './iob-chart.component.html',
  styleUrls: ['./iob-chart.component.css']
})
export class IobChartComponent implements OnInit, OnDestroy, AfterViewInit {
  @ViewChild('chartContainer') chartContainer!: ElementRef;
  @Input() instrumentToken: number = 256265; // Default NIFTY

  private chart: IChartApi | null = null;
  private candleSeries: ISeriesApi<'Candlestick'> | null = null;
  private priceLine: any = null;
  private zoneRectangles: Map<number, any> = new Map();
  private levelLines: Map<string, any> = new Map();

  isLoading = false;
  selectedInterval = '5minute';
  selectedInstrument = 'NIFTY';

  currentPrice: number | null = null;
  chartData: CompleteChartData | null = null;

  // Zone selection for filtering displayed zones
  selectedZoneIds: Set<number> = new Set();
  selectAllZones = true; // By default, select all zones

  private subscriptions: Subscription[] = [];
  private priceUpdateInterval: any = null;
  private resizeObserver: ResizeObserver | null = null;
  private intersectionObserver: IntersectionObserver | null = null;
  private chartInitialized = false;
  private dataLoaded = false;

  readonly intervals = [
    { value: '5minute', label: '5 min' },
    { value: '15minute', label: '15 min' },
    { value: '60minute', label: '1 hour' },
    { value: 'day', label: 'Daily' }
  ];

  constructor(
    private performanceService: PerformanceService,
    private wsService: WebsocketService,
    private cdr: ChangeDetectorRef,
    private ngZone: NgZone
  ) {}

  ngOnInit(): void {
    // Subscribe to real-time price updates
    this.subscriptions.push(
      this.wsService.getPriceUpdates().subscribe(tick => {
        this.handlePriceUpdate(tick);
      })
    );

    // Subscribe to IOB zone touch alerts
    this.subscriptions.push(
      this.wsService.getZoneTouchAlerts().subscribe(alert => {
        this.handleZoneTouchAlert(alert);
      })
    );
  }

  ngAfterViewInit(): void {
    // Use IntersectionObserver to detect when the chart becomes visible (important for tabs)
    this.ngZone.runOutsideAngular(() => {
      this.intersectionObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
          if (entry.isIntersecting && entry.intersectionRatio > 0) {
            this.ngZone.run(() => {
              this.handleVisibilityChange();
            });
          }
        });
      }, { threshold: [0, 0.1, 0.5, 1.0] });

      // Use ResizeObserver to detect when container has proper dimensions
      this.resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          const { width, height } = entry.contentRect;
          if (width > 0 && height > 0) {
            this.ngZone.run(() => {
              this.handleVisibilityChange();
              // Handle resize after initialization
              if (this.chart && this.chartInitialized) {
                this.chart.applyOptions({ width: width });
              }
            });
          }
        }
      });

      if (this.chartContainer?.nativeElement) {
        this.resizeObserver.observe(this.chartContainer.nativeElement);
        this.intersectionObserver.observe(this.chartContainer.nativeElement);
      }
    });

    // Fallback: try to initialize after a delay if observers don't trigger
    setTimeout(() => {
      this.handleVisibilityChange();
    }, 500);
  }

  private handleVisibilityChange(): void {
    if (this.chartContainer?.nativeElement) {
      const container = this.chartContainer.nativeElement;
      const rect = container.getBoundingClientRect();

      // Only initialize if container has proper dimensions and is visible
      if (rect.width > 0 && rect.height > 0) {
        if (!this.chartInitialized) {
          this.initChart();
          this.chartInitialized = true;
        }

        if (!this.dataLoaded && this.chartInitialized) {
          this.loadChartData();
          this.dataLoaded = true;
        }

        // Force chart resize when becoming visible
        if (this.chart && this.chartInitialized) {
          this.chart.applyOptions({ width: container.clientWidth });
          this.chart.timeScale().fitContent();
          this.cdr.detectChanges();
        }
      }
    }
  }

  ngOnDestroy(): void {
    this.subscriptions.forEach(sub => sub.unsubscribe());
    if (this.priceUpdateInterval) {
      clearInterval(this.priceUpdateInterval);
    }
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    }
    if (this.intersectionObserver) {
      this.intersectionObserver.disconnect();
      this.intersectionObserver = null;
    }
    if (this.chart) {
      this.chart.remove();
    }
  }

  // ==================== Chart Initialization ====================

  private initChart(): void {
    if (!this.chartContainer?.nativeElement) return;

    const container = this.chartContainer.nativeElement;

    this.chart = createChart(container, {
      width: container.clientWidth,
      height: 500,
      layout: {
        background: { color: '#1e222d' },
        textColor: '#d1d4dc'
      },
      grid: {
        vertLines: { color: '#2B2B43' },
        horzLines: { color: '#2B2B43' }
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#758696',
          width: 1,
          style: LineStyle.Dashed
        },
        horzLine: {
          color: '#758696',
          width: 1,
          style: LineStyle.Dashed
        }
      },
      rightPriceScale: {
        borderColor: '#2B2B43',
        scaleMargins: {
          top: 0.1,
          bottom: 0.1
        }
      },
      timeScale: {
        borderColor: '#2B2B43',
        timeVisible: true,
        secondsVisible: false
      }
    });

    this.candleSeries = this.chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderUpColor: '#26a69a',
      borderDownColor: '#ef5350',
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350'
    });

    // Force an immediate resize to ensure chart renders at correct size
    requestAnimationFrame(() => {
      if (this.chart && container.clientWidth > 0) {
        this.chart.applyOptions({ width: container.clientWidth });
        this.chart.timeScale().fitContent();
      }
    });
  }

  // ==================== Data Loading ====================

  loadChartData(): void {
    this.isLoading = true;
    const token = this.selectedInstrument === 'NIFTY' ? 256265 : 265;

    this.performanceService.getCompleteChartData(token, this.selectedInterval).subscribe({
      next: (data) => {
        this.chartData = data;
        this.updateChart(data);
        this.currentPrice = data.currentPrice;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading chart data:', err);
        this.isLoading = false;
      }
    });
  }

  private updateChart(data: CompleteChartData): void {
    if (!this.candleSeries || !data.candles) return;

    // Convert candles to Lightweight Charts format
    const candleData: CandlestickData[] = data.candles.map(c => ({
      time: c.time as any,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close
    }));

    this.candleSeries.setData(candleData);

    // Draw IOB zones - only selected ones
    this.clearZones();
    if (data.iobZones) {
      // If selectAllZones is true or no specific zones selected, select all by default
      if (this.selectAllZones || this.selectedZoneIds.size === 0) {
        this.selectedZoneIds = new Set(data.iobZones.map(z => z.id));
        this.selectAllZones = true;
      }

      // Only draw selected zones
      const zonesToDraw = data.iobZones.filter(zone => this.selectedZoneIds.has(zone.id));
      zonesToDraw.forEach(zone => this.drawIOBZone(zone));
    }

    // Draw swing points
    if (data.swingHighs && data.swingLows) {
      this.drawSwingPoints(data.swingHighs, data.swingLows);
    }

    // Set current price line
    if (data.currentPrice) {
      this.updatePriceLine(data.currentPrice);
    }

    // Fit content
    this.chart?.timeScale().fitContent();
  }

  // ==================== Zone Drawing ====================

  private drawIOBZone(zone: IOBZone): void {
    if (!this.candleSeries) return;

    const isBullish = zone.type === 'BULLISH_IOB';
    const color = isBullish ? 'rgba(76, 175, 80, 0.2)' : 'rgba(244, 67, 54, 0.2)';
    const borderColor = isBullish ? '#4CAF50' : '#F44336';

    // Draw zone as horizontal band using price lines
    const zoneHighLine = this.candleSeries.createPriceLine({
      price: zone.zoneHigh,
      color: borderColor,
      lineWidth: 1,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
      title: `${zone.type.includes('BULLISH') ? '🟢' : '🔴'} Zone High`
    });

    const zoneLowLine = this.candleSeries.createPriceLine({
      price: zone.zoneLow,
      color: borderColor,
      lineWidth: 1,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
      title: 'Zone Low'
    });

    // Draw midpoint
    const midLine = this.candleSeries.createPriceLine({
      price: zone.zoneMidpoint,
      color: borderColor,
      lineWidth: 1,
      lineStyle: LineStyle.Dotted,
      axisLabelVisible: false,
      title: 'Entry'
    });

    // Store references for cleanup
    this.levelLines.set(`zone_high_${zone.id}`, zoneHighLine);
    this.levelLines.set(`zone_low_${zone.id}`, zoneLowLine);
    this.levelLines.set(`zone_mid_${zone.id}`, midLine);

    // Draw trade levels if available
    if (zone.stopLoss) {
      const slLine = this.candleSeries.createPriceLine({
        price: zone.stopLoss,
        color: '#F44336',
        lineWidth: 2,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'SL'
      });
      this.levelLines.set(`sl_${zone.id}`, slLine);
    }

    if (zone.target1) {
      const t1Line = this.candleSeries.createPriceLine({
        price: zone.target1,
        color: '#4CAF50',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'T1'
      });
      this.levelLines.set(`t1_${zone.id}`, t1Line);
    }

    if (zone.target2) {
      const t2Line = this.candleSeries.createPriceLine({
        price: zone.target2,
        color: '#8BC34A',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'T2'
      });
      this.levelLines.set(`t2_${zone.id}`, t2Line);
    }

    if (zone.target3) {
      const t3Line = this.candleSeries.createPriceLine({
        price: zone.target3,
        color: '#CDDC39',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'T3'
      });
      this.levelLines.set(`t3_${zone.id}`, t3Line);
    }

    // Draw BOS level
    if (zone.bosLevel) {
      const bosLine = this.candleSeries.createPriceLine({
        price: zone.bosLevel,
        color: '#FF9800',
        lineWidth: 1,
        lineStyle: LineStyle.Dotted,
        axisLabelVisible: true,
        title: 'BOS'
      });
      this.levelLines.set(`bos_${zone.id}`, bosLine);
    }
  }

  private drawSwingPoints(highs: any[], lows: any[]): void {
    if (!this.candleSeries) return;

    // Add markers for swing points
    const markers: any[] = [];

    highs.forEach(sh => {
      markers.push({
        time: sh.time,
        position: 'aboveBar',
        color: '#F44336',
        shape: 'arrowDown',
        text: 'SH'
      });
    });

    lows.forEach(sl => {
      markers.push({
        time: sl.time,
        position: 'belowBar',
        color: '#4CAF50',
        shape: 'arrowUp',
        text: 'SL'
      });
    });

    // Sort markers by time
    markers.sort((a, b) => a.time - b.time);
    this.candleSeries.setMarkers(markers);
  }

  private clearZones(): void {
    // Remove all price lines
    this.levelLines.forEach((line, key) => {
      if (this.candleSeries) {
        this.candleSeries.removePriceLine(line);
      }
    });
    this.levelLines.clear();
    this.zoneRectangles.clear();
  }

  private updatePriceLine(price: number): void {
    if (!this.candleSeries) return;

    if (this.priceLine) {
      this.candleSeries.removePriceLine(this.priceLine);
    }

    this.priceLine = this.candleSeries.createPriceLine({
      price: price,
      color: '#2196F3',
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
      title: 'LTP'
    });
  }

  // ==================== Real-Time Updates ====================

  private handlePriceUpdate(tick: any): void {
    if (!tick || !tick.prices) return;

    const priceData = tick.prices.NIFTY;

    if (priceData) {
      this.currentPrice = priceData.price;
      this.updatePriceLine(priceData.price);

      // Update last candle
      if (this.candleSeries && this.chartData?.candles?.length) {
        const lastCandle = this.chartData.candles[this.chartData.candles.length - 1];
        this.candleSeries.update({
          time: lastCandle.time as any,
          open: lastCandle.open,
          high: Math.max(lastCandle.high, priceData.price),
          low: Math.min(lastCandle.low, priceData.price),
          close: priceData.price
        });
      }
    }
  }

  private handleZoneTouchAlert(alert: any): void {
    console.log('Zone touch alert:', alert);
    // Highlight the zone that was touched
    // Could add visual effect or notification
  }

  // ==================== UI Actions ====================

  changeInstrument(instrument: string): void {
    this.selectedInstrument = instrument;
    this.instrumentToken = 256265;
    // Reset zone selection when changing instrument
    this.selectedZoneIds.clear();
    this.selectAllZones = true;
    this.loadChartData();
  }

  changeInterval(interval: string): void {
    this.selectedInterval = interval;
    // Reset zone selection when changing interval
    this.selectedZoneIds.clear();
    this.selectAllZones = true;
    this.loadChartData();
  }

  refreshChart(): void {
    // Keep current selection when refreshing
    this.loadChartData();
  }

  toggleZones(): void {
    // Toggle visibility of IOB zones
    this.levelLines.forEach((line, key) => {
      // Lightweight Charts doesn't have direct visibility toggle
      // Would need to remove/add lines
    });
  }

  zoomToFit(): void {
    this.chart?.timeScale().fitContent();
  }

  // ==================== Zone Selection ====================

  /**
   * Toggle selection of a specific zone
   */
  toggleZoneSelection(zoneId: number): void {
    if (this.selectedZoneIds.has(zoneId)) {
      this.selectedZoneIds.delete(zoneId);
    } else {
      this.selectedZoneIds.add(zoneId);
    }

    // Update selectAllZones flag
    this.selectAllZones = this.chartData?.iobZones
      ? this.selectedZoneIds.size === this.chartData.iobZones.length
      : false;

    this.redrawSelectedZones();
  }

  /**
   * Check if a zone is selected
   */
  isZoneSelected(zoneId: number): boolean {
    return this.selectedZoneIds.has(zoneId);
  }

  /**
   * Toggle select all zones
   */
  toggleSelectAllZones(): void {
    if (this.selectAllZones) {
      // Deselect all
      this.selectedZoneIds.clear();
      this.selectAllZones = false;
    } else {
      // Select all
      if (this.chartData?.iobZones) {
        this.selectedZoneIds = new Set(this.chartData.iobZones.map(z => z.id));
      }
      this.selectAllZones = true;
    }

    this.redrawSelectedZones();
  }

  /**
   * Select only a single zone (exclusive selection)
   */
  selectOnlyZone(zoneId: number): void {
    this.selectedZoneIds.clear();
    this.selectedZoneIds.add(zoneId);
    this.selectAllZones = false;
    this.redrawSelectedZones();
  }

  /**
   * Redraw only the selected zones
   */
  private redrawSelectedZones(): void {
    if (!this.chartData?.iobZones || !this.candleSeries) return;

    // Clear all existing zones
    this.clearZones();

    // Draw only selected zones
    const zonesToDraw = this.chartData.iobZones.filter(zone => this.selectedZoneIds.has(zone.id));
    zonesToDraw.forEach(zone => this.drawIOBZone(zone));

    // Trigger change detection
    this.cdr.detectChanges();
  }

  /**
   * Get the count of selected zones
   */
  getSelectedZoneCount(): number {
    return this.selectedZoneIds.size;
  }
}
