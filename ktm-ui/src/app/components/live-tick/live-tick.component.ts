import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatButtonModule } from '@angular/material/button';
import { Subscription } from 'rxjs';
import { TickerService } from '../../services/ticker.service';

@Component({
  selector: 'app-live-tick',
  standalone: true,
  imports: [CommonModule, MatIconModule, MatChipsModule, MatTooltipModule, MatButtonModule],
  templateUrl: './live-tick.component.html',
  styleUrls: ['./live-tick.component.css']
})
export class LiveTickComponent implements OnInit, OnDestroy {
  liveTickData: any = null;
  isTickerConnected = false;
  isLoading = false;
  jobStatus: any = null;
  private refreshSub: Subscription | null = null;
  private connSub: Subscription | null = null;
  private jobSub: Subscription | null = null;

  constructor(private tickerService: TickerService) {}

  ngOnInit(): void {
    this.connSub = this.tickerService.isTickerConnected$.subscribe(v => this.isTickerConnected = v);
    this.refreshSub = this.tickerService.liveTick$.subscribe(v => this.liveTickData = v);
    this.jobSub = this.tickerService.jobStatus$.subscribe(s => this.jobStatus = s);
  }

  ngOnDestroy(): void {
    if (this.refreshSub) this.refreshSub.unsubscribe();
    if (this.connSub) this.connSub.unsubscribe();
    if (this.jobSub) this.jobSub.unsubscribe();
  }

  refresh(): void {
    // allow manual refresh via the service by stopping and restarting
    this.tickerService.start();
  }

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null) return '--';
    return value.toFixed(decimals);
  }
}
