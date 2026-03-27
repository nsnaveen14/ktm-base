import { Injectable, OnDestroy } from '@angular/core';
import { BehaviorSubject, interval, Subscription } from 'rxjs';
import { DataService } from './data.service';

@Injectable({ providedIn: 'root' })
export class TickerService implements OnDestroy {
  private liveTickSubject = new BehaviorSubject<any>(null);
  private isTickerConnectedSubject = new BehaviorSubject<boolean>(false);
  private jobStatusSubject = new BehaviorSubject<any>(null);

  liveTick$ = this.liveTickSubject.asObservable();
  isTickerConnected$ = this.isTickerConnectedSubject.asObservable();
  jobStatus$ = this.jobStatusSubject.asObservable();

  private pollSub: Subscription | null = null;
  private POLL_INTERVAL_MS = 5000;

  constructor(private dataService: DataService) {
    // Start polling immediately
    this.start();
  }

  start(): void {
    if (this.pollSub) return; // already started

    // immediate fetch then interval
    this.fetchOnce();
    this.pollSub = interval(this.POLL_INTERVAL_MS).subscribe(() => this.fetchOnce());
  }

  stop(): void {
    if (this.pollSub) {
      this.pollSub.unsubscribe();
      this.pollSub = null;
    }
  }

  private fetchOnce(): void {
    // Check connection state first
    this.dataService.isTickerConnected().subscribe({
      next: (res) => {
        const connected = !!res?.isTickerConnected;
        this.isTickerConnectedSubject.next(connected);

        if (!connected) {
          // reset live data but keep jobStatus fetched below
          this.liveTickSubject.next(null);
        } else {
          // fetch live tick
          this.dataService.getLiveTickData().subscribe({
            next: (data) => this.liveTickSubject.next(data || null),
            error: (err) => {
              console.error('TickerService: error fetching live tick data', err);
              this.liveTickSubject.next(null);
            }
          });
        }
      },
      error: (err) => {
        console.error('TickerService: error checking ticker connection', err);
        this.isTickerConnectedSubject.next(false);
        this.liveTickSubject.next(null);
      }
    });

    // job status (optional) - separate call
    this.dataService.getPredictionJobStatus().subscribe({
      next: (s) => this.jobStatusSubject.next(s),
      error: (err) => { this.jobStatusSubject.next(null); }
    });
  }

  ngOnDestroy(): void {
    this.stop();
  }
}

