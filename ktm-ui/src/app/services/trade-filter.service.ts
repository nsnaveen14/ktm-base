import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class TradeFilterService {
  private filterTradeDecisionsSubject = new BehaviorSubject<boolean>(false);
  public filterTradeDecisions$: Observable<boolean> = this.filterTradeDecisionsSubject.asObservable();

  setFilterTradeDecisions(value: boolean): void {
    this.filterTradeDecisionsSubject.next(value);
  }

  getFilterTradeDecisions(): boolean {
    return this.filterTradeDecisionsSubject.value;
  }
}