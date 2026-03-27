import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { CommonReqRes } from '../models/CommonReqRes';

@Injectable({
  providedIn: 'root'
})
export class MessageService {
  private messageSubject = new Subject<CommonReqRes>();
  message$ = this.messageSubject.asObservable();

  sendMessage(message: CommonReqRes) {
    this.messageSubject.next(message);
  }
}
