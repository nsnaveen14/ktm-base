import { Component, Input } from '@angular/core';
import { SwingPointData } from '../../models/SwingPointData';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-swing-candle-modal',
  standalone: true,
  templateUrl: './swing-candle-modal.component.html',
  styleUrls: ['./swing-candle-modal.component.css'],
  imports: [CommonModule]
})
export class SwingCandleModalComponent {
  @Input() swingCandleData: SwingPointData | null = null;

  constructor(public activeModal: NgbActiveModal) {}
}