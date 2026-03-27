import { Component } from '@angular/core';
import { DataService } from '../../../services/data.service';

@Component({
  selector: 'app-index-ltpchart',
  imports: [],
  templateUrl: './index-ltpchart.component.html',
  styleUrl: './index-ltpchart.component.css'
})
export class IndexLTPChartComponent {

  constructor(private dataService: DataService) {
    
  }


}
