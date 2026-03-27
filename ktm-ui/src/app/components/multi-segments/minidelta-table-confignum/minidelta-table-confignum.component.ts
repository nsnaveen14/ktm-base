import { Component,Input,ViewChild,AfterViewInit,SimpleChanges } from '@angular/core';
import { MatTable,MatTableModule} from '@angular/material/table';
import { MiniDelta } from '../../../models/minidelta';
import { CommonModule } from '@angular/common';
import { AppJobConfig } from '../../../models/AppJobConfig';
import { DataService } from '../../../services/data.service';

@Component({
  selector: 'app-minidelta-table-confignum',
  imports: [MatTableModule,CommonModule],
  templateUrl: './minidelta-table-confignum.component.html',
  styleUrl: './minidelta-table-confignum.component.css'
})
export class MinideltaTableConfignumComponent {

  @Input() selectedConfigFromMultiSegment: AppJobConfig | any;
   miniDeltaData: { [key: number]: MiniDelta[] } = {}; // Indexed by appJobConfigNum
   miniDeltaDataSource: MiniDelta[]=[];
   miniDeltaDataTableColumns:string[] = ['Time','Strike Price','CallOI','PutOI','StrikePCR','Rate OI','CallOIChange','PutOIChange'];


   constructor(private dataService: DataService) {}

   ngOnInit(): void {
    if (this.selectedConfigFromMultiSegment) {
      this.getMiniDeltaData(this.selectedConfigFromMultiSegment.appJobConfigNum);
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
      if (
        changes['selectedConfigFromMultiSegment'] &&
        !changes['selectedConfigFromMultiSegment'].firstChange
      ) {
        
        this.getMiniDeltaData(this.selectedConfigFromMultiSegment.appJobConfigNum);
      }
      
   }

  getMiniDeltaData(appJobConfigNum: number): void {
      this.dataService.getMiniDeltaDataByAppJobConfigNum(appJobConfigNum).subscribe(
        (response: MiniDelta[]) => {
          this.miniDeltaData[appJobConfigNum] = response;
       //   console.log('Fetched MiniDelta data:', this.miniDeltaData[appJobConfigNum]);
          this.miniDeltaDataSource = this.miniDeltaData[appJobConfigNum];
        }
      );
    }

   getColorClass(value: string): string {
        return value;
       }

    round(value: number): number {
      return Math.round(value);
  }

  addNewMiniDeltaMessage(message: MiniDelta[]): void {
    //  console.log('Adding new MiniDelta message:', message);
      const appJobConfigNum = message[0].appJobConfigNum;
      if (appJobConfigNum === undefined) {
        // Optionally handle the error or just return
        return;
      }
      if (!this.miniDeltaData[appJobConfigNum]) {
        this.miniDeltaData[appJobConfigNum] = [];
      }
      this.miniDeltaData[appJobConfigNum] = message;
      if (this.selectedConfigFromMultiSegment && this.selectedConfigFromMultiSegment.appJobConfigNum === appJobConfigNum) {
        this.miniDeltaDataSource = this.miniDeltaData[appJobConfigNum];      
        }
      }
}
