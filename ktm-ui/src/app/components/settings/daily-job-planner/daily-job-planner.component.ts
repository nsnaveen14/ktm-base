import { Component,ViewChild } from '@angular/core';
import { DataService } from '../../../services/data.service';
import { DailyJobPlanner } from '../../../models/dailyJobPlanner';
import { MatTable, MatTableModule, MatTableDataSource } from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import { MatSelectModule } from '@angular/material/select';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import { MessageService } from '../../../services/message.service';
import { CommonReqRes } from '../../../models/CommonReqRes';

@Component({
  selector: 'app-daily-job-planner',
  imports: [CommonModule, MatTableModule, MatPaginatorModule, MatSelectModule,FormsModule,MatCheckboxModule,MatIconModule,MatTooltipModule],
  templateUrl: './daily-job-planner.component.html',
  styleUrl: './daily-job-planner.component.css'
})
export class DailyJobPlannerComponent {

  dailyJobPlannerPaginatorDataSource = new MatTableDataSource<DailyJobPlanner>();
  @ViewChild(MatPaginator) paginator!: MatPaginator;
  // Add these properties to your component class
  editingRow: DailyJobPlanner | null = null;
  editingColumn: string | null = null;

  constructor(private dataService: DataService,private messageService: MessageService) {}

  displayedColumns: string[] = [
    
    'appJobConfigNum',
    'jobDate',
    'jobForExpiryDate',
    'lastModified',
    'jobRequired'
  ];

  ngOnInit(): void {
    this.fetchDailyPlannerConfig();
  }

  fetchDailyPlannerConfig(): void {
    this.dataService.getDailyJobPlannerConfig().pipe().subscribe(response=>
     {
      this.dailyJobPlannerPaginatorDataSource.data = response;
      if (this.paginator) {
        this.dailyJobPlannerPaginatorDataSource.paginator = this.paginator;
      }
     }
    );
  }

  // Call this method to start editing
 startEdit(row: DailyJobPlanner, column: string) {
  this.editingRow = row;
  this.editingColumn = column;
 }

 // Call this method to save and stop editing
saveEdit(row: DailyJobPlanner, column: string, newValue: any) {
  (row as any)[column] = newValue;
  this.editingRow = null;
  this.editingColumn = null;
  // Optionally, call a service to persist the change
  this.dataService.modifyDailyJobPlannerConfig(row).subscribe({
  next: () => {
    this.messageService.sendMessage(
      new CommonReqRes(false, "Daily Job Planner configuration updated successfully.", 0, null, "success")
    );
  },
  error: (err) => {
    this.messageService.sendMessage(
      new CommonReqRes(true, "Failed to update Daily Job Planner configuration.", 0, err, "error")
    );
  }
});
}

}
