import { Component,ViewChild } from '@angular/core';
import { DataService } from '../../../services/data.service';
import { JobRunningDetails } from '../../../models/jobRunningDetails';
import { MatTable, MatTableModule, MatTableDataSource } from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import { MatSelectModule } from '@angular/material/select';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-job-running-status',
  imports: [CommonModule, MatTableModule, MatPaginatorModule, MatSelectModule],
  templateUrl: './job-running-status.component.html',
  styleUrl: './job-running-status.component.css'
})
export class JobRunningStatusComponent {

  jobRunningStatusList = new MatTableDataSource<JobRunningDetails>();
  @ViewChild(MatPaginator) paginator!: MatPaginator;
  displayedColumns: string[] = [
    'appJobConfigNum',
    'jobForExpiryDate',
    'jobName',
    'jobStartTime',
    'jobEndTime',
    'jobStatus',
        
  ];

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.fetchJobRunningDetailsForAllConfigNum();
  }

  ngAfterViewInit(): void {
    this.jobRunningStatusList.paginator = this.paginator;
  }

  fetchJobRunningDetailsForAllConfigNum(): void {
    this.dataService.getJobRunningStatusByConfigNum(-1).pipe().subscribe(response=>
     {
      this.jobRunningStatusList.data = response;
      if (this.paginator) {
        this.jobRunningStatusList.paginator = this.paginator;
      }
     }
    );
  }

}
