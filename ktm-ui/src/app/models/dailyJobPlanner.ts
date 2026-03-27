export interface DailyJobPlannerId {
    jobDate: string; // ISO date string
    appJobConfigNum: number;
  }
  
  export interface DailyJobPlanner {
    id: DailyJobPlannerId;
    jobForExpiryDate: string; // ISO date string
    lastModified: string; // ISO date string
    jobRequired: boolean;
  }