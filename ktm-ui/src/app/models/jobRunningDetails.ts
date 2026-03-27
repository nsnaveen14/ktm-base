export interface JobRunningDetails {
    id: number;
    appJobConfigNum: number;
    jobStartTime: string; // ISO date string
    jobEndTime: string;   // ISO date string
    jobStatus: string;
    jobName: string;
    jobForExpiryDate: string; // ISO date string
    
}