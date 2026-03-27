export class AppJobConfig {
    appJobConfigNum: number;
    appIndexConfig: AppIndexConfig;
    jobType: JobType;
    isActive: boolean;
  
    constructor(data: Partial<AppJobConfig>) {
      this.appJobConfigNum = data.appJobConfigNum || 0;
      this.appIndexConfig = new AppIndexConfig(data.appIndexConfig || {});
      this.jobType = new JobType(data.jobType || {});
      this.isActive = data.isActive || false;
    }
  }
  
  export class AppIndexConfig {
    indexId: number;
    indexName: string;
    instrument_token: number;
    snapshotExpression: string;
    jobStartExpression: string;
    jobEndExpression: string;
    strikePriceName: string;
    strikePriceSegment: string;
    priceGap: number;
    defaultThreshold: number;
    atmRange: number;
    active: boolean;
    tfactor: number;
  
    constructor(data: Partial<AppIndexConfig>) {
      this.indexId = data.indexId || 0;
      this.indexName = data.indexName || '';
      this.instrument_token = data.instrument_token || 0;
      this.snapshotExpression = data.snapshotExpression || '';
      this.jobStartExpression = data.jobStartExpression || '';
      this.jobEndExpression = data.jobEndExpression || '';
      this.strikePriceName = data.strikePriceName || '';
      this.strikePriceSegment = data.strikePriceSegment || '';
      this.priceGap = data.priceGap || 0;
      this.defaultThreshold = data.defaultThreshold || 0;
      this.atmRange = data.atmRange || 0;
      this.active = data.active || false;
      this.tfactor = data.tfactor || 0;
    }
  }
  
  export class JobType {
    jobTypeCode: number;
    jobType: string;
    jobIterationDelaySeconds: number;
  
    constructor(data: Partial<JobType>) {
      this.jobTypeCode = data.jobTypeCode || 0;
      this.jobType = data.jobType || '';
      this.jobIterationDelaySeconds = data.jobIterationDelaySeconds || 0;
    }
  }