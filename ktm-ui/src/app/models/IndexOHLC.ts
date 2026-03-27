export class IndexOHLC {
    appJobConfigNum: number;
    indexLTP: string;
    thresholdValue: string;
    dayLow: string;
    dayHigh: string;
  
    constructor(
      appJobConfigNum: number,
      indexLTP: string,
      thresholdValue: string,
      dayLow: string,
      dayHigh: string 
    ) {
      this.appJobConfigNum = appJobConfigNum;
      this.indexLTP = indexLTP;
      this.thresholdValue = thresholdValue;
      this.dayLow = dayLow;
      this.dayHigh = dayHigh;
    }
  }