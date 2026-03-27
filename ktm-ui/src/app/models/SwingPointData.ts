export interface Candle {
    id: number;
    instrumentToken: number;
    openPrice: number;
    highPrice: number;
    lowPrice: number;
    closePrice: number;
    candleStartTime: string;
    candleEndTime: string;
  }
  
  export interface SwingPoint {
    swingHigh: Candle;
    swingLow: Candle;
  }
  
  export interface SwingPointData {
    [key: string]: SwingPoint;
  }