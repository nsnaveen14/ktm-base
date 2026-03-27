
export class Message {
    instrumentToken: number;
    oi: number;
    lastTradedPrice:number
    lastTradedTime: Date;
    tickTimestamp: Date;

    constructor(instrumentToken: number, oi: number, lastTradedPrice: number,lastTradedTime: Date,tickTimestamp: Date) {
        this.instrumentToken = instrumentToken;
        this.oi = oi;
        this.lastTradedPrice = lastTradedPrice;
        this.lastTradedTime = lastTradedTime;
        this.tickTimestamp = tickTimestamp;
      }
}