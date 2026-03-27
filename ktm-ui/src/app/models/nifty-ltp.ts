export class NiftyLTP {
  id: number;
  niftyTS: string;  // LocalDateTime to Date in TypeScript
  niftyLTP: number;  // Double to number in TypeScript
  meanStrikePCR: number;  // Double to number in TypeScript
  meanRateOI: number;  // Double to number in TypeScript
  combiRate: number;  // Double to number in TypeScript
  support: string;  // String remains string in TypeScript
  resistance: string;  // String remains string in TypeScript
  tradeDecision: string;  // String remains string in TypeScript
  range: string;
  tradeManagement: string;  // String remains string in TypeScript
  tradeHoldCount: number;
  powerTrade: boolean;
  powerTradeType: string;
  iBuyers: boolean;
  iSellers: boolean;
  icebergTrendCounter: number;
  icebergTradeType: string;
  cpts: number;
  maxPainSP: number;  // Double to number in TypeScript
  maxPainSPSecond: number;
  straddleUpside: boolean;
 straddleDownside: boolean;
  closeFull: boolean;
  maxPainCELTP: number;
  maxPainPELTP: number;
 

  constructor() {
    this.id = 0;
    this.niftyTS = '';
    this.niftyLTP = 0;
    this.meanStrikePCR = 0;
    this.meanRateOI = 0;
    this.combiRate = 0;
    this.support = '';
    this.resistance = '';
    this.tradeDecision = '';
    this.range = '';
    this.tradeManagement = '';
    this.tradeHoldCount = 0;
    this.powerTrade = false;
    this.powerTradeType = '';
    this.iBuyers = false;
    this.iSellers = false;
    this.icebergTrendCounter = 0;
    this.icebergTradeType = '';
    this.cpts = 0.0;
    this.maxPainSP = 0;
    this.maxPainSPSecond = 0;
    this.straddleUpside = false;
    this.straddleDownside = false;
    this.closeFull = false;
    this.maxPainCELTP = 0;
    this.maxPainPELTP = 0;

  }
}

