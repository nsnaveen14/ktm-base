
export interface IndexLTPModel {
  id?: number;
  appJobConfigNum?: number;
  indexTS?: Date;
  indexLTP?: number;
  meanStrikePCR?: number;
  meanRateOI?: number;
  combiRate?: number;
  support?: string;
  resistance?: string;
  range?: string;
  tradeDecision?: string;
  maxPainSP?: number;
  maxPainSPSecond?: number;
  dayHigh?: string;
  dayLow?: string;
  jobIterationId?: number;
  maxPainCELTP?: number;
  maxPainPELTP?: number;
}
