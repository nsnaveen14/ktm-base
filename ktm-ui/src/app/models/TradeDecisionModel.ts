import { AppJobConfig } from "./AppJobConfig";

export interface TradeDecisionModel {
  id?: number;
  tradeDecision?: string;
  tradeDecisionType?: string;
  tradeDecisionTS?: string;
  indexLTP?: number;
  entryIndexLTP?: number;
  targetIndexLTP?: number;
  stopLossIndexLTP?: number;
  status?: string;
  trade_decision_result?: string;
  trade_decision_result_ts?: string;
  appJobConfig?: AppJobConfig;
  jobIterationDetails?: number;
  swingTarget?: number;
  swingTaken?:boolean;
  confirmationTaken?:boolean;
}



