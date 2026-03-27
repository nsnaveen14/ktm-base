// Models for Internal Order Block (IOB) Analysis

export interface InternalOrderBlock {
  id?: number;
  instrumentToken?: number;
  instrumentName?: string;
  timeframe?: string;
  detectionTimestamp?: string;

  // Order Block Candle Details
  obCandleTime?: string;
  obType?: string; // BULLISH_IOB, BEARISH_IOB
  obHigh?: number;
  obLow?: number;
  obOpen?: number;
  obClose?: number;

  // Zone levels
  zoneHigh?: number;
  zoneLow?: number;
  zoneMidpoint?: number;

  // Market context
  currentPrice?: number;
  distanceToZone?: number;
  distancePercent?: number;

  // Break of Structure
  bosLevel?: number;
  bosType?: string; // BULLISH_BOS, BEARISH_BOS

  // Fair Value Gap
  hasFvg?: boolean;
  fvgHigh?: number;
  fvgLow?: number;

  // FVG Validation (6-Factor)
  fvgValid?: boolean;
  fvgValidationScore?: number;
  fvgValidationDetails?: string;
  fvgPriority?: number;
  fvgUnmitigated?: boolean;
  fvgCandleReactionValid?: boolean;
  fvgSrConfluence?: boolean;
  fvgGannBoxValid?: boolean;
  fvgBosConfirmed?: boolean;

  // Trade setup
  tradeDirection?: string; // LONG, SHORT
  entryPrice?: number;
  stopLoss?: number;
  target1?: number;
  target2?: number;
  target3?: number;
  riskRewardRatio?: number;

  // Status
  status?: string; // FRESH, MITIGATED, EXPIRED, TRADED
  isValid?: boolean;
  validationNotes?: string;
  signalConfidence?: number;

  // Trade execution
  tradeTaken?: boolean;
  tradeId?: string;
  mitigationTime?: string;

  // Alert tracking flags
  detectionAlertSent?: boolean;
  mitigationAlertSent?: boolean;
  target1AlertSent?: boolean;
  target2AlertSent?: boolean;
  target3AlertSent?: boolean;
  iobSignature?: string;

  // Trade timeline tracking
  entryTriggeredTime?: string;
  actualEntryPrice?: number;
  stopLossHitTime?: string;
  stopLossHitPrice?: number;
  target1HitTime?: string;
  target1HitPrice?: number;
  target2HitTime?: string;
  target2HitPrice?: number;
  target3HitTime?: string;
  target3HitPrice?: number;
  maxFavorableExcursion?: number;
  maxAdverseExcursion?: number;
  tradeOutcome?: string; // WIN, LOSS, BREAKEVEN, ACTIVE
  pointsCaptured?: number;
}

export interface IOBDashboard {
  iobs: InternalOrderBlock[];
  totalIOBs: number;
  bullishCount: number;
  bearishCount: number;
  timestamp: string;
}

export interface IOBDetailedAnalysis {
  instrumentToken: number;
  instrumentName: string;
  detectedIOBs: InternalOrderBlock[];
  freshBullishIOBs: InternalOrderBlock[];
  freshBearishIOBs: InternalOrderBlock[];
  tradableIOBs: InternalOrderBlock[];
  activeIOBs: InternalOrderBlock[];
  mitigatedIOBs: InternalOrderBlock[];
  completedIOBs: InternalOrderBlock[];
  activeCount: number;
  mitigatedCount: number;
  completedCount: number;
  timestamp: string;
}

export interface IOBTradeSetup {
  iobId: number;
  instrumentName: string;
  direction: string;
  entryZone: {
    high: number;
    low: number;
  };
  entryPrice: number;
  stopLoss: number;
  target1: number;
  target2: number;
  target3: number;
  riskRewardRatio: number;
  confidence: number;
  hasFVG: boolean;
  validationNotes: string;
}

export interface IOBStatistics {
  totalToday: number;
  bullishToday: number;
  bearishToday: number;
  mitigatedToday: number;
  tradedToday: number;
  freshCount: number;
}

export interface AllIndicesIOBData {
  NIFTY: IOBDetailedAnalysis;
  success: boolean;
}

// IOB Auto Trade Models

export interface IOBTradeResult {
  id?: number;
  tradeId?: string;
  iobId?: number;
  instrumentToken?: number;
  instrumentName?: string;
  timeframe?: string;
  iobType?: string;
  tradeDirection?: string;
  signalConfidence?: number;
  hasFvg?: boolean;
  zoneHigh?: number;
  zoneLow?: number;
  plannedEntry?: number;
  actualEntry?: number;
  entryTime?: string;
  entryTrigger?: string;
  plannedStopLoss?: number;
  actualStopLoss?: number;
  target1?: number;
  target2?: number;
  target3?: number;
  plannedRR?: number;
  exitPrice?: number;
  exitTime?: string;
  exitReason?: string;
  pointsCaptured?: number;
  achievedRR?: number;
  isWinner?: boolean;
  targetHit?: number;
  grossPnl?: number;
  netPnl?: number;
  status?: string;
  tradeMode?: string;
  htfAligned?: boolean;
  mtfConfluenceScore?: number;
}

export interface IOBAutoTradeConfig {
  autoTradingEnabled: boolean;
  minConfidence: number;
  maxZoneDistancePercent: number;
  maxOpenTrades: number;
  dailyLossLimit: number;
  riskPerTrade: number;
  trailingSLActivation: number;
  trailingSLDistance: number;
  entryOnZoneTouch: boolean;
  entryOnZoneMidpoint: boolean;
  requireFVG: boolean;
  requireHTFAlignment: boolean;
}

export interface IOBTradingSummary {
  totalTrades: number;
  openTrades: number;
  closedTrades: number;
  wins: number;
  losses: number;
  winRate: number;
  totalPnl: number;
  averageRR: number;
  autoTradingEnabled: boolean;
  timestamp: string;
}

export interface IOBPerformanceStats {
  period: { start: string; end: string };
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  averageRR: number;
  totalPnl: number;
  performanceByType: Array<{
    category: string;
    totalTrades: number;
    wins: number;
    avgRR: number;
  }>;
  performanceByTimeframe: Array<{
    category: string;
    totalTrades: number;
    wins: number;
    avgRR: number;
  }>;
}

export interface IOBBacktestResult {
  success: boolean;
  instrumentToken: number;
  period: { start: string; end: string };
  timeframe: string;
  totalTrades: number;
  wins: number;
  losses: number;
  winRate: number;
  totalPnl: number;
  averageRR: number;
  totalPointsCaptured: number;
  trades: IOBTradeResult[];
}

export interface IOBRiskMetrics {
  dailyLossLimitReached: boolean;
  maxOpenTradesReached: boolean;
  portfolioHeat: number;
  openTradesCount: number;
}

export interface IOBMTFAnalysis {
  instrumentToken: number;
  instrumentName: string;
  htfBias: string;
  alignedIOBs: InternalOrderBlock[];
  totalAligned: number;
  summaryByTimeframe: {
    [timeframe: string]: {
      bullish: number;
      bearish: number;
      total: number;
    };
  };
  iobsByTimeframe: {
    [timeframe: string]: InternalOrderBlock[];
  };
  timestamp: string;
}


