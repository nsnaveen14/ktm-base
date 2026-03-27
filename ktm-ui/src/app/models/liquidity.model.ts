// Models for Liquidity Zone Analysis

export interface LiquidityZoneAnalysis {
  id?: number;
  instrumentToken?: number;
  instrumentName?: string;
  timeframe?: string;
  analysisTimestamp?: string;
  currentPrice?: number;
  currentHigh?: number;
  currentLow?: number;

  // Previous and current day levels
  previousDayHigh?: number;
  previousDayLow?: number;
  dayBeforeYesterdayHigh?: number;
  dayBeforeYesterdayLow?: number;
  currentDayHigh?: number;
  currentDayLow?: number;

  // Timeframe specific swing points
  timeframeHigh1?: number;
  timeframeHigh2?: number;
  timeframeHigh3?: number;
  timeframeLow1?: number;
  timeframeLow2?: number;
  timeframeLow3?: number;

  // Liquidity zones (stop loss clusters)
  buySideLiquidity1?: number;
  buySideLiquidity2?: number;
  buySideLiquidity3?: number;
  sellSideLiquidity1?: number;
  sellSideLiquidity2?: number;
  sellSideLiquidity3?: number;

  // Liquidity grab detection
  buySideGrabbed?: boolean;
  sellSideGrabbed?: boolean;
  grabbedLevel?: number;
  grabType?: string;

  // Market structure
  marketStructure?: string;
  trendStrength?: number;

  // Trade setup
  tradeSignal?: string;
  signalConfidence?: number;
  entryPrice?: number;
  stopLoss?: number;
  target1?: number;
  target2?: number;
  target3?: number;
  riskRewardRatio?: number;
  positionType?: string;
  strategyType?: string;

  // Validation
  isValidSetup?: boolean;
  rejectionReason?: string;
  notes?: string;

  volumeAtGrab?: number;
  createdAt?: string;
}

export interface LiquidityDashboard {
  analyses: LiquidityZoneAnalysis[];
  totalAnalyses: number;
  validSetups: number;
  liquidityGrabs: number;
  timestamp: string;
}

export interface MultiTimeframeAnalysis {
  instrumentToken: number;
  instrumentName: string;
  timeframes: {
    '5min'?: LiquidityZoneAnalysis;
    '15min'?: LiquidityZoneAnalysis;
    '1hour'?: LiquidityZoneAnalysis;
  };
  timestamp: string;
}

export interface LiquidityChartData {
  buySideZones: LiquidityLevel[];
  sellSideZones: LiquidityLevel[];
  currentPrice: number;
  previousDayHigh?: number;
  previousDayLow?: number;
  currentDayHigh?: number;
  currentDayLow?: number;
}

export interface LiquidityLevel {
  level: number;
  strength: number; // 1-3
}

export interface ActiveSetup {
  setup: LiquidityZoneAnalysis;
  distance: number;
  urgency: string;
}

export interface AllIndicesData {
  NIFTY: MultiTimeframeAnalysis;
  timestamp: string;
}

