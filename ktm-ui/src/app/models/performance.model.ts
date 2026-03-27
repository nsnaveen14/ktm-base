// Performance and Trade Result models

export interface TradeResult {
  id: number;
  tradeId: string;
  iobId: number;
  instrumentToken: number;
  instrumentName: string;
  timeframe: string;
  tradeDirection: string;
  tradeType: string;
  entryPrice: number;
  entryTime: string;
  entryReason: string;
  exitPrice: number;
  exitTime: string;
  exitReason: string;
  stopLoss: number;
  target1: number;
  target2: number;
  target3: number;
  trailingStop: number;
  quantity: number;
  lotSize: number;
  positionValue: number;
  grossPnl: number;
  netPnl: number;
  pnlPercent: number;
  pnlPoints: number;
  brokerage: number;
  taxes: number;
  riskAmount: number;
  rewardAmount: number;
  plannedRRRatio: number;
  achievedRRRatio: number;
  maxFavorableExcursion: number;
  maxAdverseExcursion: number;
  outcome: string;
  targetHit: string;
  stopLossHit: boolean;
  iobType: string;
  zoneHigh: number;
  zoneLow: number;
  signalConfidence: number;
  enhancedConfidence: number;
  hadFvg: boolean;
  wasTrendAligned: boolean;
  volumeType: string;
  durationMinutes: number;
  holdingPeriod: string;
  status: string;
  notes: string;
}

export interface PerformanceMetrics {
  id: number;
  metricDate: string;
  periodType: string;
  instrumentToken: number;
  instrumentName: string;
  strategyType: string;
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  breakevenTrades: number;
  winRate: number;
  lossRate: number;
  avgWinAmount: number;
  avgLossAmount: number;
  largestWin: number;
  largestLoss: number;
  winLossRatio: number;
  avgRRPlanned: number;
  avgRRAchieved: number;
  expectancy: number;
  profitFactor: number;
  totalGrossPnl: number;
  totalNetPnl: number;
  totalBrokerage: number;
  totalTaxes: number;
  avgPnlPerTrade: number;
  avgPointsPerTrade: number;
  maxDrawdown: number;
  maxDrawdownPercent: number;
  maxDrawdownDurationDays: number;
  currentDrawdown: number;
  peakEquity: number;
  currentWinStreak: number;
  maxWinStreak: number;
  currentLossStreak: number;
  maxLossStreak: number;
  avgTradeDurationMinutes: number;
  avgWinnerDurationMinutes: number;
  avgLoserDurationMinutes: number;
  target1HitCount: number;
  target2HitCount: number;
  target3HitCount: number;
  stopLossHitCount: number;
  target1HitRate: number;
  target2HitRate: number;
  target3HitRate: number;
  bullishIOBTrades: number;
  bearishIOBTrades: number;
  bullishWinRate: number;
  bearishWinRate: number;
  fvgConfluenceTrades: number;
  fvgConfluenceWinRate: number;
  trendAlignedTrades: number;
  trendAlignedWinRate: number;
  avgConfidenceWinners: number;
  avgConfidenceLosers: number;
  highConfidenceTrades: number;
  highConfidenceWinRate: number;
  morningSessionTrades: number;
  afternoonSessionTrades: number;
  morningSessionWinRate: number;
  afternoonSessionWinRate: number;
  calculationTimestamp: string;
}

export interface EquityCurvePoint {
  date: string;
  pnl: number;
  cumulativePnl: number;
  tradeId: string;
  outcome: string;
}

export interface PerformanceDashboard {
  allTimeMetrics: PerformanceMetrics;
  dailyMetrics: PerformanceMetrics[];
  recentTrades: TradeResult[];
  openTrades: TradeResult[];
  quickStats: {
    totalTrades: number;
    winRate: number;
    totalPnl: number;
    profitFactor: number;
    expectancy: number;
    maxDrawdown: number;
    avgRR: number;
  };
  timestamp: string;
}

export interface BacktestResult {
  backtestId: string;
  instrumentToken: number;
  timeframe: string;
  startDate: string;
  endDate: string;
  parameters: any;
  status: string;
  startTime: string;
  endTime: string;
  totalCandles: number;
  totalIOBsDetected: number;
  totalTradesSimulated: number;
  performance: {
    totalTrades: number;
    wins: number;
    losses: number;
    winRate: number;
    totalNetPnl: number;
    avgPnlPerTrade: number;
    avgWin: number;
    avgLoss: number;
    profitFactor: number;
    maxDrawdown: number;
    maxDrawdownPercent: number;
    avgRR: number;
    expectancy: number;
  };
  equityCurve: EquityCurvePoint[];
  distribution: any;
}

// Auto Trading models

export interface AutoTradingConfig {
  autoTradingEnabled: boolean;
  paperTradingMode: boolean;
  entryType: string;
  minConfidence: number;
  requireFvg: boolean;
  requireTrendAlignment: boolean;
  requireInstitutionalVolume: boolean;
  maxPositionSize: number;
  maxLotsPerTrade: number;
  maxOpenPositions: number;
  maxDailyTrades: number;
  maxDailyLoss: number;
  useDynamicSL: boolean;
  slAtrMultiplier: number;
  enableTrailingSL: boolean;
  trailingSLTrigger: string;
  trailingSLDistancePoints: number;
  bookPartialProfits: boolean;
  partialProfitPercent: number;
  partialProfitAt: string;
  defaultExitTarget: string;
  exitAtMarketClose: boolean;
  marketCloseTime: string;
  tradeStartTime: string;
  tradeEndTime: string;
  avoidFirstCandle: boolean;
  enabledInstruments: string;
  defaultProductType: string;
}

export interface AutoTradeOrder {
  id: number;
  orderId: string;
  iobId: number;
  tradeResultId: number;
  instrumentToken: number;
  instrumentName: string;
  tradingSymbol: string;
  exchange: string;
  orderType: string;
  transactionType: string;
  productType: string;
  quantity: number;
  price: number;
  triggerPrice: number;
  filledQuantity: number;
  pendingQuantity: number;
  averagePrice: number;
  status: string;
  statusMessage: string;
  kiteOrderId: string;
  orderTime: string;
  exchangeTime: string;
  fillTime: string;
  orderPurpose: string;
  parentOrderId: string;
  errorCode: string;
  errorMessage: string;
}

export interface OpenPosition {
  positionId: string;
  iobId: number;
  tradeResultId: number;
  entryOrderId: string;
  instrumentToken: number;
  instrumentName: string;
  direction: string;
  quantity: number;
  entryPrice: number;
  entryTime: string;
  stopLoss: number;
  target1: number;
  target2: number;
  target3: number;
  currentPrice: number;
  trailingStopActive: boolean;
  partialProfitBooked: boolean;
  unrealizedPnl?: number;
  unrealizedPnlPercent?: number;
}

export interface AutoTradingStats {
  isEnabled: boolean;
  openPositions: number;
  todaysOrders: number;
  todaysEntries: number;
  pendingOrders: number;
  paperTradingMode: boolean;
  maxDailyTrades: number;
  maxOpenPositions: number;
}

// Chart data models

export interface ChartCandle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface IOBZone {
  id: number;
  type: string;
  zoneHigh: number;
  zoneLow: number;
  zoneMidpoint: number;
  startTime: number;
  bosLevel: number;
  bosType: string;
  direction: string;
  status: string;
  confidence: number;
  hasFvg: boolean;
  fvgHigh: number;
  fvgLow: number;
  entryPrice: number;
  stopLoss: number;
  target1: number;
  target2: number;
  target3: number;
  color: string;
  borderColor: string;
}

export interface SwingPoint {
  time: number;
  price: number;
  type: string;
  position: string;
  color: string;
  shape: string;
  text: string;
}

export interface ChartLevel {
  label: string;
  price: number;
  color: string;
  lineStyle: string;
  lineWidth: number;
  axisLabelVisible: boolean;
}

export interface CompleteChartData {
  success: boolean;
  instrumentToken: number;
  instrumentName: string;
  interval: string;
  candles: ChartCandle[];
  iobZones: IOBZone[];
  swingHighs: SwingPoint[];
  swingLows: SwingPoint[];
  currentPrice: number;
  distanceToZone: any;
  timestamp: string;
}

// Real-time price models

export interface PriceUpdate {
  instrumentToken: number;
  lastPrice: number;
  timestamp: string;
  previousPrice: number;
  change: number;
  changePercent: number;
}

export interface PriceTick {
  type: string;
  timestamp: string;
  prices: {
    NIFTY?: {
      price: number;
      change: number;
      changePercent: number;
      token: number;
    };
  };
}
