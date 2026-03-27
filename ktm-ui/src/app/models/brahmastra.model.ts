/**
 * Brahmastra Triple Confirmation Strategy Models
 */

// Signal Request
export interface SignalRequest {
  symbol: string;
  timeframe: string;
  fromDate: string;
  toDate: string;
  usePCR: boolean;
  supertrendPeriod?: number;
  supertrendMultiplier?: number;
  macdFastPeriod?: number;
  macdSlowPeriod?: number;
  macdSignalPeriod?: number;
  vwapTolerance?: number;
}

// Signal DTO
export interface SignalDTO {
  id: number;
  symbol: string;
  timeframe: string;
  signalType: string;
  signalTime: string;
  entryPrice: number;
  stopLoss: number;
  target1: number;
  target2: number;
  target3: number;
  riskRewardRatio: number;
  confidenceScore: number;
  supertrendValue: number;
  supertrendTrend: string;
  macdLine: number;
  macdSignalLine: number;
  macdHistogram: number;
  vwapValue: number;
  priceToVwapPercent: number;
  pcrValue: number;
  pcrBias: string;
  status: string;
  exitTime: string;
  exitPrice: number;
  pnl: number;
  pnlPercent: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// Backtest Request
export interface BacktestRequest {
  symbol: string;
  timeframe: string;
  fromDate: string;
  toDate: string;
  usePCR: boolean;
  initialCapital: number;
  riskPerTrade: number;
  maxPositionSize?: number;
  supertrendPeriod?: number;
  supertrendMultiplier?: number;
  macdFastPeriod?: number;
  macdSlowPeriod?: number;
  macdSignalPeriod?: number;
  vwapTolerance?: number;
  useTrailingStop?: boolean;
  trailingStopPercent?: number;
  usePartialProfits?: boolean;
  partialProfitPercent?: number;
}

// Trade Log
export interface TradeLog {
  tradeNumber: number;
  signalType: string;
  entryTime: string;
  exitTime: string;
  entryPrice: number;
  exitPrice: number;
  stopLoss: number;
  target: number;
  positionSize: number;
  pnl: number;
  pnlPercent: number;
  cumulativePnl: number;
  cumulativePnlPercent: number;
  exitReason: string;
  riskReward: number;
  drawdownAtExit: number;
  supertrendAtEntry?: number;
  macdAtEntry?: number;
  vwapAtEntry?: number;
  pcrAtEntry?: number;
}

// Equity Point
export interface EquityPoint {
  timestamp: string;
  equity: number;
  dailyReturn: number;
  cumulativeReturn: number;
  tradeNumber: number;
}

// Drawdown Point
export interface DrawdownPoint {
  timestamp: string;
  drawdown: number;
  drawdownPercent: number;
  peakEquity: number;
  currentEquity: number;
}

// Backtest Result
export interface BacktestResult {
  symbol: string;
  timeframe: string;
  backtestStart: string;
  backtestEnd: string;
  runTimestamp: string;
  initialCapital: number;
  finalCapital: number;
  netPnL: number;
  netPnLPercent: number;
  riskPerTrade: number;
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  maxConsecutiveWins: number;
  maxConsecutiveLosses: number;
  averageWin: number;
  averageLoss: number;
  averageRiskReward: number;
  profitFactor: number;
  expectancy: number;
  maxDrawdown: number;
  maxDrawdownPercent: number;
  maxDrawdownDate: string;
  currentDrawdown: number;
  sharpeRatio: number;
  sortinoRatio: number;
  calmarRatio: number;
  volatility: number;
  averageHoldingPeriodMinutes: number;
  longestTrade: number;
  shortestTrade: number;
  tradesPerDay: number;
  buySignals: number;
  sellSignals: number;
  buyWinRate: number;
  sellWinRate: number;
  signalsFilteredByPCR: number;
  pcrFilterImprovement: number;
  tradeLog: TradeLog[];
  equityCurve: EquityPoint[];
  drawdownCurve: DrawdownPoint[];
}

// Live Scan Result
export interface LiveScanResult {
  symbol: string;
  instrumentToken: number;
  scanTime: string;
  signalType: string;
  currentPrice: number;
  entryPrice: number;
  stopLoss: number;
  target1: number;
  target2: number;
  riskReward: number;
  confidenceScore: number;
  supertrendStatus: string;
  macdStatus: string;
  vwapStatus: string;
  pcrBias: string;
  vwap: number;
  supertrend: number;
  macdLine: number;
  macdSignal: number;
  pcr: number;
  isNewSignal: boolean;
  message: string;
  alertLevel: string;
}

// Symbol Summary
export interface SymbolSummary {
  symbol: string;
  instrumentToken: number;
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  totalPnL: number;
  totalPnLPercent: number;
  averageRR: number;
  maxDrawdown: number;
  sharpeRatio: number;
  activeSignals: number;
  lastSignalType: string;
  lastSignalPrice: number;
  currentTrend: string;
}

// Dashboard Summary
export interface DashboardSummary {
  generatedAt: string;
  totalPnL: number;
  totalPnLPercent: number;
  totalTrades: number;
  overallWinRate: number;
  overallProfitFactor: number;
  todaysTrades: number;
  todaysPnL: number;
  todaysWinningTrades: number;
  todaysLosingTrades: number;
  activeSignals: number;
  symbolSummaries: SymbolSummary[];
  currentMarketBias: string;
  niftyPCR: number;
  liveSignals: SignalDTO[];
  recentTrades: TradeLog[];
  last7DaysWinRate: number;
  last30DaysWinRate: number;
  currentDrawdown: number;
  strategyStatus: string;
  // Option Chain Integration
  optionChainMetrics?: OptionChainMetrics[];
  optionChainOverallBias?: string;
  maxPainNifty?: number;
  niftyGexRegime?: string;
}

// API Response Wrappers
export interface SignalResponse {
  success: boolean;
  symbol: string;
  timeframe: string;
  fromDate: string;
  toDate: string;
  usePCR: boolean;
  totalSignals: number;
  buySignals: number;
  sellSignals: number;
  signals: SignalDTO[];
  error?: string;
}

export interface BacktestResponse {
  success: boolean;
  result: BacktestResult;
  error?: string;
}

export interface LiveScanResponse {
  success: boolean;
  scannedAt: string;
  symbolsScanned: number;
  signalsFound: number;
  results: LiveScanResult[];
  error?: string;
}

export interface DashboardResponse {
  success: boolean;
  summary: DashboardSummary;
  error?: string;
}

export interface PCRResponse {
  success: boolean;
  niftyPCR: number;
  niftyBias: string;
  error?: string;
}

// ==================== Indicator Metrics ====================

export interface SupertrendData {
  value: number;
  trend: string;
  atrValue: number;
  upperBand: number;
  lowerBand: number;
  period: number;
  multiplier: number;
  priceDistance: number;
  priceDistancePercent: number;
  consecutiveBars: number;
  isTrendChange: boolean;
}

export interface MACDData {
  macdLine: number;
  signalLine: number;
  histogram: number;
  signal: string;
  crossover: boolean;
  crossoverType: string;
  fastPeriod: number;
  slowPeriod: number;
  signalPeriod: number;
  divergence: number;
  isConverging: boolean;
  barsSinceCrossover: number;
}

export interface VWAPData {
  value: number;
  position: string;
  priceToVwapPercent: number;
  upperBand1SD: number;
  lowerBand1SD: number;
  upperBand2SD: number;
  lowerBand2SD: number;
  cumulativeVolume: number;
  cumulativeTpv: number;
  tradingZone: string;
}

export interface IndicatorHistoryPoint {
  timestamp: string;
  price: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  supertrend: number;
  supertrendTrend: string;
  macdLine: number;
  macdSignal: number;
  macdHistogram: number;
  vwap: number;
  vwapUpperBand: number;
  vwapLowerBand: number;
}

export interface IndicatorMetrics {
  symbol: string;
  timeframe: string;
  timestamp: string;
  currentPrice: number;
  supertrend: SupertrendData;
  macd: MACDData;
  vwap: VWAPData;
  overallSignal: string;
  confidenceScore: number;
  recommendation: string;
  history: IndicatorHistoryPoint[];
}

export interface IndicatorMetricsResponse {
  success: boolean;
  metrics: IndicatorMetrics;
  error?: string;
}

export interface AllIndicatorMetricsResponse {
  success: boolean;
  timeframe: string;
  symbolsCount: number;
  metrics: IndicatorMetrics[];
  error?: string;
}

// ==================== Option Chain Integration ====================

export interface MaxPainData {
  maxPainStrike: number;
  maxPainSecondStrike: number;
  distanceFromSpot: number;
  distanceFromSpotPercent: number;
  priceRelation: string;
  confirmsBullish: boolean;
  confirmsBearish: boolean;
  pullStrength: number;
  actsAsSupport: boolean;
  actsAsResistance: boolean;
}

export interface StrikeOIChange {
  strikePrice: number;
  callOIChange: number;
  putOIChange: number;
  callOIChangePercent: number;
  putOIChangePercent: number;
  interpretation: string;
}

export interface OIAnalysisData {
  pcr: number;
  pcrChange: number;
  pcrTrend: string;
  pcrSignal: string;
  totalCallOI: number;
  totalPutOI: number;
  callOIChange: number;
  putOIChange: number;
  oiBuildUpType: string;
  confirmsUptrend: boolean;
  confirmsDowntrend: boolean;
  highestCallOIStrike: number;
  highestPutOIStrike: number;
  callOIConcentration: number;
  putOIConcentration: number;
  significantOIChanges: StrikeOIChange[];
}

export interface OptionChainMetrics {
  symbol: string;
  timestamp: string;
  spotPrice: number;
  atmStrike: number;
  maxPain: MaxPainData;
  oiAnalysis: OIAnalysisData;
  optionChainSignal: string;
  optionChainConfidence: number;
  optionChainBias: string;
  recommendedAction: string;
}

export interface OptionChainMetricsResponse {
  success: boolean;
  metrics: OptionChainMetrics;
  error?: string;
}

export interface AllOptionChainMetricsResponse {
  success: boolean;
  symbolsCount: number;
  metrics: OptionChainMetrics[];
  error?: string;
}

export interface OptionChainConfirmationResponse {
  success: boolean;
  symbol: string;
  signalType: string;
  confirms: boolean;
  optionChainSignal: string;
  optionChainBias: string;
  optionChainConfidence: number;
  recommendation: string;
  error?: string;
}

