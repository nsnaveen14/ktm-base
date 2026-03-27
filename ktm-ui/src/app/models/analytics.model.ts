// Models for Candle Prediction Analytics

export interface PredictedCandleStick {
  id: number;
  instrumentToken: number;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
  candleStartTime: string;
  candleEndTime: string;
  predictionGeneratedAt: string;
  predictionSequence: number;
  confidenceScore: number;
  trendDirection: string;
  predictedVolatility: number;
  supportLevel: number;
  resistanceLevel: number;
  maxPainStrike: number;
  pcrAtPrediction: number;
  predictionBasis: string;
  verified: boolean;
  actualClosePrice: number;
  predictionAccuracy: number;
}

export interface PredictionDeviation {
  id: number;
  instrumentToken: number;
  verificationTime: string;
  tradingDate: string;
  batchId: string;
  predictionsVerified: number;
  avgCloseDeviation: number;
  avgCloseDeviationPercent: number;
  maxCloseDeviation: number;
  minCloseDeviation: number;
  closeDeviationStd: number;
  avgHighDeviation: number;
  avgHighDeviationPercent: number;
  avgLowDeviation: number;
  avgLowDeviationPercent: number;
  directionAccuracyPercent: number;
  bullishPredictions: number;
  bearishPredictions: number;
  neutralPredictions: number;
  correctBullish: number;
  correctBearish: number;
  systematicBias: number;
  biasDirection: string;
  predictedAvgVolatility: number;
  actualAvgVolatility: number;
  volatilityUnderestimateRatio: number;
  avgPcr: number;
  avgVix: number;
  marketTrend: string;
  marketHour: number;
  isOpeningHour: boolean;
  isClosingHour: boolean;
  dayOfWeek: number;
  isExpiryDay: boolean;
  suggestedCloseCorrection: number;
  suggestedVolatilityCorrection: number;
  cumulativeSessions: number;
  cumulativeAvgDeviation: number;
  cumulativeAccuracy: number;
  seq1AvgDeviation: number;
  seq2AvgDeviation: number;
  seq3AvgDeviation: number;
  seq4AvgDeviation: number;
  seq5AvgDeviation: number;
  createdAt: string;
}

export interface TradeSetup {
  id: number;
  instrumentToken: number;
  tradeDirection: string;
  setupType: string;
  confidence: number;
  entryPrice: number;
  entryType: string;
  entryReason: string;
  target1: number;
  target2: number;
  target3: number;
  targetReason: string;
  stopLoss: number;
  trailingStopDistance: number;
  stopLossReason: string;
  riskPoints: number;
  rewardPoints1: number;
  riskRewardRatio1: number;
  riskRewardRatio2: number;
  suggestedOptionType: string;
  suggestedStrike: number;
  optionStrategy: string;
  currentPrice: number;
  marketTrend: string;
  smcBias: string;
  smcConfidence: number;
  channelType: string;
  nearestFibLevel: string;
  pcrValue: number;
  maxPainStrike: number;
  createdAt: string;
  validUntil: string;
  isValid: boolean;
  invalidReason: string;
  isExecuted: boolean;
  executionPrice: number;
  executionTime: string;
  isClosed: boolean;
  exitPrice: number;
  exitTime: string;
  exitReason: string;
  profitLoss: number;
}

export interface PredictionJobStatus {
  isActive: boolean;
  isTickerConnected: boolean;
  isWithinMarketHours: boolean;
  totalPredictionRuns: number;
  totalVerificationRuns: number;
  lastPredictionTime: string;
  lastVerificationTime: string;
  todayVerificationCount: number;
  todayAvgDeviationPercent: number;
  todayDirectionAccuracyPercent: number;
  latestBatchId: string;
  latestAvgDeviation: number;
  latestSystematicBias: number;
  latestBiasDirection: string;
  sessionAccuracy: number;
  correctionFactors: CorrectionFactors;
}

export interface CorrectionFactors {
  closeCorrection: number;
  volatilityMultiplier: number;
  hourlyCorrection: number;
  expiryDayCorrection?: number;
  directionAccuracy: number;
  seq1Correction?: number;
  seq2Correction?: number;
  seq3Correction?: number;
  seq4Correction?: number;
  seq5Correction?: number;
}

export interface DeviationSummary {
  todayDeviationCount: number;
  avgDeviationPercent: number;
  avgDirectionAccuracy: number;
  latestBias: number;
  latestBiasDirection: string;
  cumulativeSessions: number;
  cumulativeAvgDeviation: number;
  cumulativeAccuracy: number;
  correctionFactors: CorrectionFactors;
  sessionAccuracy: number;
  jobActive: boolean;
  jobStats: PredictionJobStatus;
  dataSource?: string;  // 'historical' or 'none' when no today's data
  lastCalculationDate?: string;  // Date of last calculation when using historical data
  message?: string;  // Informational message for the user
}

export interface TradeSetupPerformance {
  todaySetupsCount: number;
  todayValidSetups: number;
  weeklyWinRate: number;
  weeklyProfitLoss: number;
  weeklyClosedSetups: number;
  averageRiskReward: number;
}

// Chart data models
export interface CandleChartData {
  timestamp: number;
  predictedClose: number;
  actualClose: number;
  deviation: number;
}

export interface AnalysisData {
  greeksAnalysis?: GreeksAnalysis;
  optionChainAnalysis?: OptionChainAnalysis;
  priceActionAnalysis?: PriceActionAnalysis;
  smcAnalysis?: SMCAnalysis;
  channelPattern?: ChannelPattern;
  fibonacciLevels?: FibonacciLevels;
  technicalLevels?: TechnicalLevels;
}

export interface GreeksAnalysis {
  vixValue: number;
  vixBias: string;
  isHighVix: boolean;
  isLowVix: boolean;
  atmTheta: number;
  thetaDecayRate: number;
  daysToExpiry: number;
  isExpiryDay: boolean;
  isNearExpiry: boolean;
  thetaBias: string;
  overallGreeksBias: string;
}

export interface OptionChainAnalysis {
  pcr: number;
  maxPainStrike: number;
  callOITotal: number;
  putOITotal: number;
  callOIChangeTotal: number;
  putOIChangeTotal: number;
  bias: string;
  strongestSupport: number;
  strongestResistance: number;
}

export interface PriceActionAnalysis {
  marketStructure: string;
  dominantPattern: string;
  recentMomentum: string;
  priceActionBias: string;
  isTrendingUp: boolean;
  isTrendingDown: boolean;
  isConsolidating: boolean;
}

export interface SMCAnalysis {
  smcBias: string;
  smcConfidence: number;
  tradeSuggestion: string;
  tradeSuggestionReason: string;
  orderBlocks: OrderBlock[];
  fairValueGaps: FairValueGap[];
  nearestBullishOB: number;
  nearestBearishOB: number;
  nearestBullishFVG: number;
  nearestBearishFVG: number;
}

export interface OrderBlock {
  type: string;
  topPrice: number;
  bottomPrice: number;
  strength: number;
  isMitigated: boolean;
}

export interface FairValueGap {
  type: string;
  topPrice: number;
  bottomPrice: number;
  midPoint: number;
  isFilled: boolean;
}

export interface ChannelPattern {
  channelType: string;
  upperBound: number;
  lowerBound: number;
  channelMid: number;
  channelWidth: number;
  channelSlope: number;
  isValid: boolean;
  pricePositionInChannel: string;
  channelBias: string;
}

export interface FibonacciLevels {
  swingHigh: number;
  swingLow: number;
  fib236: number;
  fib382: number;
  fib500: number;
  fib618: number;
  fib786: number;
  nearestFibLevel: string;
  nearestFibPrice: number;
  distanceToNearestFib: number;
  fibBias: string;
  isRetracement: boolean;
}

export interface TechnicalLevels {
  strongestSupport: number;
  strongestResistance: number;
  allSupportLevels: number[];
  allResistanceLevels: number[];
  overallBias: string;
  analysisConfidence: number;
}

export interface LiveTickData {
  niftyLTP: number;
  niftyChange?: number;
  niftyChangePercent?: number;
  niftyOpen?: number;
  niftyHigh?: number;
  niftyLow?: number;
  previousDayHigh?: number;
  previousDayLow?: number;
  previousDayOpen?: number;
  previousDayClose?: number;
  vixValue?: number;
  atmStrike: number;
  atmCELTP: number;
  atmPELTP: number;
  atmCEChange?: number;
  atmPEChange?: number;
  atmCESymbol?: string;
  atmPESymbol?: string;
  cePeDiff: number;
  straddlePremium: number;
  syntheticFuture: number;
  sentiment: string;
  timestamp: string;
  isLive: boolean;
  tickerMapSize?: number;
  error?: string;
}

export interface EMAChartData {
  ema9: [number, number][];
  ema21: [number, number][];
  ema50: [number, number][];
  priceSeries: [number, number][];
  dataPoints: number;
  lastUpdated: string;
  currentEMA9?: number;
  currentEMA21?: number;
  currentEMA50?: number;
  emaTrend?: string;        // BULLISH, BEARISH, NEUTRAL
  emaAlignment?: string;    // STRONG_BULLISH (9>21>50), STRONG_BEARISH (9<21<50), MIXED
  tradeSignal?: string;     // BUY, SELL, HOLD
  signalStrength?: string;  // STRONG, MODERATE, WEAK
  signalReason?: string;    // Explanation of the signal
  emaSpread9_21?: number;   // Difference between 9 and 21 EMA
  emaSpread21_50?: number;  // Difference between 21 and 50 EMA
  dataSource?: string;
  error?: string;
  message?: string;
}

export interface RollingChartData {
  actualSeries: [number, number][];
  predictedSeries: [number, number][];
  windowStart: number;
  windowEnd: number;
  currentTime: number;
  actualCount: number;
  predictedCount: number;
  lastUpdated: string;
  error?: string;
}

// ============= Simulated Trading Models =============

export interface SimulatedTrade {
  id: number;
  tradeId: string;
  tradeDate: string;
  signalSource: string; // TRADE_SETUP, EMA_CROSSOVER, MANUAL
  signalType: string; // BUY, SELL
  signalStrength: string; // STRONG, MODERATE, WEAK
  optionType: string; // CE, PE
  strikePrice: number;
  underlyingPriceAtEntry: number;
  quantity: number;
  lotSize: number;
  numLots: number;
  entryPrice: number;
  entryTime: string;
  entryReason: string;
  exitPrice?: number;
  exitTime?: string;
  exitReason?: string; // TARGET_HIT, STOPLOSS_HIT, TRAILING_SL, MANUAL, TIME_EXIT
  targetPrice: number;
  stopLossPrice: number;
  trailingStopLoss?: number;
  riskRewardRatio: number;
  premiumT1?: number;
  premiumT2?: number;
  premiumT3?: number;
  grossPnl?: number;
  brokerage?: number;
  netPnl?: number;
  pnlPercentage?: number;
  status: string; // PENDING, OPEN, CLOSED, CANCELLED
  isProfitable?: boolean;
  vixAtEntry?: number;
  marketTrend?: string;
  peakPrice?: number;
  peakPnl?: number;
  createdAt: string;
  updatedAt: string;
}

export interface TradingLedger {
  id: number;
  tradeDate: string;
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  breakevenTrades: number;
  winRate: number;
  grossPnl: number;
  totalBrokerage: number;
  netPnl: number;
  tradeSetupTrades: number;
  emaCrossoverTrades: number;
  tradeSetupPnl: number;
  emaCrossoverPnl: number;
  buyTrades: number;
  sellTrades: number;
  buyPnl: number;
  sellPnl: number;
  ceTrades: number;
  peTrades: number;
  cePnl: number;
  pePnl: number;
  targetHitCount: number;
  stoplossHitCount: number;
  trailingSlCount: number;
  timeExitCount: number;
  avgProfitPerTrade: number;
  avgLossPerTrade: number;
  profitFactor: number;
  maxDrawdown: number;
  peakPnl: number;
  startingCapital: number;
  endingCapital: number;
  capitalReturnPercent: number;
  niftyOpen: number;
  niftyClose: number;
  niftyChangePercent: number;
  avgVix: number;
  createdAt: string;
  updatedAt: string;
}

export interface TradingSummary {
  totalTrades: number;
  closedTrades: number;
  openTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  grossPnl: number;
  netPnl: number;
  unrealizedPnl: number;
  autoTradingEnabled: boolean;
  lastUpdated: string;
}

export interface TradingConfig {
  autoTradingEnabled: boolean;
  numLots: number;
  quantity: number;
  targetPercent: number;
  stoplossPercent: number;
  maxDailyLoss: number;
  maxDailyTrades: number;
  lotSize: number;
  // trailing SL runtime parameters
  trailingActivationThresholdPercent?: number;
  trailingTrailPercentOfProfit?: number;
}

export interface TradeSignal {
  hasSignal: boolean;
  signalSource?: string;
  signalType?: string;
  signalStrength?: string;
  reason?: string;
  ema9?: number;
  ema21?: number;
  ema50?: number;
  alignment?: string;
  confidence?: number;
  setupId?: number;
}
