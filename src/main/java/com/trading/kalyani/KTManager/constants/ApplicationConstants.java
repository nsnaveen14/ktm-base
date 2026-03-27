package com.trading.kalyani.KTManager.constants;

public class ApplicationConstants {
    
    public static final String SIMPLE_BROKER = "/topic";
    public static final String CHAT_ENDPOINT = "/chat";
    public static final String APPLICATION_DESTINATION_PREFIX = "/app";
    public static final String INSTRUMENT_TOPIC = "/instrument";
    public static final String MINI_DELTA_TOPIC = "/miniDelta";
    public static final String NIFTY_LTP_TOPIC = "/niftyLTP";
    public static final String MESSAGE_TOPIC = "/messages";
    public static final String NIFTY_LTP_VALUE_TOPIC = "/niftyLTPValue";
    public static final String COMMON_MESSAGE_TOPIC = "/commonMessage";
    public static final String PT_DELTA_OI_TOPIC ="/powerTradeDeltaOI";
    public static final String INDEX_OHLC_TOPIC ="/indexOHLC";
    public static final String INDEX_LTP_TOPIC ="/indexLTP";
    public static final String TRADE_DECISION_TOPIC ="/tradeDecision";
    public static final String REVERSAL_PATTERN_TOPIC ="/reversalPattern";
    public static final String IOB_SIGNAL_TOPIC = "/iobSignal";
    public static final String IOB_MITIGATION_TOPIC = "/iobMitigation";
    public static final String BRAHMASTRA_SIGNAL_TOPIC = "/brahmastraSignal";

    public static final Long NIFTY_INSTRUMENT_TOKEN = 256265L;
    public static final Long INDIA_VIX_INSTRUMENT_TOKEN = 264969L; // India VIX instrument token

    public static final String TRADE_DECISION_BUY = "BUY";
    public static final String TRADE_DECISION_SELL = "SELL";
    public static final String TRADE_DECISION_NO_TRADE = "NO TRADE";

    public static final String TRADE_MANAGEMENT_HOLD = "HOLD";
    public static final String TRADE_MANAGEMENT_CLOSE = "CLOSE TRADE";


    public static final String TRADER_TYPE_BUY = "B";
    public static final String TRADER_TYPE_BB = "BB";
    public static final String TRADER_TYPE_BBB = "BBB";
    public static final String TRADER_TYPE_SELL = "S";
    public static final String TRADER_TYPE_SS = "SS";
    public static final String TRADER_TYPE_SSS = "SSS";

    public static final String POWER_TRADE_DECISION_BUY = "POWER BUY";
    public static final String POWER_TRADE_DECISION_SELL = "POWER SELL";

    public static final String TOTAL = "Total";

    public static final String CALL = "CE";
    public static final String PUT = "PE";

    public static final String NFO_SEGMENT = "NFO";

    public static final String COLOR_GREEN = "GREEN";
    public static final String COLOR_RED = "RED";
    public static final String COLOR_BLACK = "BLACK";
    public static final String FONT_BOLD = "BOLD";

    public static final Double UPPER_TARGET_VALUE = 1.49;
    public static final Double LOWER_TARGET_VALUE = 0.49;
    public static final Double RATE_OI_TARGET_VALUE = 0.99;
    public static final Double MID_TARGET_VALUE = 1.25;

    public static final Double D_THREE = 3.0;
    public static final Double D_FOUR = 4.0;
    public static final Double D_FIVE = 5.0;
    public static final Double D_ZERO = 0.0;
    public static final Double D_ONE = 1.0;
    public static final Double D_TWO = 2.0;
    public static final Double D_ONE_HUNDRED = 100.0;
    public static final Double D_FIFTY = 50.0;
    public static final Double D_TWENTY_FIVE = 25.0;
    public static final Double D_ONE_THOUSAND = 1000.0;
    public static final Double D_TEN = 10.0;
    public static final Double D_TWENTY = 20.0;
    public static final Double D_TWO_HUNDRED = 200.0;

    public static final Integer I_ZERO = 0;
    public static final Integer I_ONE = 1;
    public static final Integer I_TWO = 2;
    public static final Integer I_THREE = 3;
    public static final Integer TWENTY_FOUR = 24;
    public static final Integer TWENTY_FIVE = 25;
    public static final Integer FIFTY = 50;
    public static final Integer ONE_HUNDRED = 100;
    public static final Integer FIVE_HUNDRED = 500;
    public static final Integer ONE_THOUSAND = 1000;
    public static final Integer TWO_THOUSAND = 2000;

    public static final Integer NIFTY_LOT_SIZE = 65;
    public static final String STATUS_RUNNING= "RUNNING";
    public static final String STATUS_COMPLETED= "COMPLETED";
    public static final String STATUS_FAILED= "FAILED";
    public static final String STATUS_PENDING= "PENDING";

    public static final String JOB_TYPE_NIFTY = "NIFTY";

    // ── Simulated Trading: signal sources ──────────────────────────────────
    public static final String SOURCE_IOB_SIGNAL       = "IOB_SIGNAL";
    public static final String SOURCE_EMA_CROSSOVER    = "EMA_CROSSOVER";
    public static final String SOURCE_LIQUIDITY_SWEEP  = "LIQUIDITY_SWEEP";
    public static final String SOURCE_TRADE_SETUP      = "TRADE_SETUP";

    // ── Simulated Trading: exit reasons ────────────────────────────────────
    public static final String EXIT_TARGET_HIT    = "TARGET_HIT";
    public static final String EXIT_STOPLOSS_HIT  = "STOPLOSS_HIT";
    public static final String EXIT_TRAILING_SL   = "TRAILING_SL";
    public static final String EXIT_INDEX_SL_HIT  = "INDEX_SL_HIT";
    public static final String EXIT_MARKET_CLOSE  = "MARKET_CLOSE";
    public static final String EXIT_REVERSE_SIGNAL = "REVERSE_SIGNAL";
    public static final String EXIT_MANUAL        = "MANUAL";

    // ── Simulated Trading: trade status ────────────────────────────────────
    public static final String TRADE_STATUS_OPEN      = "OPEN";
    public static final String TRADE_STATUS_CLOSED    = "CLOSED";
    public static final String TRADE_STATUS_DISCARDED = "DISCARDED";

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILURE = "FAILURE";
    public static final String ERROR = "ERROR";
    public static final String WARNING = "WARNING";
    public static final String STOPLOSS_HIT = "STOPLOSS HIT";
    public static final String TARGET_HIT = "TARGET HIT";
    public static final String TRADE_DECISION_TYPE_REGULAR = "REGULAR";
    public static final String TRADE_DECISION_TYPE_STOPLOSS = "STOPLOSS";
    public static final String TRADE_DECISION_TYPE_TRENDING = "TRENDING";

    public static final String COMMA = ",";
    public static final String DASH = "-";
    public static final String EMPTY = "";
    public static final String SPACE = " ";
    public static final String SLASH = "/";
    public static final String COLON = ":";
    public static final String NEW_LINE = "\n";
    public static final String ASTERISK = "*";
    public static final String UNDER_SCORE = "_";

    public static final Double PTC_1 = D_THREE;
    public static final Double PTC_2 = D_FIVE;
    public static final Double PTC_3 = D_TEN;

    public enum JobName {
        OISNAPSHOT,
        MARKET,
        PRE_MARKET,
        POST_MARKET
    }

    public static class EmailTemplates {
        public static final String HTML_TEMPLATE_START = "<html><body>";
        public static final String HTML_TEMPLATE_END = "</body></html>";
        public static final String TABLE_START = "<table style=\"border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;width:100%;max-width:600px;\">";
        public static final String TABLE_END = "</table>";
        public static final String TABLE_ROW_START = "<tr>";
        public static final String TABLE_ROW_END = "</tr>";
        public static final String TABLE_HEADER_START = "<th colspan=\"2\" style=\"text-align:left;padding:8px;background:#f2f2f2;border:1px solid #ddd;font-size:14px;\">";
        public static final String TABLE_HEADER_END = "</th>";
        public static final String TABLE_DATA_LABEL_START = "<td style=\"padding:8px;border:1px solid #ddd;background:#fff;font-weight:600;width:40%;\">";
        public static final String TABLE_DATA_VALUE_START = "<td style=\"padding:8px;border:1px solid #ddd;background:#fff;\">";
        public static final String TABLE_DATA_END = "</td>";


    }


}
