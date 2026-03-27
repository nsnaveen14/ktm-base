package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity for auto trading configuration.
 */
@Entity
@Table(name = "auto_trading_config")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AutoTradingConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "config_name", unique = true)
    private String configName;

    // Global Settings
    @Column(name = "auto_trading_enabled")
    private Boolean autoTradingEnabled;

    @Column(name = "paper_trading_mode")
    private Boolean paperTradingMode; // Simulate without real orders

    // Entry Settings
    @Column(name = "entry_type")
    private String entryType; // ZONE_TOUCH, ZONE_MIDPOINT, CONFIRMATION_CANDLE

    @Column(name = "min_confidence")
    private Double minConfidence; // Minimum confidence for auto trade

    @Column(name = "require_fvg")
    private Boolean requireFvg; // Require FVG confluence

    @Column(name = "require_trend_alignment")
    private Boolean requireTrendAlignment; // Require trend alignment

    @Column(name = "require_institutional_volume")
    private Boolean requireInstitutionalVolume;

    // Risk Settings
    @Column(name = "max_position_size")
    private Integer maxPositionSize; // Max quantity per trade

    @Column(name = "max_lots_per_trade")
    private Integer maxLotsPerTrade;

    @Column(name = "max_open_positions")
    private Integer maxOpenPositions;

    @Column(name = "max_daily_trades")
    private Integer maxDailyTrades;

    @Column(name = "max_daily_loss")
    private Double maxDailyLoss;

    @Column(name = "use_dynamic_sl")
    private Boolean useDynamicSL; // Use ATR-based stop loss

    @Column(name = "sl_atr_multiplier")
    private Double slAtrMultiplier;

    // Position Management
    @Column(name = "enable_trailing_sl")
    private Boolean enableTrailingSL;

    @Column(name = "trailing_sl_trigger")
    private String trailingSLTrigger; // TARGET_1, BREAKEVEN, POINTS

    @Column(name = "trailing_sl_distance_points")
    private Double trailingSLDistancePoints;

    @Column(name = "book_partial_profits")
    private Boolean bookPartialProfits;

    @Column(name = "partial_profit_percent")
    private Integer partialProfitPercent; // e.g., 50%

    @Column(name = "partial_profit_at")
    private String partialProfitAt; // TARGET_1, TARGET_2

    // Exit Settings
    @Column(name = "default_exit_target")
    private String defaultExitTarget; // TARGET_1, TARGET_2, TARGET_3

    @Column(name = "exit_at_market_close")
    private Boolean exitAtMarketClose;

    @Column(name = "market_close_time")
    private String marketCloseTime; // "15:20" for 3:20 PM

    // Trading Hours
    @Column(name = "trade_start_time")
    private String tradeStartTime; // "09:20"

    @Column(name = "trade_end_time")
    private String tradeEndTime; // "15:00"

    @Column(name = "avoid_first_candle")
    private Boolean avoidFirstCandle; // Don't trade in first 5 min

    // Instruments
    @Column(name = "enabled_instruments")
    private String enabledInstruments; // Comma-separated tokens

    // Order Settings
    @Column(name = "default_product_type")
    private String defaultProductType; // MIS, NRML

    @Column(name = "use_bracket_orders")
    private Boolean useBracketOrders;

    @Column(name = "use_cover_orders")
    private Boolean useCoverOrders;

    // Notification Settings
    @Column(name = "notify_on_entry")
    private Boolean notifyOnEntry;

    @Column(name = "notify_on_exit")
    private Boolean notifyOnExit;

    @Column(name = "notify_on_sl_hit")
    private Boolean notifyOnSLHit;

    @Column(name = "notify_on_target_hit")
    private Boolean notifyOnTargetHit;

    // Timestamps
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
