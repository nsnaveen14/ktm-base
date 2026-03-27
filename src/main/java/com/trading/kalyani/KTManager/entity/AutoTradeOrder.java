package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing an auto trading order.
 */
@Entity
@Table(name = "auto_trade_orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AutoTradeOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", unique = true)
    private String orderId;

    @Column(name = "iob_id")
    private Long iobId;

    @Column(name = "trade_result_id")
    private Long tradeResultId;

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "trading_symbol")
    private String tradingSymbol;

    @Column(name = "exchange")
    private String exchange; // NFO, NSE, BSE

    // Order Details
    @Column(name = "order_type")
    private String orderType; // MARKET, LIMIT, SL, SL-M

    @Column(name = "transaction_type")
    private String transactionType; // BUY, SELL

    @Column(name = "product_type")
    private String productType; // MIS, NRML, CNC

    @Column(name = "quantity")
    private Integer quantity;

    @Column(name = "price")
    private Double price;

    @Column(name = "trigger_price")
    private Double triggerPrice;

    @Column(name = "disclosed_quantity")
    private Integer disclosedQuantity;

    // Fill Details
    @Column(name = "filled_quantity")
    private Integer filledQuantity;

    @Column(name = "pending_quantity")
    private Integer pendingQuantity;

    @Column(name = "average_price")
    private Double averagePrice;

    // Status
    @Column(name = "status")
    private String status; // PENDING, PLACED, OPEN, COMPLETE, CANCELLED, REJECTED

    @Column(name = "status_message")
    private String statusMessage;

    @Column(name = "kite_order_id")
    private String kiteOrderId; // Order ID from Kite

    // Timestamps
    @Column(name = "order_time")
    private LocalDateTime orderTime;

    @Column(name = "exchange_time")
    private LocalDateTime exchangeTime;

    @Column(name = "fill_time")
    private LocalDateTime fillTime;

    // Purpose
    @Column(name = "order_purpose")
    private String orderPurpose; // ENTRY, EXIT, STOP_LOSS, TAKE_PROFIT, TRAILING_SL

    @Column(name = "parent_order_id")
    private String parentOrderId;

    // Error handling
    @Column(name = "error_code")
    private String errorCode;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "retry_count")
    private Integer retryCount;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (orderId == null) {
            orderId = "ATO_" + System.currentTimeMillis();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
