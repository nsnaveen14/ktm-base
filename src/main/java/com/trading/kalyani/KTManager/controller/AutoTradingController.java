package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.service.AutoTradingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST Controller for Auto Trading functionality.
 */
@RestController
@RequestMapping("/api/auto-trade")
@CrossOrigin(origins = "*")
public class AutoTradingController {

    private static final Logger logger = LoggerFactory.getLogger(AutoTradingController.class);

    @Autowired
    private AutoTradingService autoTradingService;

    // ==================== Configuration ====================

    /**
     * Get auto trading configuration
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfig() {
        try {
            Map<String, Object> config = autoTradingService.getAutoTradingConfig();
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            logger.error("Error getting config: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Update auto trading configuration
     */
    @PutMapping("/config")
    public ResponseEntity<Map<String, Object>> updateConfig(@RequestBody Map<String, Object> config) {
        try {
            autoTradingService.updateAutoTradingConfig(config);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Configuration updated",
                    "config", autoTradingService.getAutoTradingConfig()
            ));
        } catch (Exception e) {
            logger.error("Error updating config: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Enable auto trading
     */
    @PostMapping("/enable")
    public ResponseEntity<Map<String, Object>> enableAutoTrading() {
        try {
            autoTradingService.setAutoTradingEnabled(true);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Auto trading enabled",
                    "isEnabled", true
            ));
        } catch (Exception e) {
            logger.error("Error enabling auto trading: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Disable auto trading
     */
    @PostMapping("/disable")
    public ResponseEntity<Map<String, Object>> disableAutoTrading() {
        try {
            autoTradingService.setAutoTradingEnabled(false);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Auto trading disabled",
                    "isEnabled", false
            ));
        } catch (Exception e) {
            logger.error("Error disabling auto trading: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get auto trading status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        try {
            Map<String, Object> stats = autoTradingService.getAutoTradingStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting status: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Entry Conditions ====================

    /**
     * Check entry conditions for an IOB
     */
    @GetMapping("/check-entry/{iobId}")
    public ResponseEntity<Map<String, Object>> checkEntryConditions(
            @PathVariable Long iobId,
            @RequestParam Double currentPrice) {
        try {
            Map<String, Object> conditions = autoTradingService.checkEntryConditions(iobId, currentPrice);
            return ResponseEntity.ok(conditions);
        } catch (Exception e) {
            logger.error("Error checking entry conditions: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get IOBs ready for entry
     */
    @GetMapping("/ready-for-entry/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getReadyIOBs(
            @PathVariable Long instrumentToken,
            @RequestParam Double currentPrice) {
        try {
            var iobs = autoTradingService.getIOBsReadyForEntry(instrumentToken, currentPrice);
            return ResponseEntity.ok(Map.of(
                    "instrumentToken", instrumentToken,
                    "count", iobs.size(),
                    "iobs", iobs
            ));
        } catch (Exception e) {
            logger.error("Error getting ready IOBs: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Order Management ====================

    /**
     * Place an order for an IOB
     */
    @PostMapping("/place-order/{iobId}")
    public ResponseEntity<Map<String, Object>> placeOrder(
            @PathVariable Long iobId,
            @RequestParam(required = false) Double entryPrice,
            @RequestParam(required = false) Integer quantity) {
        try {
            Map<String, Object> result = autoTradingService.placeIOBOrder(iobId, entryPrice, quantity);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error placing order: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Place a market order
     */
    @PostMapping("/market-order/{iobId}")
    public ResponseEntity<Map<String, Object>> placeMarketOrder(
            @PathVariable Long iobId,
            @RequestParam(required = false) Integer quantity) {
        try {
            Map<String, Object> result = autoTradingService.placeMarketOrder(iobId, quantity);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error placing market order: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Cancel an order
     */
    @PostMapping("/cancel-order/{orderId}")
    public ResponseEntity<Map<String, Object>> cancelOrder(@PathVariable String orderId) {
        try {
            Map<String, Object> result = autoTradingService.cancelOrder(orderId);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error cancelling order: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get order status
     */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<Map<String, Object>> getOrderStatus(@PathVariable String orderId) {
        try {
            Map<String, Object> status = autoTradingService.getOrderStatus(orderId);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            logger.error("Error getting order status: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get pending orders
     */
    @GetMapping("/pending-orders")
    public ResponseEntity<List<Map<String, Object>>> getPendingOrders() {
        try {
            List<Map<String, Object>> orders = autoTradingService.getPendingOrders();
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            logger.error("Error getting pending orders: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    // ==================== Position Management ====================

    /**
     * Get open positions
     */
    @GetMapping("/positions")
    public ResponseEntity<List<Map<String, Object>>> getOpenPositions() {
        try {
            List<Map<String, Object>> positions = autoTradingService.getOpenPositions();
            return ResponseEntity.ok(positions);
        } catch (Exception e) {
            logger.error("Error getting positions: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Update trailing stop
     */
    @PostMapping("/positions/{positionId}/trailing-stop")
    public ResponseEntity<Map<String, Object>> updateTrailingStop(
            @PathVariable String positionId,
            @RequestParam Double newStopLoss) {
        try {
            Map<String, Object> result = autoTradingService.updateTrailingStop(positionId, newStopLoss);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error updating trailing stop: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Book partial profits
     */
    @PostMapping("/positions/{positionId}/partial-exit")
    public ResponseEntity<Map<String, Object>> bookPartialProfits(
            @PathVariable String positionId,
            @RequestParam int percentage,
            @RequestParam Double price) {
        try {
            Map<String, Object> result = autoTradingService.bookPartialProfits(positionId, percentage, price);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error booking partial profits: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Close a position
     */
    @PostMapping("/positions/{positionId}/close")
    public ResponseEntity<Map<String, Object>> closePosition(
            @PathVariable String positionId,
            @RequestParam Double exitPrice,
            @RequestParam(defaultValue = "MANUAL") String reason) {
        try {
            Map<String, Object> result = autoTradingService.closePosition(positionId, exitPrice, reason);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error closing position: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Close all positions
     */
    @PostMapping("/positions/close-all")
    public ResponseEntity<Map<String, Object>> closeAllPositions(
            @RequestParam(required = false) Long instrumentToken) {
        try {
            Map<String, Object> result = autoTradingService.closeAllPositions(instrumentToken);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error closing all positions: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Statistics & Logs ====================

    /**
     * Get today's auto trades
     */
    @GetMapping("/today-trades")
    public ResponseEntity<List<Map<String, Object>>> getTodaysTrades() {
        try {
            List<Map<String, Object>> trades = autoTradingService.getTodaysAutoTrades();
            return ResponseEntity.ok(trades);
        } catch (Exception e) {
            logger.error("Error getting today's trades: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get activity log
     */
    @GetMapping("/activity-log")
    public ResponseEntity<List<Map<String, Object>>> getActivityLog(
            @RequestParam(defaultValue = "50") int limit) {
        try {
            List<Map<String, Object>> log = autoTradingService.getActivityLog(limit);
            return ResponseEntity.ok(log);
        } catch (Exception e) {
            logger.error("Error getting activity log: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
}
