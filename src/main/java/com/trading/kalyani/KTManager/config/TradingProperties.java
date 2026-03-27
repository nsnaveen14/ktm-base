package com.trading.kalyani.KTManager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "ktm.trading.trailing")
public class TradingProperties {

    /** Percent of distance to target required to activate trailing SL (e.g., 50 for 50%) */
    private double activationThresholdPercent = 50.0;

    /** Percent of profit to use when calculating trailing SL (e.g., 50 for 50%) */
    private double trailPercentOfProfit = 50.0;

    public double getActivationThresholdPercent() {
        return activationThresholdPercent;
    }

    public void setActivationThresholdPercent(double activationThresholdPercent) {
        this.activationThresholdPercent = activationThresholdPercent;
    }

    public double getTrailPercentOfProfit() {
        return trailPercentOfProfit;
    }

    public void setTrailPercentOfProfit(double trailPercentOfProfit) {
        this.trailPercentOfProfit = trailPercentOfProfit;
    }
}

