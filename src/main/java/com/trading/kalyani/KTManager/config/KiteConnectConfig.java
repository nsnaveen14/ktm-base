package com.trading.kalyani.KTManager.config;

import com.zerodhatech.kiteconnect.KiteConnect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class KiteConnectConfig {

    @Value("${kite.apiKey}")
    private String apiKey;

    @Value("${kite.webSocketUrl}")
    private String webSocketUrl;

    @Value("${kiteUserName}")
    private String kiteUserName;


    private final Environment environment;

    public KiteConnectConfig(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public KiteConnect kiteConnect() {
        KiteConnect kiteSdk = new KiteConnect(apiKey);
        kiteSdk.setUserId(kiteUserName);

        return kiteSdk;
    }

    @Bean
    public String getActiveProfile() {
        String[] activeProfiles = environment.getActiveProfiles();
        return activeProfiles.length > 0 ? activeProfiles[0] : "default";
    }




}
