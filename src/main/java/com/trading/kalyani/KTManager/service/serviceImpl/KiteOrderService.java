package com.trading.kalyani.KTManager.service.serviceImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.OrderParams;

@Service
public class KiteOrderService {

    private static final Logger logger = LoggerFactory.getLogger(KiteOrderService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final WebClient webClient;

    public KiteOrderService() {
        this.webClient = WebClient.builder()
                .baseUrl("https://kite.zerodha.com/oms")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .build();
    }

    public Order placeOrderUsingWebClient(OrderParams orderParams, String enctoken) {
        try {
            // Kite returns {"status":"success","data":{"order_id":"..."}}
            // Deserialize to JsonNode first, then extract the "data" node
            JsonNode root = webClient.post()
                    .uri("/orders/regular")
                    .header("X-Kite-Version", "3")
                    .header("Authorization", enctoken)
                    .bodyValue(buildRequestBody(orderParams))
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();

            if (root == null || !root.path("status").asText().equals("success")) {
                String errMsg = root != null ? root.path("message").asText("unknown error") : "null response";
                logger.error("Kite order placement failed: {}", errMsg);
                throw new RuntimeException("Order placement failed: " + errMsg);
            }

            Order order = objectMapper.treeToValue(root.path("data"), Order.class);
            logger.info("Order placed successfully: orderId={}", order.orderId);
            return order;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to place order: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to place order: " + e.getMessage());
        }
    }

    private String buildRequestBody(OrderParams orderParams) {
        StringBuilder requestBody = new StringBuilder();
        requestBody.append("tradingsymbol=").append(orderParams.tradingsymbol)
                .append("&exchange=").append(orderParams.exchange)
                .append("&transaction_type=").append(orderParams.transactionType)
                .append("&order_type=").append(orderParams.orderType)
                .append("&quantity=").append(orderParams.quantity)
                .append("&product=").append(orderParams.product)
                .append("&validity=").append(orderParams.validity);

        if (orderParams.price != null) {
            requestBody.append("&price=").append(orderParams.price);
        }
        if (orderParams.triggerPrice != null) {
            requestBody.append("&trigger_price=").append(orderParams.triggerPrice);
        }
        if (orderParams.tag != null) {
            requestBody.append("&tag=").append(orderParams.tag);
        }

        return requestBody.toString();
    }

    public Object getEquityFundBalance(String enctoken) {

        return webClient.get()
                .uri("/user/margins")
                .header("X-Kite-Version", "3")
                .header("Authorization", enctoken)
                .retrieve()
                .bodyToMono(Object.class)
                .block();

    }
}