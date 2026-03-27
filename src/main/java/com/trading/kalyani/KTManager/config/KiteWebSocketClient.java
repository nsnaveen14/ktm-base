package com.trading.kalyani.KTManager.config;


import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class KiteWebSocketClient extends WebSocketClient {

    private static final Logger logger = LoggerFactory.getLogger(KiteWebSocketClient.class);

    public KiteWebSocketClient(URI serverUri) {
        super(serverUri);
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        logger.info("Connected to WebSocket server");
    }

    @Override
    public void onMessage(String message) {
        logger.debug("Received message: {}", message);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        logger.info("Disconnected from WebSocket server");
    }

    @Override
    public void onError(Exception ex) {
        logger.error("WebSocket error: {}", ex.getMessage(), ex);
    }

    public static void connectToWS(String wsUri) {
        try {
            URI uri = new URI(wsUri);
            KiteWebSocketClient client = new KiteWebSocketClient(uri);
            client.connect();
        } catch (URISyntaxException e) {
            logger.error("Invalid WebSocket URI {}: {}", wsUri, e.getMessage(), e);
        }
    }
}
