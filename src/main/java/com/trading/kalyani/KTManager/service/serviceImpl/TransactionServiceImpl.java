package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.OrderPlaceModel;
import com.trading.kalyani.KTManager.service.MessagingService;
import com.trading.kalyani.KTManager.service.TransactionService;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.OrderParams;
import com.zerodhatech.models.Position;
import com.zerodhatech.models.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;


@Service
public class TransactionServiceImpl implements TransactionService {

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    MessagingService messagingService;

    @Autowired
    KiteOrderService kiteOrderService;

    private static final Logger logger = LoggerFactory.getLogger(TransactionServiceImpl.class);

    @Override
    public Order placeOrder(OrderPlaceModel orderPlaceModel) {
        Order orderResponse;
        try {
            String variety = "regular";
            KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
            if(kiteSdk.getAccessToken()!=null) {
                logger.info("Order Place Model Trading Symbol: {} , Qty : {}", orderPlaceModel.getTradingSymbol(),orderPlaceModel.getQuantity());
                OrderParams params = mapOrderParams(orderPlaceModel);
                params.tag = "navisa07";
                logger.info("Order Place Params Trading Symbol: {} , Qty : {}", params.tradingsymbol,params.quantity);
                orderResponse = kiteSdk.placeOrder(params,variety);
                logger.info("Order Response: {}", orderResponse);
                CommonReqRes orderSuccessMessage = new CommonReqRes(true, "Order Placed for: "+ orderPlaceModel.getTradingSymbol(), orderPlaceModel.getQuantity(),null, SUCCESS);
                messagingService.sendCommonMessage(orderSuccessMessage);
                return orderResponse;
            }
        } catch (IOException | KiteException e) {
            logger.error("Exception while placing order {}",e.getMessage());
            CommonReqRes orderErrorMessage = new CommonReqRes(false, "Order Failed for: "+ orderPlaceModel.getTradingSymbol(), orderPlaceModel.getQuantity(),null,ERROR);
            messagingService.sendCommonMessage(orderErrorMessage);
        }
        return new Order();
    }

    @Override
    public Order placeOrderWebClient(OrderPlaceModel orderPlaceModel) {
        Order orderResponse;
        try {
            String variety = "regular";

            //get enctoken from db
            String enctoken = "enctoken 03tNiMsXeoyOWqaGKf+CK2Lkldlfk8Gz1Cr0b61MDK2eKcR9giAoAKiMo8mNpNg48xDLyF1sTJIiD+sUyqGvSDGYqOSpO3L7lzdpCk2I2gC2pkgh0astbQ==";
            String tag = "kiteUserId";

            if(!enctoken.isEmpty()) {
                logger.info("Order Place Model Trading Symbol: {} , Qty : {}", orderPlaceModel.getTradingSymbol(),orderPlaceModel.getQuantity());
                OrderParams params = mapOrderParams(orderPlaceModel);
                params.tag = tag;
                logger.info("Order Place Params Trading Symbol: {} , Qty : {}", params.tradingsymbol,params.quantity);
                orderResponse = placeOrderUsingEncToken(params,enctoken);
                logger.info("Order Response: {}", orderResponse);
                CommonReqRes orderSuccessMessage = new CommonReqRes(true, "Order Placed for: "+ orderPlaceModel.getTradingSymbol(), orderPlaceModel.getQuantity(),orderResponse, SUCCESS);
                messagingService.sendCommonMessage(orderSuccessMessage);
                return orderResponse;
            }
        } catch (Exception e) {
            logger.error("Exception while placing order: {}", e.getMessage(), e);
            CommonReqRes orderErrorMessage = new CommonReqRes(false, "Order Failed for: "+ orderPlaceModel.getTradingSymbol(), orderPlaceModel.getQuantity(),null,ERROR);
            messagingService.sendCommonMessage(orderErrorMessage);
        }
        return new Order();
    }

    private Order placeOrderUsingEncToken(OrderParams params, String enctoken) {

        Order response = kiteOrderService.placeOrderUsingWebClient(params, enctoken);
        logger.info("Order Response: {}", response);
        return response;
    }

    private OrderParams mapOrderParams(OrderPlaceModel orderPlaceModel)
    {
        OrderParams orderParams = new OrderParams();
        if (orderPlaceModel.getTradingSymbol() != null) {
            orderParams.tradingsymbol = orderPlaceModel.getTradingSymbol();
        }
        if (orderPlaceModel.getExchange() != null) {
            if(orderPlaceModel.getExchange().equals("NFO"))
                orderParams.exchange = Constants.EXCHANGE_NFO;
        }
        if (orderPlaceModel.getTransaction_type() != null) {
            if(orderPlaceModel.getTransaction_type().equals("BUY"))
                orderParams.transactionType = Constants.TRANSACTION_TYPE_BUY;
        }
        if (orderPlaceModel.getOrder_type() != null) {
            if(orderPlaceModel.getOrder_type().equals("MARKET"))
                orderParams.orderType = Constants.ORDER_TYPE_MARKET;
        }
        if (orderPlaceModel.getQuantity() != null) {
            orderParams.quantity = orderPlaceModel.getQuantity();
        }
        if (orderPlaceModel.getProduct() != null) {
            if(orderPlaceModel.getProduct().equals("NRML"))
                orderParams.product = Constants.PRODUCT_NRML;
        }
        if (orderPlaceModel.getPrice() != null) {
            orderParams.price = orderPlaceModel.getPrice();
        }
        if (orderPlaceModel.getTrigger_price() != null) {
            orderParams.triggerPrice = orderPlaceModel.getTrigger_price();
        }
        if (orderPlaceModel.getValidity() != null) {
            orderParams.validity = orderPlaceModel.getValidity();
        }
        return orderParams;
    }

    @Override
    public int closeOrders() {

        int closedOrdersNum = 0;

        KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
        try {

            //call cancel open orders
            Boolean areAllOpenOrdersClosed = cancelAllOpenOrders();

            Map<String, List<Position>> portfolioPositions = kiteSdk.getPositions();

            List<Position> currentPositions = portfolioPositions.get(Constants.POSITION_DAY);

            if (!currentPositions.isEmpty() && areAllOpenOrdersClosed) {
                for (Position position : currentPositions) {
                    logger.info("Position: {} , Net Quantity : {}", position.tradingSymbol, position.netQuantity);
                    if (position.netQuantity > 0) {
                        OrderParams orderParams = new OrderParams();
                        orderParams.tradingsymbol = position.tradingSymbol;
                        orderParams.exchange = position.exchange;
                        orderParams.transactionType = Constants.TRANSACTION_TYPE_SELL;
                        orderParams.orderType = Constants.ORDER_TYPE_MARKET;
                        orderParams.product = Constants.PRODUCT_NRML;
                        orderParams.quantity = position.netQuantity;

                        Order orderResponse = kiteSdk.placeOrder(orderParams, "regular");

                        if(!Objects.isNull(orderResponse.orderId))
                            closedOrdersNum++;

                        logger.info("Order Response after closing position: {}", orderResponse);
                    }
                }

                logger.info("Closed {} orders", closedOrdersNum);
            }


        } catch (KiteException | IOException e) {
            logger.error("Exception while closing orders {}",e.getMessage());
        }
        return closedOrdersNum;
    }

    @Override
    public Double getAvailableMargins() {

        try {
            KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
            if (kiteSdk.getAccessToken() != null) {
                return  Double.valueOf(kiteSdk.getMargins().get("equity").available.liveBalance);
              }
        } catch (KiteException | JSONException | IOException e) {
            throw new RuntimeException(e);
        } catch (NullPointerException e) {
            logger.error("Authentication required to get details.");
        }

        return 0.0;
    }

    @Override
    public Double getTotalPositionPNL() {
        try {
            KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
            if (kiteSdk.getAccessToken() != null) {
                Map<String, List<Position>> portfolioPositions = kiteSdk.getPositions();
                List<Position> dayPositions = portfolioPositions.get(Constants.POSITION_DAY);
                Double totalPNL =0.0;
                if (dayPositions != null && !dayPositions.isEmpty()) {
                    for (Position position : dayPositions) {
                        logger.info("Position: {} , Net Quantity : {} , PNL: {}", position.tradingSymbol, position.netQuantity,position.pnl);

                            totalPNL+= position.pnl;

                    }
                } else {
                    logger.info("No positions available");
                }

                logger.info("Total PNL: {}", totalPNL);

                return totalPNL;
            }
        } catch (KiteException | JSONException | IOException e) {
            throw new RuntimeException(e);
        } catch (NullPointerException e) {
            logger.error("Authentication required to get details.");
        }
        return null;
    }

    @Override
    public Boolean cancelAllOpenOrders() {

        KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();

        try {
            List<Order> currentOrders = kiteSdk.getOrders();
            currentOrders.stream().filter(co -> (co.status.equalsIgnoreCase(Constants.ORDER_OPEN) && co.exchange.equalsIgnoreCase(Constants.EXCHANGE_NFO) && co.transactionType.equalsIgnoreCase(Constants.TRANSACTION_TYPE_SELL))).forEach(co -> {
                try {
                    kiteSdk.cancelOrder(co.orderId, Constants.VARIETY_REGULAR);
                } catch (KiteException | IOException e) {
                    logger.error("Error cancelling order {}: {}", co.orderId, e.getMessage(), e);
                }
            });
            logger.info("All Open Orders are cancelled successfully");
            return true;
        }

        catch (RuntimeException ex) {
            logger.error("Cancel Order execution failed: {}", ex.getMessage(), ex);

        } catch (IOException | KiteException e) {
            logger.error("Cancel Order execution failed: {}", e.getMessage(), e);
        }

        return false;

    }

    @Override
    public Map<String, Quote> getMarketQuote(ArrayList<Long> instrumentTokens) {

        logger.info("Fetching market quotes for instrument tokens: {}", instrumentTokens);

        Map<String, Quote> quotes = null;

        try {

            if(!instrumentTokens.isEmpty()) {

                String[] instruments = instrumentTokens.stream()
                        .map(String::valueOf)
                        .toArray(String[]::new);

                quotes = kiteConnectConfig.kiteConnect().getQuote(instruments);

                quotes.forEach((key, value) -> {
                    logger.info("Instrument: {}, LTP: {}, TS: {}", key, value.lastPrice,value.timestamp);
                });
            }

        } catch (KiteException | IOException e) {
            logger.error("Error fetching market quotes: {}", e.getMessage());
        }

        return quotes;
    }

}
