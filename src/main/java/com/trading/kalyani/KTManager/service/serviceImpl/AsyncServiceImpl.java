package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.EmailConfig;
import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.OrderBook;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.OrderPlaceModel;
import com.trading.kalyani.KTManager.repository.OrderBookRepository;
import com.trading.kalyani.KTManager.service.*;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.zerodhatech.kiteconnect.utils.Constants.*;


@Service
public class AsyncServiceImpl implements AsyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncServiceImpl.class);

    @Autowired
    UserService userService;

    @Autowired
    InstrumentService instrumentService;

    @Autowired
    EmailService emailService;

    @Autowired
    EmailConfig emailConfig;

    @Autowired
    MessagingService messagingService;

    @Autowired
    TransactionService transactionService;

    @Value("${kiteUserName}")
    String userName;

    @Autowired
    OrderBookRepository orderBookRepository;


    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> saveUserNInstrumentDataAsync(User userModel, List<Instrument> instruments) {
        try {
            userService.saveUserModelData(userModel);
            logger.info("User data saved successfully");
            instrumentService.clearAllCaches();
            messagingService.sendCommonMessage(new CommonReqRes(true, "Instrument data saving is in progress", 0, null, WARNING));
            instrumentService.saveInstrumentsData(instruments);
            logger.info("Instruments data saved successfully");
        } catch (Exception e) {
            throw new RuntimeException("Error saving data asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }
    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> sendEmailToAllUsersAsync(String subject, String body, String fileName) {
        try {
            int sentEmailTo = emailService.sendEmailToAllUsers(subject, body, fileName);
            if (sentEmailTo == emailConfig.getUserListForEmail().size())
                logger.info("Email sent to all users successfully");
            else
                logger.info("Email was not sent to all users");
        } catch (Exception e) {
            throw new RuntimeException("Error sending email asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> placeOrderAsync(IndexLTP indexLTP, AutoTradeParams autoTradeParams) {
        OrderBook orderBook = new OrderBook();
        try {
            OrderPlaceModel orderPlaceModel = getOrderPlaceModel(indexLTP, autoTradeParams);
            Order orderResponse = transactionService.placeOrder(orderPlaceModel);

            logger.info("Order response: {}", orderResponse);

            orderBook.setOrderId(orderResponse.orderId);
            orderBook.setUserId(userName);
            orderBook.setOrderTargetPrice(Integer.valueOf(indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) ? indexLTP.getResistance() : indexLTP.getSupport()));
            orderBook.setJobIterationDetails(indexLTP.getJobIterationDetails());
            orderBook.setOrderStopLossPrice(indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) ? indexLTP.getIndexLTP() - TWENTY_FIVE : indexLTP.getIndexLTP() + TWENTY_FIVE);

            orderBook = orderBookRepository.save(orderBook);
            logger.info("Order placed successfully: {}", orderBook);

            Optional<String> emailSendTO = emailConfig.getUserListForEmail().stream().filter(u -> u.contains(userName)).findFirst();
            if (emailSendTO.isPresent()) {
                emailService.sendEmail(emailSendTO.get(), "KTM: Order Placed Successfully", orderBook.toString());
                logger.info("Email notification sent to: {} for OrderId: {}", emailSendTO.get(), orderBook.getOrderId());
            } else {
                logger.warn("No matching email found for user: {}", userName);
            }

            CommonReqRes orderMessage = new CommonReqRes(true, "Order placed successfully with Order ID: " + orderBook.getOrderId(), orderPlaceModel.getQuantity(), orderBook, SUCCESS);
            messagingService.sendCommonMessage(orderMessage);

        } catch (Exception e) {
            logger.error("Error placing order asynchronously: {}", e.getMessage());
            throw new RuntimeException("Error placing order asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> closeOrdersAsync(IndexLTP indexLTP) {
        try {
            int closedOrdersNum = transactionService.closeOrders();
            if (closedOrdersNum > 0) {
                logger.info("Closed {} orders successfully", closedOrdersNum);
                messagingService.sendCommonMessage(new CommonReqRes(true, "Closed " + closedOrdersNum + " orders successfully", closedOrdersNum, null, SUCCESS));
            } else {
                logger.info("No open orders to close");
                messagingService.sendCommonMessage(new CommonReqRes(false, "No open orders to close", 0, null, FAILURE));
            }
        } catch (Exception e) {
            logger.error("Error closing orders asynchronously: {}", e.getMessage());
            throw new RuntimeException("Error closing orders asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }


    @NotNull
    private static OrderPlaceModel getOrderPlaceModel(IndexLTP indexLTP, AutoTradeParams autoTradeParams) {
        OrderPlaceModel orderPlaceModel = new OrderPlaceModel();

        if(indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY)) {
            orderPlaceModel.setTradingSymbol(autoTradeParams.getAutoTradeCallSymbol());
            orderPlaceModel.setQuantity(autoTradeParams.getAutoTradeCallLotSize()*NIFTY_LOT_SIZE);
        }
        else if(indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL)) {
            orderPlaceModel.setTradingSymbol(autoTradeParams.getAutoTradePutSymbol());
            orderPlaceModel.setQuantity(autoTradeParams.getAutoTradePutLotSize()*NIFTY_LOT_SIZE);
        }

        orderPlaceModel.setExchange(NFO_SEGMENT);
        orderPlaceModel.setTransaction_type(TRADE_DECISION_BUY);

        orderPlaceModel.setOrder_type(ORDER_TYPE_MARKET);
        orderPlaceModel.setProduct(PRODUCT_NRML);
        orderPlaceModel.setValidity(VALIDITY_DAY);

        return orderPlaceModel;
    }




}