package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.InstrumentEntity;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.trading.kalyani.KTManager.repository.AutoTradeRepository;
import com.trading.kalyani.KTManager.repository.InstrumentRepository;
import com.trading.kalyani.KTManager.repository.NiftyLTPRepository;
import com.trading.kalyani.KTManager.service.AutoTradeService;
import com.trading.kalyani.KTManager.service.TransactionService;
import com.trading.kalyani.KTManager.utilities.DateUtilities;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

@Service
public class AutoTradeServiceImpl implements AutoTradeService {

    @Autowired
    NiftyLTPRepository niftyLTPRepository;

    @Autowired
    InstrumentRepository instrumentRepository;

    @Autowired
    TransactionService transactionService;

    @Autowired
    AutoTradeRepository autoTradeRepository;

    private static final Logger logger = LoggerFactory.getLogger(AutoTradeServiceImpl.class);

    @Override
    public void calculateAutoTradeSymbols(AutoTradeParams autoTradeParams,IndexLTP latestIndexLTP) {


        String strikePrice ="";
        Integer price = Integer.valueOf(latestIndexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY)?latestIndexLTP.getResistance():latestIndexLTP.getSupport());

        if(((price/TWENTY_FIVE) % I_TWO) != 0)
           strikePrice = String.valueOf(price + TWENTY_FIVE);
        else
           strikePrice = String.valueOf(price);

        logger.info("Latest Nifty LTP: {}, Strike Price: {}", latestIndexLTP.getIndexLTP(), strikePrice);

        calculateTradeSymbolFromStrikePrice(autoTradeParams,strikePrice);


    }

    private void calculateTradeSymbolFromStrikePrice(AutoTradeParams autoTradeParams, String strikePrice) {

        List<InstrumentEntity> instrumentSPList = instrumentRepository.findInstrumentFromStrikePrice(strikePrice);

        boolean isWedOrThu = DateUtilities.isWednesdayOrThursday(LocalDate.now());

        if (instrumentSPList.size() < 4) {
            logger.error("Insufficient instruments found for strike price: {}. Expected 4, got {}", strikePrice, instrumentSPList.size());
            throw new RuntimeException("Insufficient instruments found for strike price: " + strikePrice);
        }

        List<InstrumentEntity> instrumentSubList = !isWedOrThu ? instrumentSPList.subList(0, 2) : instrumentSPList.subList(2, 4);

        Instrument firstInstrument = instrumentSubList.getFirst().getInstrument();
        Instrument secondInstrument = instrumentSubList.getLast().getInstrument();

        logger.info("First Instrument: {}, Second Instrument: {}", firstInstrument.getTradingsymbol(), secondInstrument.getTradingsymbol());

            if(firstInstrument.getInstrument_type().equalsIgnoreCase(CALL)) {
                autoTradeParams.setAutoTradeCallSymbol(firstInstrument.getTradingsymbol());
                autoTradeParams.setAutoTradeCallInstrumentToken(firstInstrument.getInstrument_token());
                autoTradeParams.setAutoTradePutSymbol(secondInstrument.getTradingsymbol());
                autoTradeParams.setAutoTradePutInstrumentToken(secondInstrument.getInstrument_token());
            }
            else {
                autoTradeParams.setAutoTradeCallSymbol(secondInstrument.getTradingsymbol());
                autoTradeParams.setAutoTradeCallInstrumentToken(secondInstrument.getInstrument_token());
                autoTradeParams.setAutoTradePutSymbol(firstInstrument.getTradingsymbol());
                autoTradeParams.setAutoTradePutInstrumentToken(firstInstrument.getInstrument_token());
            }


        logger.info("Auto Trade Call Symbol: {}, Auto Trade Put Symbol: {}", autoTradeParams.getAutoTradeCallSymbol(), autoTradeParams.getAutoTradePutSymbol());
     }

    @Override
    public void calculateAutoTradeLotSizes(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP) {

        Double availableFunds = transactionService.getAvailableMargins();
        logger.info("Available Funds for Auto Trade: {}", availableFunds);
        double fundsToBeUsed = availableFunds/ D_FOUR; // Using 25% of available funds for auto trade
        logger.info("Funds to be used for Auto Trade: {}", fundsToBeUsed);

        ArrayList<Long> instrumentTokens = new ArrayList<>();
        instrumentTokens.add(autoTradeParams.getAutoTradeCallInstrumentToken());
        instrumentTokens.add(autoTradeParams.getAutoTradePutInstrumentToken());

        Map<String, Quote> marketQuotes = transactionService.getMarketQuote(instrumentTokens);

        Double callLTP, putLTP;

        Quote callQuote = marketQuotes.get(String.valueOf(autoTradeParams.getAutoTradeCallInstrumentToken()));
        Quote putQuote = marketQuotes.get(String.valueOf(autoTradeParams.getAutoTradePutInstrumentToken()));
        if (callQuote == null || putQuote == null) {
            logger.error("Market quote not found for tokens: call={}, put={}", autoTradeParams.getAutoTradeCallInstrumentToken(), autoTradeParams.getAutoTradePutInstrumentToken());
            throw new RuntimeException("Market quote not found for one or both instrument tokens");
        }
        callLTP = callQuote.lastPrice;
        putLTP = putQuote.lastPrice;
        logger.info("Call LTP: {}, Put LTP: {}", callLTP, putLTP);

        autoTradeParams.setAutoTradeCallLotSize((int) Math.floor(fundsToBeUsed / (callLTP*NIFTY_LOT_SIZE)));
        autoTradeParams.setAutoTradePutLotSize((int) Math.floor(fundsToBeUsed / (putLTP*NIFTY_LOT_SIZE)));
        logger.info("Auto Trade Call Lot Size: {}, Auto Trade Put Lot Size: {}", autoTradeParams.getAutoTradeCallLotSize(), autoTradeParams.getAutoTradePutLotSize());
        autoTradeParams.setAutoTradeParamsTS(LocalDateTime.now());

    }

    @Override
    public void calculateAutoTradeEntryPrice(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP) {

        logger.info("Calculating Auto Trade Entry Price for latest Nifty LTP: {}", latestIndexLTP.getTradeDecision());
        if(latestIndexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY))
        {
            autoTradeParams.setAutoTradeCallEntryPrice(Integer.parseInt(latestIndexLTP.getSupport()) - 10);
            logger.info("Auto Trade Call Entry Price set to: {}", autoTradeParams.getAutoTradeCallEntryPrice());
        }

        else if(latestIndexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL))
        {
            autoTradeParams.setAutoTradePutEntryPrice(Integer.parseInt(latestIndexLTP.getResistance()) + 10);
            logger.info("Auto Trade Put Entry Price set to: {}", autoTradeParams.getAutoTradePutEntryPrice());
        }
        else {
            logger.warn("No valid trade decision found for Auto Trade Entry Price calculation.");
        }


    }

    @Override
    public void calculateAutoTradeSLPrice(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP) {

        logger.info("Calculating Auto Trade SL Price for latest Nifty LTP: {}", latestIndexLTP.getTradeDecision());
        if(latestIndexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY))
        {
            autoTradeParams.setAutoTradeCallSLPrice(Integer.parseInt(latestIndexLTP.getSupport()) - 40);
            logger.info("Auto Trade Call SL Price set to: {}", autoTradeParams.getAutoTradeCallSLPrice());
        }

        else if(latestIndexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL))
        {
            autoTradeParams.setAutoTradePutSLPrice(Integer.parseInt(latestIndexLTP.getResistance()) + 40);
            logger.info("Auto Trade Put SL Price set to: {}", autoTradeParams.getAutoTradePutSLPrice());
        }
        else {
            logger.warn("No valid trade decision found for Auto Trade SL Price calculation.");
        }

    }

    @Override
    public AutoTradeParams saveAutoTradeParams(AutoTradeParams autoTradeParams) {
        logger.info("Saving Auto Trade Parameters: {}", autoTradeParams.toString());
        return autoTradeRepository.save(autoTradeParams);
    }
}
