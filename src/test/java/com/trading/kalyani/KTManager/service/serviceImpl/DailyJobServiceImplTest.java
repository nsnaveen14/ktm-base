package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.utilities.KiteTickerProvider;
import com.trading.kalyani.KTManager.entity.CandleStick;
import com.trading.kalyani.KTManager.model.SwingHighLow; import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks; import org.mockito.Mock; import org.mockito.junit.jupiter.MockitoExtension;
import java.lang.reflect.Field; import java.time.LocalDateTime; import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
@ExtendWith(MockitoExtension.class) class DailyJobServiceImplTest {

    @InjectMocks
    private DailyJobServiceImpl dailyJobService;

    @Mock
    private KiteTickerProvider kiteTickerProvider;

    // helper to set the private/public field `candleSticks` on the mocked provider
    private void setCandleSticksOnProvider(List<CandleStick> list) throws Exception {
        Field f = KiteTickerProvider.class.getDeclaredField("candleSticks");
        f.setAccessible(true);
        f.set(kiteTickerProvider, list);
    }

    @Test
    void getSwingHighLowByConfigNum_returnsCalculatedSwingHighAndLow() throws Exception {
        // prepare 5 consecutive minute candles
        LocalDateTime base = LocalDateTime.now().withSecond(0).withNano(0);
        List<CandleStick> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CandleStick cs = new CandleStick();
            cs.setCandleStartTime(base.plusMinutes(i));
            // default neighbors values
            cs.setLowPrice(20.0);
            cs.setHighPrice(30.0);
            list.add(cs);
        }

        // Make index 3 a clear swing low (lower than neighbors)
        list.get(3).setLowPrice(10.0);
        // Make index 2 a clear swing high (higher than neighbors)
        list.get(2).setHighPrice(50.0);

        setCandleSticksOnProvider(list);

        Integer configNum = 42;
        Map<Integer, SwingHighLow> result = dailyJobService.getSwingHighLowByConfigNum(configNum);

        assertNotNull(result);
        assertTrue(result.containsKey(configNum));

        SwingHighLow shl = result.get(configNum);
        assertNotNull(shl);

        CandleStick swingLow = shl.getSwingLow();
        CandleStick swingHigh = shl.getSwingHigh();

        assertNotNull(swingLow);
        assertNotNull(swingHigh);

        // verify values selected match the prepared candles
        assertEquals(10.0, swingLow.getLowPrice());
        assertEquals(list.get(3).getCandleStartTime(), swingLow.getCandleStartTime());

        assertEquals(50.0, swingHigh.getHighPrice());
        assertEquals(list.get(2).getCandleStartTime(), swingHigh.getCandleStartTime());
    }

    @Test
    void getSwingHighLowByConfigNum_withEmptyCandleList_returnsDefaults() throws Exception {
        setCandleSticksOnProvider(Collections.emptyList());

        Integer configNum = 1;
        Map<Integer, SwingHighLow> result = dailyJobService.getSwingHighLowByConfigNum(configNum);

        assertNotNull(result);
        assertTrue(result.containsKey(configNum));

        SwingHighLow shl = result.get(configNum);
        assertNotNull(shl);

        CandleStick swingLow = shl.getSwingLow();
        CandleStick swingHigh = shl.getSwingHigh();

        // Default CandleStick constructed by service should have null/zero fields based on domain class.
        // Assert common nullable expectations (adjust if your CandleStick defaults differ)
        assertNotNull(swingLow);
        assertNull(swingLow.getCandleStartTime());
        assertNull(swingLow.getLowPrice());
        assertNotNull(swingHigh);
        assertNull(swingHigh.getCandleStartTime());
        assertNull(swingHigh.getHighPrice());
    }

}
