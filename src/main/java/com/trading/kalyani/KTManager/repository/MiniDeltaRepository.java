package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.MiniDelta;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface MiniDeltaRepository extends CrudRepository<MiniDelta,Long> {
    @Query(value = "SELECT COALESCE(MIN(DELTA_INSTANT),'1970-01-01') FROM MINI_DELTA",nativeQuery = true)
    LocalDateTime findMinDBDate();

    @Query(value = "SELECT * FROM MINI_DELTA ORDER BY ID ASC", nativeQuery = true)
    List<MiniDelta> getAllDataForMiniDelta();

    @Query(value="SELECT ID,DELTA_INSTANT,STRIKE_PRICE, PUTOI, CALLOI, STRIKEPCR, RATEOI,CALLOICHANGE,PUTOICHANGE FROM MINI_DELTA WHERE DELTA_INSTANT = (SELECT MAX(DELTA_INSTANT) FROM MINI_DELTA) ORDER BY ID ASC ", nativeQuery = true)
    List<MiniDelta> getLatestMiniDelta();

    @Modifying
    @Transactional
    void deleteByAppJobConfigNum(Integer appJobConfigNum);

    List<MiniDelta> findByAppJobConfigNumOrderByIdAsc(Integer appJobConfigNum);

}
