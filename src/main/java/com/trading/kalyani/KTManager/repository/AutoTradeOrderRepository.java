package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.AutoTradeOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface AutoTradeOrderRepository extends JpaRepository<AutoTradeOrder, Long> {

    Optional<AutoTradeOrder> findByOrderId(String orderId);

    Optional<AutoTradeOrder> findByKiteOrderId(String kiteOrderId);

    Optional<AutoTradeOrder> findByIobId(Long iobId);

    @Query("SELECT o FROM AutoTradeOrder o WHERE o.status IN ('PENDING', 'PLACED', 'OPEN') " +
            "ORDER BY o.orderTime DESC")
    List<AutoTradeOrder> findPendingOrders();

    @Query("SELECT o FROM AutoTradeOrder o WHERE o.instrumentToken = :instrumentToken " +
            "AND o.status IN ('PENDING', 'PLACED', 'OPEN')")
    List<AutoTradeOrder> findPendingOrdersByInstrument(@Param("instrumentToken") Long instrumentToken);

    @Query("SELECT o FROM AutoTradeOrder o WHERE o.orderTime >= :startTime " +
            "ORDER BY o.orderTime DESC")
    List<AutoTradeOrder> findTodaysOrders(@Param("startTime") LocalDateTime startTime);

    @Query("SELECT o FROM AutoTradeOrder o WHERE o.orderPurpose = :purpose " +
            "AND o.status IN ('PENDING', 'PLACED', 'OPEN')")
    List<AutoTradeOrder> findPendingByPurpose(@Param("purpose") String purpose);

    @Query("SELECT COUNT(o) FROM AutoTradeOrder o WHERE o.orderTime >= :startTime " +
            "AND o.status = 'COMPLETE' AND o.orderPurpose = 'ENTRY'")
    int countTodaysCompletedEntries(@Param("startTime") LocalDateTime startTime);
}
