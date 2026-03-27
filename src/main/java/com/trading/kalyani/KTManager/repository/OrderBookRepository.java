package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.OrderBook;
import org.springframework.data.repository.CrudRepository;

public interface OrderBookRepository extends CrudRepository<OrderBook,String> {
}
