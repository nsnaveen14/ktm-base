package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.LTPTracker;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LTPTrackerRepository extends CrudRepository<LTPTracker, Long> {

    @Query(value="SELECT * FROM ltptracker WHERE job_iteration_id = :id",nativeQuery = true)
    LTPTracker getLTPTrackerByJITID(Long id);

    // Custom query methods can be defined here if needed

    // Example: Find by some field
    // List<LTPTracker> findBySomeField(String someField);

    // Example: Find all records
    // List<LTPTracker> findAll();


}
