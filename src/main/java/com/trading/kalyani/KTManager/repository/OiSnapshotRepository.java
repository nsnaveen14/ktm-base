package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.OISnapshotEntity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public interface OiSnapshotRepository extends CrudRepository<OISnapshotEntity,Long> {

    @Query(value="SELECT * FROM OISnapshot o WHERE o.instrument_token IN :listInstrumentToken", nativeQuery = true)
    Optional<ArrayList<OISnapshotEntity>> findSnapshotTokenByListInstrument_token(List<Long> listInstrumentToken);

}
