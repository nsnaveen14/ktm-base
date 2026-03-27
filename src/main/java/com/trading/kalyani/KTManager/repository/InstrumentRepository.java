package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.InstrumentEntity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;

@Repository
public interface InstrumentRepository extends CrudRepository<InstrumentEntity,Long> {


    @Query(value= """
            SELECT i.id, i.last_price, i.lot_size, i.tick_size, i.exchange_token, i.expiry, i.instrument_token, i.exchange, i.instrument_type, i.name, i.segment, i.strike, i.tradingsymbol
                 FROM INSTRUMENT i
                 WHERE i.NAME = 'NIFTY' AND i.SEGMENT = 'NFO-OPT'
                 AND (
                     i.EXPIRY = (
                         SELECT MIN(EXPIRY)
                         FROM INSTRUMENT
                         WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE
                     )
                     OR i.EXPIRY = (
                         SELECT MAX(EXPIRY)
                         FROM INSTRUMENT
                         WHERE
                             EXTRACT(MONTH FROM i.EXPIRY) = EXTRACT(MONTH FROM (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE))
                             AND i.NAME = 'NIFTY'
                             AND i.SEGMENT = 'NFO-OPT'
                             AND EXTRACT(YEAR FROM i.EXPIRY) = EXTRACT(YEAR FROM (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE))
                     )
                 );

            """,nativeQuery = true)
    ArrayList<InstrumentEntity> findAllInstruments();

    @Query(value= """
    SELECT  * FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY = (SELECT MIN(EXPIRY) FROM INSTRUMENT  WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE)
    ORDER BY INSTRUMENT_TYPE ASC
    """,nativeQuery = true)
    ArrayList<InstrumentEntity> findNFOInstrumentsForCurrentWeek();

    @Query(value= """
            SELECT * FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY BETWEEN (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE) AND
               (SELECT MIN(EXPIRY) + INTERVAL '2 WEEK' FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE) ORDER BY expiry ASC
    """,nativeQuery = true)
    ArrayList<InstrumentEntity> findNFOInstrumentsForCurrentAndFollowingWeek();

    @Query(value = """
            SELECT * FROM INSTRUMENT WHERE CAST(STRIKE AS NUMERIC) = CAST(:strikePrice AS NUMERIC) ORDER BY EXPIRY ASC LIMIT 4
            """, nativeQuery = true)
    ArrayList<InstrumentEntity> findInstrumentFromStrikePrice(String strikePrice);

    @Query(value = """
            SELECT * FROM INSTRUMENT WHERE CAST(STRIKE AS NUMERIC) = CAST(:strikePrice AS NUMERIC) AND NAME = 'NIFTY' AND EXPIRY >= CURRENT_DATE ORDER BY EXPIRY ASC LIMIT 2
            """, nativeQuery = true)
    ArrayList<InstrumentEntity> findNearestExpiryInstrumentFromStrikePrice(String strikePrice);

    @Query(value = """
            SELECT * FROM INSTRUMENT
            WHERE NAME = 'NIFTY'
            AND SEGMENT = 'NFO-OPT'
            AND CAST(STRIKE AS NUMERIC) = CAST(:strikePrice AS NUMERIC)
            AND EXPIRY = (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = 'NIFTY' AND SEGMENT = 'NFO-OPT' AND EXPIRY >= CURRENT_DATE)
            ORDER BY INSTRUMENT_TYPE ASC
            """, nativeQuery = true)
    ArrayList<InstrumentEntity> findNiftyATMOptionsForNearestExpiry(@Param("strikePrice") String strikePrice);


    @Query(value = """
            SELECT I.* FROM INSTRUMENT I JOIN APP_INDEX_CONFIG AC ON I.instrument_token = AC.instrument_token
            AND AC.index_id = :indexId
            """, nativeQuery = true)
    InstrumentEntity findInstrumentByAppIndexId(@Param("indexId") Integer indexId);

    @Query(value= """
            SELECT i.id, i.last_price, i.lot_size, i.tick_size, i.exchange_token, i.expiry, i.instrument_token, i.exchange, i.instrument_type, i.name, i.segment, i.strike, i.tradingsymbol
                 FROM INSTRUMENT i
                 WHERE i.NAME = :name AND i.SEGMENT = :segment
                 AND (
                     i.EXPIRY = (
                         SELECT MIN(EXPIRY)
                         FROM INSTRUMENT
                         WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE
                     )
                     OR i.EXPIRY = (
                         SELECT MAX(EXPIRY)
                         FROM INSTRUMENT
                         WHERE
                             EXTRACT(MONTH FROM i.EXPIRY) = EXTRACT(MONTH FROM (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE))
                             AND i.NAME = :name
                             AND i.SEGMENT = :segment
                             AND EXTRACT(YEAR FROM i.EXPIRY) = EXTRACT(YEAR FROM (SELECT MIN(EXPIRY) FROM INSTRUMENT WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE))
                     )
                 );

            """,nativeQuery = true)
    ArrayList<InstrumentEntity> findAllInstrumentsByNameANDSegment(@Param("name") String name, @Param("segment") String segment );

    @Query(value= """
    SELECT  * FROM INSTRUMENT WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY = (SELECT MIN(EXPIRY) FROM INSTRUMENT  WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE)
    ORDER BY INSTRUMENT_TYPE ASC
    """,nativeQuery = true)
    ArrayList<InstrumentEntity> findInstrumentsForNearestExpiryByNameANDSegment(@Param("name") String name, @Param("segment") String segment);

    @Query(value= """
            SELECT * FROM INSTRUMENT WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY BETWEEN\s
                                        (SELECT MIN(EXPIRY) + INTERVAL '1 DAY' FROM INSTRUMENT  WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE)
                                        and
                                        (SELECT MIN(EXPIRY) + INTERVAL '8 DAY' FROM INSTRUMENT  WHERE NAME = :name AND SEGMENT = :segment AND EXPIRY >= CURRENT_DATE)
                                        ORDER BY INSTRUMENT_TYPE asc
    """,nativeQuery = true)
    ArrayList<InstrumentEntity> findInstrumentsForFollowingWeeklyExpiryByNameANDSegment(@Param("name") String name, @Param("segment") String segment);

    @Query(value = """
    select
	*
    from
    INSTRUMENT i
    where
    i.NAME = :name
    and i.SEGMENT = :segment
    and i.EXPIRY = (
    select
    MAX(EXPIRY)
    from
    INSTRUMENT i
    where
    extract(month from i.EXPIRY) = extract(month from (select MIN(EXPIRY) from INSTRUMENT i where NAME = :name and SEGMENT = :segment AND EXPIRY >= CURRENT_DATE))
    and i.NAME = :name
    and i.SEGMENT = :segment
    and extract(year from i.EXPIRY) = extract(year from (select MIN(EXPIRY) from INSTRUMENT i where NAME = :name and SEGMENT = :segment AND EXPIRY >= CURRENT_DATE))
            )
    ORDER BY INSTRUMENT_TYPE ASC
    """, nativeQuery = true)
    ArrayList<InstrumentEntity> findInstrumentsForMonthlyExpiryByNameANDSegment(@Param("name") String name, @Param("segment") String segment);

}
