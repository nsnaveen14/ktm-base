package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.AppJobConfig;
import com.trading.kalyani.KTManager.entity.DailyJobPlanner;
import com.trading.kalyani.KTManager.entity.DailyJobPlannerId;
import com.trading.kalyani.KTManager.entity.InstrumentEntity;
import com.trading.kalyani.KTManager.repository.AppJobConfigRepository;
import com.trading.kalyani.KTManager.repository.DailyJobPlannerRepository;
import com.trading.kalyani.KTManager.service.DailyJobPlannerService;
import com.trading.kalyani.KTManager.service.InstrumentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertDateToLocalDate;

@Service
public class DailyJobPlannerServiceImpl implements DailyJobPlannerService {

    @Autowired
    InstrumentService instrumentService;

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Autowired
    DailyJobPlannerRepository dailyJobPlannerRepository;

    private static final Logger logger = LoggerFactory.getLogger(DailyJobPlannerServiceImpl.class);

    @Override
    public List<DailyJobPlanner> createDailyJobPlannerEntry() {

        // Tracks seen expiry dates per index to detect duplicates across job types 2 & 3
        Map<Integer, List<Date>> expiryDatesMap = new HashMap<>();
        List<DailyJobPlanner> dailyJobPlanners = new ArrayList<>();

        for (AppJobConfig appJobConfig : appJobConfigRepository.findAllByOrderByAppJobConfigNumAsc()) {
            logger.info("Configuring daily job planner for AppJobConfig: {}", appJobConfig);

            if (!appJobConfig.getIsActive()) {
                continue;
            }

            int jobTypeCode = appJobConfig.getJobType().getJobTypeCode();
            logger.info("Processing job type: {}", jobTypeCode);

            // Fetch instruments once — avoids double service/DB call
            List<InstrumentEntity> instruments =
                    instrumentService.getInstrumentsFromAppJobConfigNum(appJobConfig.getAppJobConfigNum());
            InstrumentEntity instrument = instruments.isEmpty() ? new InstrumentEntity() : instruments.getFirst();

            if (instrument.getInstrument() == null) {
                logger.warn("No instrument found for AppJobConfigNum: {}", appJobConfig.getAppJobConfigNum());
                continue;
            }

            Date expiry = instrument.getInstrument().getExpiry();
            Integer indexId = appJobConfig.getAppIndexConfig().getIndexId();
            logger.info("Fetched instrument for expiry: {}", expiry);

            DailyJobPlanner dailyJobPlanner;

            if (jobTypeCode == I_ONE) {
                // Current week expiry job — seed the expiry list for this index
                logger.info("Creating job planner entry for job type 1");
                expiryDatesMap.computeIfAbsent(indexId, k -> new ArrayList<>()).add(expiry);
                dailyJobPlanner = buildDailyJobPlannerObj(appJobConfig, expiry, true);

            } else if (jobTypeCode == I_TWO || jobTypeCode == I_THREE) {
                // Next week / Monthly expiry job
                logger.info("Creating job planner entry for job type {}", jobTypeCode);

                // Guard: I_ONE may have been skipped (inactive) — initialise list if absent
                List<Date> seenExpiries = expiryDatesMap.computeIfAbsent(indexId, k -> new ArrayList<>());

                if (seenExpiries.contains(expiry)) {
                    dailyJobPlanner = buildDailyJobPlannerObj(appJobConfig, expiry, false);
                    logger.info("Created job planner entry for job type {} with jobRequired=false", jobTypeCode);
                } else {
                    seenExpiries.add(expiry);
                    dailyJobPlanner = buildDailyJobPlannerObj(appJobConfig, expiry, true);
                    logger.info("Created job planner entry for job type {} with jobRequired=true", jobTypeCode);
                }

            } else {
                logger.warn("Unknown job type code: {}", jobTypeCode);
                continue;
            }

            dailyJobPlanners.add(dailyJobPlanner);
        }

        // Batch-save all entries in a single DB round-trip
        if (!dailyJobPlanners.isEmpty()) {
            dailyJobPlanners = (List<DailyJobPlanner>) dailyJobPlannerRepository.saveAll(dailyJobPlanners);
        }

        return dailyJobPlanners;
    }

    /** Builds (does NOT save) a DailyJobPlanner entity. Saving is batched by the caller. */
    private DailyJobPlanner buildDailyJobPlannerObj(AppJobConfig appJobConfig, Date expiry, boolean isJobRequired) {

        DailyJobPlanner dailyJobPlanner = new DailyJobPlanner();
        dailyJobPlanner.setId(new DailyJobPlannerId(LocalDate.now(), appJobConfig.getAppJobConfigNum()));
        dailyJobPlanner.setJobForExpiryDate(convertDateToLocalDate(expiry));
        dailyJobPlanner.setJobRequired(isJobRequired);
        dailyJobPlanner.setLastModified(LocalDateTime.now().withNano(I_ZERO));

        logger.info("Built DailyJobPlanner entry: {}", dailyJobPlanner);
        return dailyJobPlanner;
    }

    @Override
    public boolean validateDailyPlannerByConfigNum(Integer appConfigNum, LocalDate currentDate) {
        return dailyJobPlannerRepository
                .findById_AppJobConfigNumAndId_JobDate(appConfigNum, currentDate)
                .map(DailyJobPlanner::isJobRequired)
                .orElse(false);
    }

    @Override
    public List<DailyJobPlanner> getDailyJobPlannerList() {

        LocalDate latestJobDate = LocalDate.now();
    //  LocalDate latestJobDate = LocalDate.of(2025,11,28); // For testing purpose only, to be removed later
        return dailyJobPlannerRepository.findByLatestJobDate(latestJobDate);
    }

    @Override
    public DailyJobPlanner modifyDailyJobPlannerConfig(DailyJobPlanner dailyJobPlanner) {
        dailyJobPlanner.setLastModified(LocalDateTime.now().withNano(0));
        return dailyJobPlannerRepository.save(dailyJobPlanner);
    }


}
