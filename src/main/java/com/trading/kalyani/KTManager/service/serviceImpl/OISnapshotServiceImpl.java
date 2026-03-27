package com.trading.kalyani.KTManager.service.serviceImpl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.trading.kalyani.KTManager.entity.OISnapshotEntity;
import com.trading.kalyani.KTManager.repository.OiSnapshotRepository;
import com.trading.kalyani.KTManager.service.OISnapshotService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class OISnapshotServiceImpl implements OISnapshotService {

    private static final Logger logger = LoggerFactory.getLogger(OISnapshotServiceImpl.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Autowired
    OiSnapshotRepository oiSnapshotRepository;

    @Override
    public boolean processOISnapshot(MultipartFile file) {

        // Define the path where the file will be saved
        File uploadFile = new File(Objects.requireNonNull(file.getOriginalFilename()));

        // Save the file to the server
        try (FileOutputStream fos = new FileOutputStream(uploadFile)) {
            fos.write(file.getBytes());
            fos.flush();
        }
        catch (Exception e) {
            logger.error("Error writing OISnapshot upload file: {}", e.getMessage(), e);
            return false;
        }

        try {
            List<OISnapshotEntity> oiSnapshotEntityList = objectMapper.readValue(uploadFile, new TypeReference<>() {
            });

            logger.info("File contains {} records: ", oiSnapshotEntityList.size());

            int insertedRecords = loadOISnapshot(oiSnapshotEntityList);
            logger.info("Inserted {} new records into the database", insertedRecords);
            return true;

        } catch (RuntimeException | IOException e) {
            logger.error("Error processing OISnapshot file: {}", e.getMessage());
            return false;
        }
        finally {
            // Delete the temp file after processing
            if (uploadFile.exists() && !uploadFile.delete()) {
                logger.warn("Failed to delete temp upload file: {}", uploadFile.getAbsolutePath());
            }
        }


    }

    @Override
    @Transactional
    public int deleteOISnapshotByInstrumentTokens(List<Long> instrumentTokenList) {

        try {
            List<OISnapshotEntity> oiSnapshotEntitysInDB = oiSnapshotRepository.findSnapshotTokenByListInstrument_token(instrumentTokenList)
                    .orElse(new ArrayList<>());
            logger.info("Found {} matching records in the database", oiSnapshotEntitysInDB.size());
            oiSnapshotRepository.deleteAll(oiSnapshotEntitysInDB);
            logger.info("Deleting all previous OISnapshotEntity records");
            return oiSnapshotEntitysInDB.size();
        }
        catch (Exception e) {
            logger.error("Error deleting OISnapshotEntity records: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    @Transactional
    public int loadOISnapshot(List<OISnapshotEntity> oiSnapshotEntityList) {
        try {
            // Bulk delete existing records for these tokens, then bulk insert fresh copies
            List<Long> tokens = oiSnapshotEntityList.stream()
                    .map(OISnapshotEntity::getInstrument_token)
                    .toList();
            oiSnapshotRepository.deleteAllById(tokens);

            List<OISnapshotEntity> freshEntities = oiSnapshotEntityList.stream()
                    .map(OISnapshotServiceImpl::getOiSnapshotEntity)
                    .toList();
            oiSnapshotRepository.saveAll(freshEntities);

            logger.info("Saved {} OISnapshotEntity records", freshEntities.size());
            return freshEntities.size();
        } catch (Exception e) {
            logger.error("Error saving OISnapshotEntity records: {}", e.getMessage(), e);
            return 0;
        }
    }

    @NotNull
    private static OISnapshotEntity getOiSnapshotEntity(OISnapshotEntity oiSnapshotEntity) {
        OISnapshotEntity freshOISnapshotEntity = new OISnapshotEntity();
        freshOISnapshotEntity.setInstrument_token(oiSnapshotEntity.getInstrument_token());
        freshOISnapshotEntity.setOi(oiSnapshotEntity.getOi());
        freshOISnapshotEntity.setTickTimestamp(oiSnapshotEntity.getTickTimestamp());
        freshOISnapshotEntity.setTradingsymbol(oiSnapshotEntity.getTradingsymbol());
        freshOISnapshotEntity.setName(oiSnapshotEntity.getName());
        return freshOISnapshotEntity;
    }
}
