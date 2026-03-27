package com.trading.kalyani.KTManager.service.serviceImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.trading.kalyani.KTManager.service.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;

@Service

public class FileServiceImpl implements FileService {

    private static final Logger logger = LoggerFactory.getLogger(FileServiceImpl.class);

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

   public boolean writeObjToFile(Object inputObj,String fileName)
   {
       try {

           // create a file object
           File file = new File(fileName);

           if(file.exists())
               file.delete();

           // write JSON to a File
           objectMapper.writeValue(file, inputObj);

           logger.info("File saved to: {}", file.getAbsolutePath());

           return true;

       } catch (IOException e) {
           throw new RuntimeException(e);
       }

   }

   public JsonNode convertJsonFileToObj(String fileName) {
       JsonNode jsonNode = null;
       File file = new File(fileName + ".json");
       logger.info("JSON file read successfully: {}", file.getAbsolutePath());
       try {
           jsonNode = objectMapper.readTree(file);
           logger.info("JSON content: {}", jsonNode.toString());
           return jsonNode;
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
   }

}
