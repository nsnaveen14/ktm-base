package com.trading.kalyani.KTManager.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

public interface FileService {
     boolean writeObjToFile(Object inputObj,String fileName);

     JsonNode convertJsonFileToObj(String fileName);

}
