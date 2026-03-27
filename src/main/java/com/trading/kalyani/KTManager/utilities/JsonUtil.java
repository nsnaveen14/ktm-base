package com.trading.kalyani.KTManager.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtil {

    public static String convertToJson(Object object) {
        String jsonString = "";
        try {
            jsonString = new ObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    public static <T> T convertFromJson(String jsonString, Class<T> clazz) {
        T object = null;
        try {
            object = new ObjectMapper().readValue(jsonString, clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return object;
    }


    public static JsonNode transformStringToJson(String inputString) {
        // Create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
             // Parse JSON string into JsonNode
            String validJson =  inputString
                    .replaceAll("([a-zA-Z0-9_]+):", "\"$1\":")
                    .replaceAll("([a-zA-Z0-9_]+)(,|\\s*})", "\"$1\"$2")
                    .replaceAll("=", ":");

            jsonNode = objectMapper.readTree(validJson);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return jsonNode;
    }

}
