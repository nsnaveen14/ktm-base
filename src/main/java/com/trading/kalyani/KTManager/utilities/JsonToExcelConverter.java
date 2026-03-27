package com.trading.kalyani.KTManager.utilities;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class JsonToExcelConverter {
    public static void convertJsonToExcel(String jsonFilePath, String excelFilePath) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
        JSONArray jsonArray = new JSONArray(content);

        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Sheet1");

        // Write header
        JSONObject firstObj = jsonArray.getJSONObject(0);
        Row headerRow = sheet.createRow(0);
        Iterator<String> keys = firstObj.keys();
        int cellIdx = 0;
        while (keys.hasNext()) {
            String key = keys.next();
            headerRow.createCell(cellIdx++).setCellValue(key);
        }

        // Write data rows
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            Row row = sheet.createRow(i + 1);
            int colIdx = 0;
            for (String key : firstObj.keySet()) {
                row.createCell(colIdx++).setCellValue(obj.optString(key, ""));
            }
        }

        // Write to file
        try (FileOutputStream fos = new FileOutputStream(excelFilePath)) {
            workbook.write(fos);
        }
        workbook.close();
    }

    public static <T> void saveListToExcel(List<T> list, String excelFilePath) throws Exception {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("List is null or empty");
        }
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Sheet1");

        // Use reflection to get fields
        Class<?> clazz = list.get(0).getClass();
        java.lang.reflect.Field[] fields = clazz.getDeclaredFields();

        // Write header
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < fields.length; i++) {
            fields[i].setAccessible(true);
            headerRow.createCell(i).setCellValue(fields[i].getName());
        }

        // Write data rows
        for (int i = 0; i < list.size(); i++) {
            Row row = sheet.createRow(i + 1);
            T obj = list.get(i);
            for (int j = 0; j < fields.length; j++) {
                Object value = fields[j].get(obj);
                row.createCell(j).setCellValue(value != null ? value.toString() : "");
            }
        }

        // Write to file
        try (FileOutputStream fos = new FileOutputStream(excelFilePath)) {
            workbook.write(fos);
        }
        workbook.close();
    }

    public static void main(String[] args) {
        try {
          /*  convertJsonToExcel("OISNAPSHOT_SENSEX_Next_Week_2025-09-17T09_10_02.json", "OISNAPSHOT_SENSEX_Next_Week_2025-09-17T09_10_02.xlsx");
            convertJsonToExcel("OISNAPSHOT_SENSEX_Current_Week_2025-09-17T09_10_02.json", "OISNAPSHOT_SENSEX_Current_Week_2025-09-17T09_10_02.xlsx");
            convertJsonToExcel("OISNAPSHOT_NIFTY_Next_Week_2025-09-17T09_10_02.json", "OISNAPSHOT_NIFTY_Next_Week_2025-09-17T09_10_02.xlsx");
            convertJsonToExcel("OISNAPSHOT_NIFTY_Current_Week_2025-09-17T09_10_02.json", "OISNAPSHOT_NIFTY_Current_Week_2025-09-17T09_10_02.xlsx");*/
            convertJsonToExcel("OISNAPSHOT_BANKNIFTY_Current_Week_2025-09-17T09_10_05.json", "OISNAPSHOT_BANKNIFTY_Current_Week_2025-09-17T09_10_05.xlsx");
            System.out.println("Conversion completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
