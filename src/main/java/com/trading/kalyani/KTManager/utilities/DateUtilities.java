package com.trading.kalyani.KTManager.utilities;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtilities {
    public static LocalDateTime convertToLocalDateTime(Date dateToConvert) {
        if (dateToConvert == null) return null;
        return LocalDateTime.ofInstant(
                dateToConvert.toInstant(), ZoneId.systemDefault());
    }

    public static LocalDate convertDateToLocalDate(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

    public static LocalDateTime convertStringToLocalDateTime(String dateTimeString, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return LocalDateTime.parse(dateTimeString, formatter);
    }

    public static boolean isWednesdayOrThursday(LocalDate currentDate) {
        DayOfWeek dayOfWeek = currentDate.getDayOfWeek();
        return dayOfWeek == DayOfWeek.WEDNESDAY || dayOfWeek == DayOfWeek.THURSDAY;
    }

    /**
     * Converts LocalDateTime to java.util.Date
     * @param localDateTime the LocalDateTime to convert
     * @return Date object or null if input is null
     */
    public static Date convertLocalDateTimeToDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

}
