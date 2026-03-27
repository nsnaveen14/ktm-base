package com.trading.kalyani.KTManager.utilities;

import java.util.Locale;

public class StringUtils {

    public static String formatSafeNumber(String raw) {
        if (raw == null || raw.isBlank()) return "N/A";
        // Try direct parse/format
        try {
            double d = Double.parseDouble(raw);
            return String.format(Locale.US, "%.2f", d);
        } catch (NumberFormatException ignored) {
        }
        // Remove any characters except digits, sign, dot and exponent marker
        String sanitized = raw.replaceAll("[^0-9eE+\\-\\.]", "");
        // Collapse repeated E/E (e.g. "EE" -> "E")
        sanitized = sanitized.replaceAll("([eE])\\1+", "$1");
        // Ensure there's not a leading standalone dot (".123" is valid for Double.parseDouble,
        // but some malformed inputs may still fail). Try parse again.
        try {
            double d = Double.parseDouble(sanitized);
            return String.format(Locale.US, "%.2f", d);
        } catch (NumberFormatException ignored) {
        }
        // As last resort return N/A
        return "N/A";
    }


}
