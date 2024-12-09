package Finding_nemo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class DateUtils {

    public static String convertToUTC(String timeStr) {
        String cleanedTimeStr;
        int offsetHours = 0;

        if (timeStr.contains(" EDT")) {
            cleanedTimeStr = timeStr.replace(" EDT", "");
            offsetHours = -4;
        } else if (timeStr.contains(" EST")) {
            cleanedTimeStr = timeStr.replace(" EST", "");
            offsetHours = -5;
        } else {
            cleanedTimeStr = timeStr;
        }

        List<String> formats = Arrays.asList(
                "MMMM dd, yyyy — hh:mm a", // "September 12, 2023 — 06:15 pm"
                "MMM dd, yyyy hh:mm a",    // "Nov 14, 2023 7:35AM"
                "dd-MMM-yy",                // "6-Jan-22"
                "yyyy-MM-dd",               // "2021-4-5"
                "yyyy/MM/dd",               // "2021/4/5"
                "MMM dd, yyyy"              // "DEC 7, 2023"
        );

        for (String format : formats) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.ENGLISH);
                java.util.Date date = sdf.parse(cleanedTimeStr);
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime(date);
                cal.add(java.util.Calendar.HOUR_OF_DAY, offsetHours);
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'").format(cal.getTime());
            } catch (ParseException e) {
                // Continue to next format
            }
        }

        return "Invalid date format";
    }
}
