package it.uniroma1.diag.iot.functions;

import com.google.gson.Gson;
import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Simple map function for converting String payloads for timestamp into Data objects.
 *
 * @author ichatz@gmail.com
 */
public class TimeParser implements MapFunction<StationData, StationData> {

    final String pattern = "yyyy-MM-dd HH:mm:ss";

    public StationData map(StationData message) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        message.setTime(simpleDateFormat.parse(message.getTimestamp()));
        return message;
    }

}
