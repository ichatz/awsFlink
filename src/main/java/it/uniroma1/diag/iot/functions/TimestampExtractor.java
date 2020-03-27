package it.uniroma1.diag.iot.functions;

import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TimestampExtractor
        implements AssignerWithPeriodicWatermarks<StationData> {

    /**
     * The current timestamp.
     */
    private long currentTimestamp = Long.MIN_VALUE;

    public long extractTimestamp(StationData element, long previousElementTimestamp) {
        currentTimestamp = element.getTime().getTime();
        return element.getTime().getTime();
    }

    public final Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp);
    }


}
