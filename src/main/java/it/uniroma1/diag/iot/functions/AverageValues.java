package it.uniroma1.diag.iot.functions;

import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.api.common.functions.ReduceFunction;

public class AverageValues implements ReduceFunction<StationData> {

    public StationData reduce(StationData a, StationData b) {
        StationData value = new StationData();

        value.setStation_id(a.getStation_id());
        value.setTime(a.getTime().getTime() < b.getTime().getTime() ? a.getTime() : b.getTime());
        value.setTimestamp(a.getTime().getTime() < b.getTime().getTime() ? a.getTimestamp() : b.getTimestamp());

        value.setTemperature((a.getTemperature() + b.getTemperature()) / 2d);
        value.setHumidity((a.getHumidity() + b.getHumidity()) / 2d);
        value.setWind_direction((a.getWind_direction() + b.getWind_direction()) / 2d);
        value.setWind_intensity((a.getWind_intensity() + b.getWind_intensity()) / 2d);
        value.setRain_height((a.getRain_height() + b.getRain_height()) / 2d);
        
        return value;
    }
}
