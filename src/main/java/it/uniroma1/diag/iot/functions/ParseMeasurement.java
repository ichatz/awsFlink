package it.uniroma1.diag.iot.functions;

import com.google.gson.Gson;
import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Simple map function for converting MQTT Message payloads to StationData objects.
 *
 * @author ichatz@diag.uniroma1.it
 */
public class ParseMeasurement implements MapFunction<byte[], StationData> {

    public StationData map(byte[] message) throws Exception {
        Gson gson = new Gson();

        // from JSON to object
        StationData value = gson.fromJson(new String(message, "UTF-8"), StationData.class);

        return value;
    }

}
