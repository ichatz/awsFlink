package it.uniroma1.diag.iot.functions;

import com.google.gson.Gson;
import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Simple map function for converting StationData objects to JSON objects.
 *
 * @author ichatz@diag.uniroma1.it
 */
public class ExtractJson implements MapFunction<StationData, String> {

    public String map(StationData value) throws Exception {
        Gson gson = new Gson();

        // Java object to JSON string
        return gson.toJson(value);
    }
}
