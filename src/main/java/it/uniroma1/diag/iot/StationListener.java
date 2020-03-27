package it.uniroma1.diag.iot;

import it.uniroma1.diag.iot.connector.AWSIoTMqttStream;
import it.uniroma1.diag.iot.functions.ParseMeasurement;
import it.uniroma1.diag.iot.functions.TimeParser;
import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.BasicConfigurator;

/**
 * A simple flink stream processing engine that connects to the AWS IoT message broker,
 * converts the messages received into measurement object
 * and outputs the values received.
 *
 * @author ichatz@diag.uniroma1.it
 */
public class StationListener {

    public static void main(String[] args) throws Exception {

        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<byte[]> awsStream = env.addSource(new AWSIoTMqttStream(AppConfiguration.brokerHost,
                "flink", AppConfiguration.certificateFile, AppConfiguration.privateKeyFile,
                AppConfiguration.topic, AppConfiguration.qos));

        final DataStream<StationData> dataStream = // convert messages to SensorData
                awsStream.map(new ParseMeasurement())
                .map(new TimeParser());

        dataStream.print().setParallelism(1);

        env.execute("Station Listener");
    }


}
