package it.uniroma1.diag.iot;

import it.uniroma1.diag.iot.connector.AWSIoTMqttSink;
import it.uniroma1.diag.iot.connector.AWSIoTMqttStream;
import it.uniroma1.diag.iot.functions.*;
import it.uniroma1.diag.iot.model.StationData;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.BasicConfigurator;

import java.util.concurrent.TimeUnit;

/**
 * A simple flink stream processing engine that connects to the AWS IoT message broker,
 * converts the messages received into measurement object
 * and processes the values received in windows of 1 minute.
 *
 * @author ichatz@diag.uniroma1.it
 */
public class WindowListener  {

    public static void main(String[] args) throws Exception {

        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<byte[]> awsStream = env.addSource(new AWSIoTMqttStream(AppConfiguration.brokerHost,
                "flink", AppConfiguration.certificateFile, AppConfiguration.privateKeyFile,
                AppConfiguration.topic, AppConfiguration.qos));

        final DataStream<StationData> dataStream = // convert messages to SensorData
                awsStream.map(new ParseMeasurement())
                        .map(new TimeParser());

        // Assign timestamps
        final DataStream<StationData> timedStream =
                dataStream.assignTimestampsAndWatermarks(new TimestampExtractor());

        final int windowMinutes = 1;

        // Define the window and apply the reduce transformation
        final DataStream<StationData> resultStream = timedStream
                .timeWindowAll(Time.minutes(windowMinutes))
                .allowedLateness(Time.minutes(1))
                .reduce(new AverageValues());

        // Define the window and apply the reduce transformation
        final DataStream<String> jsonStream = resultStream
                .map(new ExtractJson());

        final DataStreamSink<String> finalStream = jsonStream.addSink(new AWSIoTMqttSink(AppConfiguration.brokerHost,
                "flink", AppConfiguration.certificateFile, AppConfiguration.privateKeyFile,
                AppConfiguration.outExchange, AppConfiguration.qos));

        ((SingleOutputStreamOperator<String>) jsonStream).print().setParallelism(1);

        env.execute("Window Listener");
    }


}
