package it.uniroma1.diag.iot.connector;

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsTopic extends AWSIotTopic {

    private static final Logger LOG = LoggerFactory.getLogger(AwsTopic.class);

    private final SourceFunction.SourceContext<byte[]> sourceContext;

    public AwsTopic(String topic, AWSIotQos qos) {
        this(null, topic, qos);
    }

    public AwsTopic(SourceFunction.SourceContext<byte[]> ctx, String topic, AWSIotQos qos) {
        super(topic, qos);
        sourceContext = ctx;
    }

    @Override
    public void onMessage(AWSIotMessage message) {
        // called when a message is received
        LOG.debug("Mqtt message received: " + message.getStringPayload());

        if (sourceContext != null) {
            sourceContext.collect(message.getPayload());
        }
    }
}
