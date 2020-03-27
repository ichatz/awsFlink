package it.uniroma1.diag.iot;

import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;
import it.uniroma1.diag.iot.connector.AwsMessage;
import it.uniroma1.diag.iot.connector.AwsTopic;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple mqtt stream listener that connects to the AWS IoT message broker
 * and outputs the messages received without any further processing.
 *
 * @author ichatz@diag.uniroma1.it
 */
public class MQTTListener {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTListener.class);

    private static AWSIotMqttClient awsIotClient;

    public MQTTListener() {
        String clientEndpoint = AppConfiguration.brokerHost;
        String clientId = "flink";

        // SampleUtil.java and its dependency PrivateKeyReader.java can be copied from the sample source code.
        // Alternatively, you could load key store directly from a file - see the example included in this README.
        SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(AppConfiguration.certificateFile,
                AppConfiguration.privateKeyFile);
        awsIotClient = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);
    }

    public AWSIotMqttClient connect() throws Exception {
        awsIotClient.connect();
        return awsIotClient;
    }

    public static void main(String[] args) throws Exception {
        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        MQTTListener mqttListener = new MQTTListener();

        try {
            AWSIotMqttClient client = mqttListener.connect();

            client.subscribe(new AwsTopic(AppConfiguration.topic, AppConfiguration.qos));

            Thread.sleep(10000);

            AwsMessage message = new AwsMessage(AppConfiguration.topic, AppConfiguration.qos, "Hello from here");
            client.publish(message, 3000);

        } catch (Exception ex) {
            LOG.error("Error encountered", ex);
        }
    }
}
