package it.uniroma1.diag.iot.connector;

import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.sample.sampleUtil.PrivateKeyReader;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;

public class AWSIoTMqttSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 7883296716671354462L;

    private static final Logger LOG = LoggerFactory.getLogger(AWSIoTMqttSink.class);

    private final String awsIoTMqttEndpoint;
    private final String awsIoTMqttClientId;

    private final List<Certificate> certificates;
    private final PrivateKey privateKey;

    private final String awsIoTMqttTopic;
    private final AWSIotQos awsIoTQos;

    public AWSIoTMqttSink(final String clientEndpoint,
                          final String clientId,
                          final String certificateFile,
                          final String privateKeyFile,
                          final String topic,
                          final AWSIotQos qos) {

        this.awsIoTMqttEndpoint = clientEndpoint;
        this.awsIoTMqttClientId = clientId;

        certificates = loadCertificatesFromFile(certificateFile);
        this.privateKey = loadPrivateKeyFromFile(privateKeyFile);

        this.awsIoTMqttTopic = topic;
        this.awsIoTQos = qos;
    }

    public void invoke(T event) throws Exception {
        this.invoke(event, null);
    }

    public void invoke(T event, SinkFunction.Context context) throws Exception {
        LOG.info("Publishing to AWS IoT Mqtt endpoint: " + event);

        try {
            final AWSIotMqttClient awsIotClient;
            final KeyStore keyStore;
            final String keyPassword;

            try {
                keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load((KeyStore.LoadStoreParameter)null);
                keyPassword = (new BigInteger(128, new SecureRandom())).toString(32);
                Certificate[] certChain = new Certificate[certificates.size()];
                certChain = (Certificate[])certificates.toArray(certChain);
                keyStore.setKeyEntry("alias", privateKey, keyPassword.toCharArray(), certChain);

            } catch (Exception var5) {
                LOG.error("Failed to create key store", var5);
                return;
            }

            awsIotClient = new AWSIotMqttClient(awsIoTMqttEndpoint, awsIoTMqttClientId, keyStore, keyPassword);

            awsIotClient.connect();

            final AwsMessage message = new AwsMessage(awsIoTMqttTopic, awsIoTQos, ((String) event).toString());
            awsIotClient.publish(message, 3000);

            awsIotClient.disconnect();

        } catch (Exception ex) {
            LOG.error("Failed to connect to publish to endpoint.", ex);
        }
    }

    public void close() throws Exception {
        super.close();
    }

    private List<Certificate> loadCertificatesFromFile(String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            System.out.println("Certificate file: " + filename + " is not found.");
            return null;
        } else {
            try {
                BufferedInputStream stream = new BufferedInputStream(new FileInputStream(file));

                List var4 = null;
                try {
                    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                    var4 = (List)certFactory.generateCertificates(stream);
                } catch (Throwable var6) {
                    try {
                        stream.close();
                    } catch (Throwable var5) {
                        LOG.error("Cannot close stream", var5);
                    }

                    LOG.error("Cannot generate certificate", var6);
                }

                stream.close();
                return var4;
            } catch (Exception var7) {
                LOG.error("Failed to load certificate file " + filename);
                return null;
            }
        }
    }

    private PrivateKey loadPrivateKeyFromFile(String filename) {
        PrivateKey privateKey = null;
        File file = new File(filename);
        if (!file.exists()) {
            System.out.println("Private key file not found: " + filename);
            return null;
        } else {
            try {
                DataInputStream stream = new DataInputStream(new FileInputStream(file));

                try {
                    privateKey = PrivateKeyReader.getPrivateKey(stream, (String) null);
                } catch (Throwable var8) {
                    try {
                        stream.close();
                    } catch (Throwable var7) {
                        LOG.error("Cannot close stream", var7);
                    }

                    LOG.error("Cannot get private key", var8);
                }

                stream.close();
            } catch (Exception var9) {
                LOG.error("Failed to load private key from file " + filename);
            }

            return privateKey;
        }
    }

}
