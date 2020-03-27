package it.uniroma1.diag.iot.connector;

import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.sample.sampleUtil.PrivateKeyReader;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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

public class AWSIoTMqttStream implements SourceFunction<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AWSIoTMqttStream.class);

    private transient AWSIotMqttClient awsIotClient;

    private final String awsIoTMqttEndpoint;
    private final String awsIoTMqttClientId;

    private final List<Certificate> certificates;
    private final PrivateKey privateKey;

    private final String awsIoTMqttTopic;
    private final AWSIotQos awsIoTQos;

    private volatile boolean isRunning = true;

    public AWSIoTMqttStream(final String clientEndpoint,
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

    public void run(SourceContext<byte[]> ctx) throws Exception {
        awsIotClient = null;

        try {
            KeyStore keyStore;
            String keyPassword;
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

            LOG.info("Connecting to AWS IoT Mqtt endpoint" + awsIotClient.getClientEndpoint());
            awsIotClient.connect();

        } catch (Exception ex) {
            LOG.error("Failed to connect to endpoint.", ex);
            isRunning = false;
        }

        // Retrieve posts
        if (awsIotClient != null) {
            LOG.debug("Subscribing to topic " + this.awsIoTMqttTopic);
            awsIotClient.subscribe(new AwsTopic(ctx, awsIoTMqttTopic, awsIoTQos));
        }

        while (isRunning) {
            Thread.yield();
        }
    }

    public void cancel() {
        isRunning = false;
        
        // we need to close the connection
        try {
            LOG.info("Disconnecting from AWS IoT Mqtt endpoint" + this.awsIotClient.getClientEndpoint());
            awsIotClient.disconnect();

        } catch (Exception ex) {
            LOG.error("Failed to disconnect from endpoint.", ex);
        }
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
