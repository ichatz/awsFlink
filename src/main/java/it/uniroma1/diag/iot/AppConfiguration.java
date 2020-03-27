package it.uniroma1.diag.iot;

import com.amazonaws.services.iot.client.AWSIotQos;

/**
 * Central point for setting the configuration parameters
 *
 * @author ichatz@diag.uniroma1.it
 */
public interface AppConfiguration {

    public final String brokerHost = "a367uuo2nuj55s-ats.iot.eu-west-1.amazonaws.com";

    public final int brokerPort = 8883;

    public final String brokerProtocol = "ssl";

    public final String brokerURL = brokerProtocol + "://" + brokerHost + ":" + brokerPort;

    public final String topic = "station";

    public final AWSIotQos qos = AWSIotQos.QOS0;

    public final String outExchange = "flink";

    public final String certificateFile = "/home/ichatz/Local/coap-mqtt/9a7a4e5830-certificate.pem.crt";

    public final String privateKeyFile = "/home/ichatz/Local/coap-mqtt/9a7a4e5830-private.pem.key";

}
