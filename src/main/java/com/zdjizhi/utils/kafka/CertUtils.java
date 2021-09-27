package com.zdjizhi.utils.kafka;

import com.zdjizhi.common.RadiusRelationshipConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.kafka
 * @Description:
 * @date 2021/9/610:37
 */
class CertUtils {
    static void chooseCert(String type, Properties properties) {
        switch (type) {
            case "SSL":
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", RadiusRelationshipConfig.TOOLS_LIBRARY + "keystore.jks");
                properties.put("ssl.keystore.password", RadiusRelationshipConfig.KAFKA_PIN);
                properties.put("ssl.truststore.location", RadiusRelationshipConfig.TOOLS_LIBRARY + "truststore.jks");
                properties.put("ssl.truststore.password", RadiusRelationshipConfig.KAFKA_PIN);
                properties.put("ssl.key.password", RadiusRelationshipConfig.KAFKA_PIN);
                break;
            case "SASL":
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + RadiusRelationshipConfig.KAFKA_USER + " password=" + RadiusRelationshipConfig.KAFKA_PIN + ";");
                break;
            default:
        }

    }
}
