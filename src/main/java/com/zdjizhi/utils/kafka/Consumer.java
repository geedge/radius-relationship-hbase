package com.zdjizhi.utils.kafka;

import com.zdjizhi.common.RadiusRelationshipConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.kafka
 * @Description:
 * @date 2021/6/813:54
 */
public class Consumer {
    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", RadiusRelationshipConfig.INPUT_KAFKA_SERVERS);
        properties.put("group.id", RadiusRelationshipConfig.GROUP_ID);
        properties.put("session.timeout.ms", RadiusRelationshipConfig.SESSION_TIMEOUT_MS);
        properties.put("max.poll.records", RadiusRelationshipConfig.MAX_POLL_RECORDS);
        properties.put("max.partition.fetch.bytes", RadiusRelationshipConfig.MAX_PARTITION_FETCH_BYTES);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        CertUtils.chooseCert(RadiusRelationshipConfig.INPUT_KAFKA_SERVERS, properties);

        return properties;
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer() {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(RadiusRelationshipConfig.INPUT_KAFKA_TOPIC,
                new SimpleStringSchema(), createConsumerConfig());

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
}
