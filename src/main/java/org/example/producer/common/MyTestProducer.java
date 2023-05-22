package org.example.producer.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyTestProducer {
    public static final Properties PROPERTIES = new Properties();

    public static KafkaProducer<String, String> producer;

    static {
        PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        PROPERTIES.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        PROPERTIES.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        PROPERTIES.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producer = new KafkaProducer<>(PROPERTIES);
    }
}
