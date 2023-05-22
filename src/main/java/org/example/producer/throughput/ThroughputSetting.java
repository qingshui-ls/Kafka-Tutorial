package org.example.producer.throughput;

// 生产经验——生产者如何提高吞吐量

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * 吞吐量设置的一些参数
 * • batch.size：批次大小，默认16k
 * • linger.ms：等待时间，修改为5-100ms
 * • compression.type：压缩snappy
 * • RecordAccumulator：缓冲区大小，修改为64m
 */
public class ThroughputSetting {
    public static final Properties PROPERTIES = new Properties();

    private static KafkaProducer<String, String> producer;

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

    @Test
    public void test1() {
        System.out.println(PROPERTIES);
        producer.send(new ProducerRecord<>("event", "atguigu"), (x, y) -> {
            if (y == null) System.out.println(x.topic() + "\t" + x.partition());
        });
        producer.close();
    }

}
