package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomizedConsumer {
    public static final Properties PROPERTIES = new Properties();

    private static KafkaConsumer<String, String> consumer;

    static {
        PROPERTIES.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        PROPERTIES.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        PROPERTIES.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置消费者id
        PROPERTIES.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");
        consumer = new KafkaConsumer<>(PROPERTIES);
    }

    // 订阅主题
    @Test
    public void test1() {
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        // 1.订阅主题
        consumer.subscribe(topics);
        // 2.消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }

    // 订阅特定主题对应分区
    @Test
    public void test2() {
        ArrayList<TopicPartition> partition = new ArrayList<>();
        // 1.指定消费topic中那个分区数据
        partition.add(new TopicPartition("event", 0));
        // 2.订阅分区
        consumer.assign(partition);
        // 3.消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }
}
