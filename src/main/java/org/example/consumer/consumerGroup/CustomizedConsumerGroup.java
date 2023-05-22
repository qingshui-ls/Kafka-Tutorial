package org.example.consumer.consumerGroup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.example.consumer.common.CommonConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

// 消费者组测试
//Kafka有四种主流的分区分配策略： Range、RoundRobin、Sticky、CooperativeSticky。
//可以通过配置参数partition.assignment.strategy，修改分区的分配策略。默认策略是Range + CooperativeSticky。
// Kafka可以同时使用多个分区分配策略。
public class CustomizedConsumerGroup {
    @Test
    public void test1() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 设置消费者组
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }

    @Test
    public void test2() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 设置消费者组
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }

    @Test
    public void test3() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 设置消费者组
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }

    // 设置分区消费策略
    @Test
    public void test4() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup2");
        // 设置轮询策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        printConsumerRecords(properties);
    }


    public static void printConsumerRecords(Properties properties) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 设置消费者组
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }
}
