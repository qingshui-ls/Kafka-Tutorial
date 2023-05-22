package org.example.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.example.consumer.common.CommonConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

/**
 * 指定 Offset 消费
 * auto.offset.reset = earliest | latest | none 默认是 latest。
 * earliest：1. 自动将偏移量重置为最早的偏移量，--from-beginning。
 * 2. latest（默认值）：自动将偏移量重置为最新偏移量。
 * 3. none：如果未找到消费者组的先前偏移量，则向消费者抛出异常。
 */
public class OffsetTest {

    // 手动提交offset
    @Test
    public void test1() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Test1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
//            consumer.commitSync();
            // 手动提交offset
            consumer.commitAsync();
        }


    }

    // 指定offset消费
    @Test
    public void test2() throws InterruptedException {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test4");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);

        // 指定位置进行消费
        Set<TopicPartition> topicPartitions = consumer.assignment();
        // 保证分区分配方法已经制定完毕
        while (topicPartitions.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            topicPartitions = consumer.assignment();
        }

        for (TopicPartition topicPartition : topicPartitions) {
            consumer.seek(topicPartition, 65);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }

    // 指定时间进行消费
    @Test
    public void test3() {
        Properties properties = CommonConsumer.PROPERTIES;
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test5");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event");
        consumer.subscribe(topics);
        Set<TopicPartition> partitionSet = consumer.assignment();
        // 保证分区分配方法已经制定完毕
        while (partitionSet.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            partitionSet = consumer.assignment();
        }
        // 把时间转成对应的offset
        HashMap<TopicPartition, Long> map = new HashMap<>();
        partitionSet.forEach(x -> map.put(x, System.currentTimeMillis() - (24 * 3600 * 1000)));
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(map);

        for (TopicPartition t : partitionSet) {
            consumer.seek(t, topicPartitionOffsetAndTimestampMap.get(t).offset());
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(System.out::println);
        }
    }
}

