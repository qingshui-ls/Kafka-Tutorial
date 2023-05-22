package org.example.flink;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9093");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaToFlinkTest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("event", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        // 关联消费者和flink流
        env.addSource(consumer, "Kafka").print();
        // 执行
        env.execute();
    }
}
