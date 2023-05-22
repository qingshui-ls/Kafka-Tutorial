package org.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class FlinkKafkaProducerTest {

    public static void main(String[] args) throws Exception {
        // 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 3个分区
        env.setParallelism(3);
        ArrayList<String> words = new ArrayList<>();
        words.add("hello");
        words.add("scala");
        words.add("hello");
        words.add("flink");
        words.add("hello");
        words.add("spark");
        DataStreamSource<String> stream = env.fromCollection(words);
        // 创建kafka生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("event", new SimpleStringSchema(), properties);
        // 添加数据源 kafka生产者
        stream.addSink(producer);
        env.execute();
    }
}
