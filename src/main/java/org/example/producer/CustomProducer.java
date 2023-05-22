package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.producer.partitioner.CustomizedPartitioner;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class CustomProducer {
    private static Properties properties = new Properties();
    private static KafkaProducer<String, String> producer;

    static {

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1.创建kafka生产者对象
        producer = new KafkaProducer<>(properties);
    }

    // 异步发送
    @Test
    public void test1() {
        // 2.发送数据
        for (int i = 0; i < 5; i++) {
            // 普通发送方法
            producer.send(new ProducerRecord<>("event", "atguigu" + i));
        }
        // 3.关闭资源
        producer.close();
    }

    // 异步发送
    @Test
    public void test2() {
        // 2.发送数据
        producer.send(new ProducerRecord<>("event", "lshaoshuai"), (x, y) -> {
            if (y == null) {
                System.out.println("发送到的主题：" + x.topic() + "\t，分区:" + x.partition());
            }
        });

        // 3.关闭资源
        producer.close();
    }

    // 同步发送producer.send(new ProducerRecord<>()).get()
    @Test
    public void test3() throws ExecutionException, InterruptedException {

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("event", "ls->" + i), (x, y) -> {
                if (y == null) {
                    System.out.println(x.topic() + "\t" + x.partition() + "\t" + x.hasOffset() + "\t" + x.hasTimestamp() + "\t" + x.timestamp() + "\t" + x.serializedKeySize());
                }
            }).get();
        }
    }

    // 自定义分区器
    @Test
    public void test4() throws ExecutionException, InterruptedException {
        // 添加分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomizedPartitioner.class.getName());
        System.out.println(properties);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        System.out.println(kafkaProducer == producer);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("event", "atguigu" + i), (x, y) -> {
                if (y == null) System.out.println(x.topic() + "\t" + x.partition());
            });
        }
    }

    @Test
    public void test6() throws ExecutionException, InterruptedException {
        // 添加分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        System.out.println(properties);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 30; i++) {
            int finalI = i;
            kafkaProducer.send(new ProducerRecord<>("event", "x" + i), (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("value= " + "x" + finalI + "\t" + "分区:" + recordMetadata.partition());
                }
            });
            Thread.sleep(500);
        }
    }
}
