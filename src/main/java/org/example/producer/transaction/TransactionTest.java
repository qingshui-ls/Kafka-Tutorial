package org.example.producer.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.producer.common.MyTestProducer;

import java.util.Properties;

public class TransactionTest {

    public static void main(String[] args) {
        Properties properties = MyTestProducer.PROPERTIES;
        // 指定事务id ,全局唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_01");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 1 初始化事务
        producer.initTransactions();
        // 2 开启事务
        producer.beginTransaction();
//        发送数据
        try {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("event", "atguigu" + i));
            }

//            int i = 1 / 0;
            // 4 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 5 放弃事务（类似于回滚事务的操作）
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            // 关闭连接
            producer.close();
        }

        // 3 在事务内提交已经消费的偏移量（主要用于消费者）


    }
}
