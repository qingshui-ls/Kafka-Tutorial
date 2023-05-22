package org.example.producer.Ack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 可靠性总结：
 * acks=0，生产者发送过来数据就不管了，可靠性差，效率高；
 * acks=1，生产者发送过来数据Leader应答，可靠性中等，效率中等；
 * acks=-1，生产者发送过来数据Leader和ISR队列里面所有Follwer应答，可靠性高，效率低；
 * 在生产环境中，acks=0很少使用；acks=1，一般用于传输普通日志，允许丢个别数据；acks=-1，一般用于传输和钱相关的数据，
 * 对可靠性要求比较高的场景
 * <p>
 * 至少一次（At Least Once）= ACK级别设置为-1 + 分区副本大于等于2 + ISR里应答的最小副本数量大于等于2
 * • 最多一次（At Most Once）= ACK级别设置为0
 * •
 * At Least Once可以保证数据不丢失，但是不能保证数据不重复；ack= -1
 * At Most Once可以保证数据不重复，但是不能保证数据不丢失。ack= 0
 * • 精确一次（Exactly Once）：对于一些非常重要的信息，比如和钱相关的数据，要求数据既不能重复也不丢失。
 * Kafka 0.11版本以后，引入了一项重大特性：幂等性和事务。
 *
 * 精确一次（Exactly Once） = 幂等性 + 至少一次（ ack=-1 + 分区副本数>=2 + ISR最小副本数量>=2） 。
 * 重复数据的判断标准：具有<PID, Partition, SeqNumber>相同主键的消息提交时，Broker只会持久化一条。其
 * 中PID是Kafka每次重启都会分配一个新的；Partition 表示分区号；Sequence Number是单调自增的。
 * 所以幂等性只能保证的是在单分区单会话内不重复。
 */
public class AckTest {
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
        // 可靠性ACK配置
        PROPERTIES.put(ProducerConfig.ACKS_CONFIG, "1");
        // 重试次数
        PROPERTIES.put(ProducerConfig.RETRIES_CONFIG, 3);
        producer = new KafkaProducer<>(PROPERTIES);
    }

    public static void main(String[] args) {
        System.out.println(PROPERTIES);
        producer.send(new ProducerRecord<>("event", "atguigu"), (x, y) -> {
            if (y == null) System.out.println(x.topic() + "\t" + x.partition());
        });
        producer.close();
    }
}
