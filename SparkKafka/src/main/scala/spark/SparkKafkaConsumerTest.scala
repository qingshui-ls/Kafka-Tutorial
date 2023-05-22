package spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkKafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    // 上下文环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka")
    val sc = new StreamingContext(conf, Seconds(3))
    // 消费数据
    val properties = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkConsumer"
    )

    val kafkaDStream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("event"), properties))

    val valueDStream = kafkaDStream.map(x => x.value())
    valueDStream.print()

    // 执行代码并阻塞
    sc.start()
    sc.awaitTermination()

  }
}
