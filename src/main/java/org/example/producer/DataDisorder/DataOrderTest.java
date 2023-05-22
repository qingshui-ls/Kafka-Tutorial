package org.example.producer.DataDisorder;

/***
 * 解决数据乱序1）
 * kafka在1.x版本之前保证数据单分区有序，条件如下：
 * max.in.flight.requests.per.connection=1（不需要考虑是否开启幂等性）。
 * 2）kafka在1.x及以后版本保证数据单分区有序，条件如下：
 * （2）开启幂等性
 * max.in.flight.requests.per.connection需要设置小于等于5。
 * （1）未开启幂等性
 * max.in.flight.requests.per.connection需要设置为1。
 * 原因说明：因为在kafka1.x以后，启用幂等后，kafka服务端会缓存producer发来的最近5个request的元数据，
 * 故无论如何，都可以保证最近5个request的数据都是有序的。
 */
public class DataOrderTest {
}
