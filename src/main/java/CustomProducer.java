import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * @author wentao
 * &#064;date  2022-08-02  14:05
 */


public class CustomProducer {
    public static void main(String[] args) throws InterruptedException{

        /*
           普通异步发送
           在topic = first 中消费数据
           1.首先在Kafka上开启消费者
           bin/kafka-console-consumer.sh --bootstrap-server hadoop1:9092 --topic first
           2.运行代码
         */

        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");

         /*
         * 二者等价
         *  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common." +
         *                 "serialization.StringSerializer");
         * StringSerializer.class.getName() 和 org.apache.kafka.common.serialization.StringSerializer（全类名） 等价
         * 一般用前者，后者看起来比较繁琐
         * key,value 序列化（必须）：key.serializer，value.serializer
           如果出现问题需要加Class.forName
         */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 3. 创建 kafka 生产者对象
        KafkaProducer<java.lang.String, java.lang.String> kafkaProducer = new KafkaProducer<>(properties);

        // 4. 调用 send 方法,发送消息
        for (int i = 0;i < 5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","Unicorn " + i));
        }

        kafkaProducer.close();


    }
}
