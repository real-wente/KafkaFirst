package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;


/**
 * @author wentao
 * &#064;date  2022-08-02  14:05
 */


public class CustomConsumer2 {
    /**
     * 独立消费者案例（订阅主题）
     */
    public static void main(String[] args) throws InterruptedException{
        // 1. 创建消费者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");

         /*
         * 二者等价
         *  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common." +
         *                 "serialization.StringSerializer");
         * StringSerializer.class.getName() 和 org.apache.kafka.common.serialization.StringSerializer（全类名） 等价
         * 一般用前者，后者看起来比较繁琐
         * key,value 序列化（必须）：key.serializer，value.serializer
           如果出现问题需要加Class.forName
         */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // 配置消费者组（组名任意起名） 必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 3. 创建 kafka 消费者者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 注册要消费的主题（可以消费多个主题）
        ArrayList<String> topic = new ArrayList<>();
        topic.add("first");
        kafkaConsumer.subscribe(topic);

        while (true){
            // 设置 1s 中消费一批数据
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            // 打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
