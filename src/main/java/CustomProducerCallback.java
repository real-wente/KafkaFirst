import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author wentao
 * @date 2022-08-02  14:37
 */

public class CustomProducerCallback {

    /**
     * 谓带回调函数就是给一个回复的结果，发送失败或者成功
     */

    public static void main(String[] args) throws InterruptedException{

        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        //2. 给 kafka 配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");

        //key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 3. 创建 kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 4. 调用 send 方法,发送消息
        for (int i = 0;i<5;i++){
            // 添加回调
            kafkaProducer.send(
                    new ProducerRecord<>("first", "Unicron  " + i), new Callback() {

                // 该方法在 Producer 收到 ack 时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null ){
                        // 没有异常,输出信息到控制台
                        System.out.println(" 主题： " +
                                metadata.topic() + "->" + "分区：" + metadata.partition());
                    } else {
                        // 出现异常打印
                        exception.printStackTrace();
                    }
                }

            });

            // 延迟一会会看到数据发往不同分区
            Thread.sleep(2);

        }
        // 5. 关闭资源
        kafkaProducer.close();
    }
}
