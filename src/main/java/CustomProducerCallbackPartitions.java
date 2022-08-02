import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author wentao
 * @date 2022-08-02  14:37
 */

public class CustomProducerCallbackPartitions {

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
        int num = 5;
        for (int i = 0;i<num;i++){
            // 添加回调
            kafkaProducer.send(
                    /*
                    指定分区partition或者key
                    key a -> 1 partition
                        b -> 2 partition
                        c -> 1 partition
                        d -> 2 partition
                        f -> 0 partition
                        通过key的hashcode值
                        如果都不指定就按照粘性来随机分区
                     */

                    new ProducerRecord<>("first","f","Unicorn" + i), new Callback() {

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
