package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author wentao
 * @date 2022-08-03  15:00
 */

public class CustomProducerParameters {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // batch.size：批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // linger.ms：等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // compression.type：压缩，默认 none，可配置值 gzip、snappy、lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        KafkaProducer<Object, Object> kafkaProducer = new KafkaProducer<>(properties);
        int num = 5;
        for (int i = 0;i < num ;i++){
            kafkaProducer.send(new ProducerRecord<>("first","Unicorn" + i));
        }
        kafkaProducer.close();

    }

}
