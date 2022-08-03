import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author wentao
 * @date 2022-08-03  09:41
 */

public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //获取数据,转化成string
        String msgValues = value.toString();
        int partition;
        // 魔法值不允许使用未经定义的常量,new string的方法较为冗余
        String cmName = "Unicorn";
        if (msgValues.contains(cmName)){
            partition = 0;
        }else {
            partition = 1;
        }
        return partition;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
