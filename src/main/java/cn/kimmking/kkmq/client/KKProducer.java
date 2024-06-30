package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.KKMesage;
import lombok.AllArgsConstructor;

/**
 * message queue producer.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:51
 */
@AllArgsConstructor
public class KKProducer {

    KKBroker broker;

    public boolean send(String topic, KKMesage message) {
        KKMq mq = broker.find(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        return mq.send(message);
    }

}
