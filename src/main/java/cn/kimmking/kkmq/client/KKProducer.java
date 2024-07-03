package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;
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

    public boolean send(String topic, Message message) {
        return broker.send(topic, message);
    }

}
