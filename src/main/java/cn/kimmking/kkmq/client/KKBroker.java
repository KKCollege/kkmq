package cn.kimmking.kkmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:54
 */
public class KKBroker {

    Map<String, KKMq> mqMapping = new ConcurrentHashMap<>(64);

    public KKMq find(String topic) {
        return mqMapping.get(topic);
    }

    public KKMq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new KKMq(topic));
    }

    public KKProducer createProducer() {
        return new KKProducer(this);
    }

    public KKConsumer<?> createConsumer(String topic) {
        KKConsumer<?> kkConsumer = new KKConsumer<>(this);
        kkConsumer.subscribe(topic);
        return kkConsumer;
    }

}
