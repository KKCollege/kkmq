package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.KKMesage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:04
 */
public class KKConsumer<T> {

    private String id;
    KKBroker broker;
    String topic;
    KKMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public KKConsumer(KKBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(topic);
        if(mq == null) throw new RuntimeException("topic not found");
    }

    public KKMesage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public void listen(KKListener<T> listener) {
        mq.addListener(listener);
    }

}
