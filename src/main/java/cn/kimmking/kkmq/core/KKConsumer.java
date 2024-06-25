package cn.kimmking.kkmq.core;

/**
 * message consumer.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:04
 */
public class KKConsumer<T> {

    KKBroker broker;
    String topic;
    KKMq mq;

    public KKConsumer(KKBroker broker) {
        this.broker = broker;
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
