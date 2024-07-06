package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.model.Stat;
import lombok.Getter;

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

    static AtomicInteger idgen = new AtomicInteger(0);

    public KKConsumer(KKBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic) {
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
        broker.unsub(topic, id);
    }

    public Message<T> recv(String topic) {
        return broker.recv(topic, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, Message<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic, KKListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    @Getter
    private KKListener listener;

    public Stat stat(String topic) {
        return broker.stat(topic, id);
    }
}
