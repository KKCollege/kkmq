package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;

import java.util.concurrent.atomic.AtomicInteger;

import static cn.kimmking.kkmq.model.Message.HEADER_KEY_OFFSET;

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

    static AtomicInteger idgen = new AtomicInteger(0);

    public KKConsumer(KKBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public String subscribe(String topic) {
        return broker.subscribe(topic, id);
    }

    public String unsubscribe(String topic) {
        return broker.unsubscribe(topic, id);
    }

    public <T> Message<T> recv(String topic)  {
        return broker.recv(topic, id);
    }

    public String ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public String ack(String topic, Message<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get(HEADER_KEY_OFFSET));
        return broker.ack(topic, id, offset);
    }

    public void listen(KKListener<T> listener) {
        broker.addListener(listener);
    }

}
