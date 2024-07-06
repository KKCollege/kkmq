package cn.kimmking.kkmq.server;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.store.Indexer;
import cn.kimmking.kkmq.store.Store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * queues.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/30 下午8:21
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "cn.kimmking.test";
    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
//    private Message<?>[] queue = new Message[1024 * 10];
    private Store store = null;
//    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
        store = new Store(topic);
        store.init();
    }

    public static List<Message<?>> batch(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if(offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            List<Message<?>> result = new ArrayList<>();
            Message<?> recv = messageQueue.recv(next_offset);
            while(recv!=null) {
                result.add(recv);
                if(result.size() >= size) break;
                recv = messageQueue.recv(++offset);
            }
            System.out.println(" ===>> batch: topic/cid/size = " + topic + "/" + consumerId + "/" + result.size());
            System.out.println(" ===>> last message: " + recv);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public int send(Message<String> message) {
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);
        return offset;
    }

    public Message<?> recv(int offset) {
        return store.read(offset);
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ==>> sub: " + subscription);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ==>> unsub: " + subscription);
        if(messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, Message<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        System.out.println(" ==>> send: topic/message = " + topic + "/" + message);
        return messageQueue.send(message);
    }

    public static Message<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(offset);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    // 使用此方法，需要手工调用ack，更新订阅关系里的offset
    public static Message<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if(offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            Message<?> recv = messageQueue.recv(next_offset);
            System.out.println(" ===>> recv: topic/cid/ind = " + topic + "/" + consumerId + "/" + next_offset);
            System.out.println(" ===>> message: " + recv);
            return recv;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            if(offset > subscription.getOffset() && offset < Store.LEN) {
                System.out.println(" ===>> ack: topic/cid/offset = "
                        + topic + "/" + consumerId + "/" +offset);
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }



}
