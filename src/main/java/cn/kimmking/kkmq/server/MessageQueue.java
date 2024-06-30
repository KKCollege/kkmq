package cn.kimmking.kkmq.server;

import cn.kimmking.kkmq.model.Message;

import java.util.HashMap;
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
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    private Message<?>[] queue = new Message[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(Message<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public Message<?> recv(int ind) {
        if(ind <= index) return queue[ind];
        return null;
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
        if(messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if(messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, String consumerId, Message<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static Message<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    // 使用此方法，需要手工调用ack，更新订阅关系里的offset
    public static Message<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null) throw new RuntimeException("topic not found");
        if(messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            if(offset > subscription.getOffset() && offset <= messageQueue.index) {
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }



}
