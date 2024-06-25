package cn.kimmking.kkmq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * mq for topic.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:54
 */
@AllArgsConstructor
public class KKMq {

    public KKMq(String topic) {
        this.topic = topic;
    }

    private String topic;
    private LinkedBlockingQueue<KKMesage> queue = new LinkedBlockingQueue();
    private List<KKListener> listeners = new ArrayList<>();

    public boolean send(KKMesage message) {
        boolean offered = queue.offer(message);
        listeners.forEach(listener -> listener.onMessage(message));
        return offered;
    }

    // 拉模式获取消息
    @SneakyThrows
    public <T> KKMesage<T> poll(long timeout)  {
        return queue.poll(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(KKListener<T> listener) {
        listeners.add(listener);
    }
}
