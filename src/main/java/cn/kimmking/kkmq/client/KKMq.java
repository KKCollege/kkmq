package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.utils.HttpUtils;
import com.alibaba.fastjson.JSON;
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

//    public KKMq(String topic) {
//        this.topic = topic;
//    }
//
//    private String topic;
//    private LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue();
//    private List<KKListener> listeners = new ArrayList<>();
//
//    public boolean send(Message message) {
////        boolean offered = queue.offer(message);
////        listeners.forEach(listener -> listener.onMessage(message));
////        return offered;
//
//        String post = HttpUtils.post("http://localhost:8080/kkmq/send?t=" + topic, JSON.toJSONString(message));
//        System.out.println(" ===>>> send msg: " + message);
//
//        return true;
//    }
//
//    // 拉模式获取消息
//    @SneakyThrows
//    public <T> Message<T> poll(long timeout)  {
//        return queue.poll(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
//    }
//
//    public <T> void addListener(KKListener<T> listener) {
//        listeners.add(listener);
//    }
}
