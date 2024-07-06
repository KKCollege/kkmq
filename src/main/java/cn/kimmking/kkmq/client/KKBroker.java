package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.model.Result;
import cn.kimmking.kkmq.model.Stat;
import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;

/**
 * broker for topics.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:54
 */
public class KKBroker {

    @Getter
    public static KKBroker Default = new KKBroker();

    public static String bokerUrl = "http://localhost:8765/kkmq";

    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, KKConsumer<?>> consumers = getDefault().getConsumers();
            consumers.forEach((topic, consumers1) -> {
                consumers1.forEach(consumer -> {
                    Message<?> recv = consumer.recv(topic);
                    if(recv == null) return;
                    try {
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic, recv);
                    } catch (Exception ex) {
                        // TODO
                    }
                });
            });

        }, 100, 100);
    }

    public KKProducer createProducer() {
        return new KKProducer(this);
    }

    public KKConsumer<?> createConsumer(String topic) {
        KKConsumer<?> kkConsumer = new KKConsumer<>(this);
        kkConsumer.sub(topic);
        return kkConsumer;
    }

    public boolean send(String topic, Message message) {
        System.out.println(" ==>> send topic/message: " + topic + "/" + message);
        System.out.println(JSON.toJSONString(message));
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                bokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>(){});
        System.out.println(" ==>> send result: " + result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        System.out.println(" ==>> sub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(bokerUrl + "/sub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ==>> sub result: " + result);
    }

    public <T> Message<T> recv(String topic, String id) {
        System.out.println(" ==>> recv topic/id: " + topic + "/" + id);
        Result<Message<String>> result = HttpUtils.httpGet(
                bokerUrl + "/recv?t=" + topic + "&cid=" + id,
                new TypeReference<Result<Message<String>>>(){});
        System.out.println(" ==>> recv result: " + result);
        return (Message<T>) result.getData();
    }

    public void unsub(String topic, String cid) {
        System.out.println(" ==>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(bokerUrl + "/unsub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ==>> unsub result: " + result);
    }

    public boolean ack(String topic, String cid, int offset) {
        System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
        Result<String> result = HttpUtils.httpGet(
                bokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset,
                new TypeReference<Result<String>>(){});
        System.out.println(" ==>> ack result: " + result);
        return result.getCode() == 1;
    }

    @Getter
    private MultiValueMap<String, KKConsumer<?>> consumers = new LinkedMultiValueMap<>();
    public void addConsumer(String topic, KKConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }

    public Stat stat(String topic, String cid) {
        System.out.println(" ==>> stat topic/cid: " + topic + "/" + cid );
        Result<Stat> result = HttpUtils.httpGet(
                bokerUrl + "/stat?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<Stat>>(){});
        System.out.println(" ==>> stat result: " + result);
        return result.getData();
    }
}
