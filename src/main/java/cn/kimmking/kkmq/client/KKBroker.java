package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.model.Result;
import cn.kimmking.utils.HttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import lombok.SneakyThrows;

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
    private String brokerUrl = "http://localhost:8765/kkmq";

    public KKProducer createProducer() {
        return new KKProducer(this);
    }

    public KKConsumer<?> createConsumer(String topic) {
        KKConsumer<?> kkConsumer = new KKConsumer<>(this);
        kkConsumer.subscribe(topic);
        return kkConsumer;
    }

    private List<KKListener> listeners = new ArrayList<>();

    public boolean send(String topic, Message message) {
        String post = HttpUtils.post(JSON.toJSONString(message), brokerUrl+"/send?t=" + topic);
        System.out.println(" ===>>> send msg: " + message);
        System.out.println(" ===>>> send msg reply: " + post);
        return true;
    }

    // 拉模式获取消息
    @SneakyThrows
    public <T> Message<T> recv(String topic, String cid)  {
        //System.out.println(HttpUtils.get(brokerUrl+"/recv?t=" + topic + "&cid=" + cid));
        Result<Message<String>> message = HttpUtils.httpGet(brokerUrl+"/recv?t=" + topic + "&cid=" + cid, new TypeReference<Result<Message<String>>>(){});
        System.out.println(" ===>>> recv t/cid: " + topic + "/" + cid);
        System.out.println(" ===>>> recv msg reply: " + message);
        return (Message<T>) message.getData();
    }

    @SneakyThrows
    public String subscribe(String topic, String cid)  {
        String message = HttpUtils.get(brokerUrl+"/sub?t=" + topic + "&cid=" + cid);
        System.out.println(" ===>>> sub t/cid: " + topic + "/" + cid);
        System.out.println(" ===>>> sub msg reply: " + message);
        return message;
    }

    @SneakyThrows
    public String ack(String topic, String cid, int offset)  {
        String message = HttpUtils.get(brokerUrl+"/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset);
        System.out.println(" ===>>> ack t/cid/offset: " + topic + "/" + cid + "/" + offset);
        System.out.println(" ===>>> ack msg reply: " + message);
        return message;
    }

    @SneakyThrows
    public String unsubscribe(String topic, String cid)  {
        String message = HttpUtils.get(brokerUrl+"/unsub?t=" + topic + "&cid=" + cid);
        System.out.println(" ===>>> unsub t/cid: " + topic + "/" + cid);
        System.out.println(" ===>>> unsub msg reply: " + message);
        return message;
    }

    public <T> void addListener(KKListener<T> listener) {
        listeners.add(listener);
    }

}
