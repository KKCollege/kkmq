package cn.kimmking.kkmq.model;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * kk message model.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:42
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Message<T> {

    public static final String HEADER_KEY_OFFSET = "X-offset";

    //private String topic;
    static AtomicLong idgen = new AtomicLong(0);
    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); // 系统属性， X-version = 1.0
    //private Map<String, String> properties; // 业务属性

    public static long getId() {
        return idgen.getAndIncrement();
    }

    public static Message<?> create(String body, Map<String, String> headers) {
        return new Message<>(getId(), body, headers);
    }

    public static final String json(Object obj) {
        return JSON.toJSONString(obj);
    }

}
