package cn.kimmking.kkmq.model;

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
    //private String topic;
    static AtomicLong idgen = new AtomicLong(0);
    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); // 系统属性， X-version = 1.0
    //private Map<String, String> properties; // 业务属性

    public static long nextId() {
        return idgen.getAndIncrement();
    }

    public static Message<String> create(String body, Map<String, String> headers) {
        return new Message<>(nextId(), body, headers);
    }
}
