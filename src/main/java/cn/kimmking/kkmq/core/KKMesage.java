package cn.kimmking.kkmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * kk message model.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午8:42
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class KKMesage<T> {
    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers; // 系统属性， X-version = 1.0
    //private Map<String, String> properties; // 业务属性
}
