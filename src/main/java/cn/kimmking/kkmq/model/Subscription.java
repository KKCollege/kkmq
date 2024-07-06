package cn.kimmking.kkmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message Subscription.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/30 下午8:28
 */

@Data
@AllArgsConstructor
public class Subscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
