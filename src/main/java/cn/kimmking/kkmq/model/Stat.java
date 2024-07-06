package cn.kimmking.kkmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * stats for mq.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/7/7 上午3:13
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Stat {
    private Subscription subscription;
//    private int remaining;
    private int total;
    private int position;
}
