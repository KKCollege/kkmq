package cn.kimmking.kkmq.client;

import cn.kimmking.kkmq.model.Message;

/**
 * message listener.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:23
 */
public interface KKListener<T> {

    void onMessage(Message<T> message);

}
