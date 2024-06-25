package cn.kimmking.kkmq.core;

/**
 * message listener.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:23
 */
public interface KKListener<T> {

    void onMessage(KKMesage<T> message);

}