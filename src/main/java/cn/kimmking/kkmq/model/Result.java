package cn.kimmking.kkmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Result for MQServer.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/30 下午8:36
 */
@AllArgsConstructor
@Data
public class Result<T> {
    private int code; // 1==success, 0==fail
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<KKMesage<?>> msg(String msg) {
        return new Result<>(1, KKMesage.create(msg, null));
    }

    public static Result<KKMesage<?>> msg(KKMesage<?> msg) {
        return new Result<>(1, msg);
    }
}
