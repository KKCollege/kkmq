package cn.kimmking.kkmq.server;

import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


/**
 * MQ server.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/30 下午8:17
 */
@RestController
@RequestMapping("/kkmq")
public class MQServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody Message<String> message) {
        return Result.ok(""+MessageQueue.send(topic, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result<Message<?>> recv(@RequestParam("t") String topic,
                                   @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @RequestMapping("/batch")
    public Result<List<Message<?>>> batch(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId,
                                          @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }

    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok(""+MessageQueue.ack(topic, consumerId, offset));
    }

    // 1.sub
    @RequestMapping("/sub")
    public Result<String> sub(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result<String> unsub(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

}
