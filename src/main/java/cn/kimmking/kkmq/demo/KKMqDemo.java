package cn.kimmking.kkmq.demo;

import cn.kimmking.kkmq.client.KKBroker;
import cn.kimmking.kkmq.client.KKConsumer;
import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.client.KKProducer;
import cn.kimmking.kkmq.model.Stat;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

/**
 * mq demo for order.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:10
 */
public class KKMqDemo {

    @SneakyThrows
    public static void main(String[] args) {

        long ids = 0;

        String topic = "cn.kimmking.test";
        KKBroker broker = KKBroker.getDefault();

        KKProducer producer = broker.createProducer();
//        KKConsumer<?> consumer = broker.createConsumer(topic);
//        consumer.listen(topic, message -> {
//            System.out.println(" onMessage => " + message); // 这里处理消息
//        });

       KKConsumer<?> consumer1 = broker.createConsumer(topic);

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>((long) ids ++, JSON.toJSONString(order), null));
        }

        for (int i = 0; i < 10; i++) {
            Message<String> message = (Message<String>) consumer1.recv(topic);
            System.out.println(message); // 做业务处理。。。。
            consumer1.ack(topic, message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
               // consumer1.unsub(topic);s
                System.out.println(" [exit] : " + c );
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids ++, JSON.toJSONString(order), null));
                System.out.println("produce ok => " + order);
            }
            if (c == 'c') {
                Message<String> message = (Message<String>) consumer1.recv(topic);
                System.out.println("consume ok => " + message);
                consumer1.ack(topic, message);
            }
            if (c == 's') {
                Stat stat = consumer1.stat(topic);
                System.out.println(stat);
            }
            if (c == 'b') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>((long) ids ++, JSON.toJSONString(order), null));
                }
                System.out.println("batch produce 10 orders...");
            }
        }

        System.exit(1);

    }

}
