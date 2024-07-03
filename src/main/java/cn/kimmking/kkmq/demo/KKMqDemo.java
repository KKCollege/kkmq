package cn.kimmking.kkmq.demo;

import cn.kimmking.kkmq.client.KKBroker;
import cn.kimmking.kkmq.client.KKConsumer;
import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.client.KKProducer;
import lombok.SneakyThrows;

import static cn.kimmking.kkmq.model.Message.HEADER_KEY_OFFSET;

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
        KKBroker broker = new KKBroker();

        KKProducer producer = broker.createProducer();
//        KKConsumer<?> consumer = broker.createConsumer(topic);
//        consumer.subscribe(topic);
//        consumer.listen(message -> {
//            System.out.println(" onMessage => " + message);
//        });


        for (int i = 0; i < 1; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<String>((long) ids ++, Message.json(order), null));
        }

        KKConsumer<?> consumer1 = broker.createConsumer(topic);
        consumer1.subscribe(topic);

        for (int i = 0; i < 1; i++) {
            Message<String> message = consumer1.recv(topic);
            System.out.println("recv and ack for msg: " + message);
            consumer1.ack(topic, message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids ++, Message.json(order), null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') {
                Message<String> message = consumer1.recv(topic);
                System.out.println("poll ok => " + message);
                consumer1.ack(topic, message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>((long) ids ++, Message.json(order), null));
                }
                System.out.println("send 10 orders...");
            }
        }

    }

}
