package cn.kimmking.kkmq.demo;

import cn.kimmking.kkmq.client.KKBroker;
import cn.kimmking.kkmq.client.KKConsumer;
import cn.kimmking.kkmq.model.Message;
import cn.kimmking.kkmq.client.KKProducer;
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

        String topic = "kk.order";
        KKBroker broker = new KKBroker();
        broker.createTopic(topic);

        KKProducer producer = broker.createProducer();
        KKConsumer<?> consumer = broker.createConsumer(topic);
        consumer.subscribe(topic);
        consumer.listen(message -> {
            System.out.println(" onMessage => " + message);
        });


        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>((long) ids ++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            Message<Order> message = (Message<Order>) consumer.poll(1000);
            System.out.println(message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids ++, order, null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') {
                Message<Order> message = (Message<Order>) consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>((long) ids ++, order, null));
                }
                System.out.println("send 10 orders...");
            }
        }

    }

}
