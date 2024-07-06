package cn.kimmking.kkmq.store;

import cn.kimmking.kkmq.model.Message;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Description for this class.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/7/6 下午7:46
 */
public class MessageStore {

    MappedByteBuffer mappedByteBuffer;

    @Getter
    int len = 10240;

    @SneakyThrows
    public MappedByteBuffer init() {
        File file = new File("store001.dat");
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.toURI());
        FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        mappedByteBuffer = channel
                    .map(FileChannel.MapMode.READ_WRITE, 0, len);

        return mappedByteBuffer;
    }

    public void write(String topic, Message<String> km) {
        String message = encodeMessage(km);
        Indexer.addEntry(topic, km.getId(), mappedByteBuffer.position(), message.length());
        int pos = write(mappedByteBuffer, message);
        System.out.println("POS = " + pos);
    }

    private static String encodeMessage(Message<String> message) {
        return JSON.toJSONString(message);
    }

    private int write(MappedByteBuffer buffer, String content) {
        buffer.put(
                Charset.forName("utf-8")
                        .encode(content));
        return buffer.position();
    }

    public Message<String> read(String topic, int offset) {
        Entry entry = Indexer.getEntry(topic, offset);
        if (entry == null) {
            return null;
        }
        byte[] bytes = new byte[entry.getLength()];
        mappedByteBuffer.position(entry.getOffset());
        mappedByteBuffer.get(bytes);
        String content = new String(bytes, Charset.forName("utf-8"));
        return JSON.parseObject(content, Message.class);
    }

}
