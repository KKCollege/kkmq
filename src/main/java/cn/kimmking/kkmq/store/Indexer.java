package cn.kimmking.kkmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * entry indexer.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/7/6 下午8:36
 */
public class Indexer {

    static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();
    static Map<Integer, Entry> mappings = new HashMap<>();

    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;
        int length;
    }

    public static void addEntry(String topic, int offset, int length) {
        Entry value = new Entry(offset, length);
        indexes.add(topic, value);
        mappings.put(offset, value);
    }

    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        return mappings.get(offset);
    }


}
