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
    static Map<String, Map<Integer, Entry>> mappings = new HashMap<>();

    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;
        int length;
    }

    public static void addEntry(String topic, int offset, int len) {
        System.out.println(" ===❀❀❀❀❀❀❀>>>> add entry(t/p/l):" + topic + "/" + offset + "/" + len);
        Entry value = new Entry(offset, len);
        indexes.add(topic, value);
        putMapping(topic, offset, value);
    }

    private static void putMapping(String topic, int offset, Entry value) {
        mappings.computeIfAbsent(topic, k -> new HashMap<>()).put(offset, value);
    }

    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        Map<Integer, Entry> map = mappings.get(topic);
        return map==null?null:map.get(offset);
    }


}
