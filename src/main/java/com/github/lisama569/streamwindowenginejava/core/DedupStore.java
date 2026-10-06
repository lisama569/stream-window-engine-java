package com.github.lisama569.streamwindowenginejava.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 去重存储，记录 去重键 -> 事件时间/进度时间。
 * 键由调用方（WindowEngine）加上域前缀：事件 "E:<eventId>"、标记 "M:<markerId>"，
 * 两个命名空间互不相交，事件与标记各自独立去重。
 * 条目随水位推进回收：事件时间 <= 水位的事件若再次到达，会被引擎判为 LATE_REJECTED，
 * 不可能被重复计入，因此其去重条目可以安全回收，内存不随事件总数无界增长。
 * 容量达到上限且回收后仍不足时，register 返回 false，由引擎施加背压。
 */
public class DedupStore {

    private final int maxEntries;
    private final Map<String, Long> entries = new HashMap<>();

    public DedupStore(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0");
        }
        this.maxEntries = maxEntries;
    }

    /** @return true 表示该 eventId 已登记过（重复事件） */
    public boolean isDuplicate(String eventId, long eventTimeMillis) {
        return entries.containsKey(eventId);
    }

    /**
     * 登记一个已接受事件。
     *
     * @return false 表示容量已满（背压），事件未被登记
     */
    public boolean register(String eventId, long eventTimeMillis) {
        if (entries.containsKey(eventId)) {
            return true;
        }
        if (entries.size() >= maxEntries) {
            return false;
        }
        entries.put(eventId, eventTimeMillis);
        return true;
    }

    /** 回收事件时间 <= watermark 的条目，返回回收条数。 */
    public int pruneUpTo(long watermark) {
        int removed = 0;
        Iterator<Map.Entry<String, Long>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue() <= watermark) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    /** @return true 表示容量已满，再登记新条目将失败（背压预检） */
    public boolean isFull() {
        return entries.size() >= maxEntries;
    }

    public int size() {
        return entries.size();
    }

    public Map<String, Long> snapshot() {
        return Map.copyOf(entries);
    }

    public void restore(Map<String, Long> restored) {
        entries.clear();
        entries.putAll(restored);
    }
}
