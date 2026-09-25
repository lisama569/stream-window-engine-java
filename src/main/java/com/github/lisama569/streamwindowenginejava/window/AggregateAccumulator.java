package com.github.lisama569.streamwindowenginejava.window;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 单窗口聚合状态：计数、求和与去重依据（eventId 集合）。
 */
public class AggregateAccumulator {

    private long count;
    private long sum;
    private final Set<String> eventIds = new LinkedHashSet<>();

    public AggregateAccumulator() {
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public Set<String> getEventIds() {
        return eventIds;
    }

    /**
     * 计入一条事件。eventId 集合同时充当窗口内的去重依据：
     * 重复 eventId 不会改变 count/sum。
     *
     * @return true 表示是新事件，false 表示窗口内重复
     */
    public boolean add(String eventId, long value) {
        if (!eventIds.add(eventId)) {
            return false;
        }
        count++;
        sum += value;
        return true;
    }
}
