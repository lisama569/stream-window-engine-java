package com.github.lisama569.streamwindowenginejava.model;

/**
 * 标记处理结论，互相可区分：
 * ACCEPTED     - 标记已生效，来源进度前沿推进
 * DUPLICATE    - 与已处理标记 markerId 重复，未重复生效
 * STALE        - 过期/矛盾标记：进度时间点不超过该来源已知进度，不生效（不静默应用）
 * BACKPRESSURE - 来源数或标记去重表达到上限，标记未生效，调用方应稍后重试
 */
public enum MarkerStatus {
    ACCEPTED,
    DUPLICATE,
    STALE,
    BACKPRESSURE
}
