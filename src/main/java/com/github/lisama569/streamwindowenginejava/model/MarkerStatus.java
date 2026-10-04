package com.github.lisama569.streamwindowenginejava.model;

/**
 * 标记处理结论，互相可区分：
 * APPLIED     - 标记已生效，来源进度推进（可能触发窗口确定）
 * DUPLICATE   - 与已处理标记 markerId 重复，未重复推进
 * STALE       - 标记进度不领先于该来源已知进度（过期或互相矛盾），未生效
 * BACKPRESSURE- 来源数量达到上限，标记未生效，调用方应稍后重试
 */
public enum MarkerStatus {
    APPLIED,
    DUPLICATE,
    STALE,
    BACKPRESSURE
}
