package com.github.lisama569.streamwindowenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 引擎配置项。
 */
@ConfigurationProperties(prefix = "engine")
public class EngineProperties {

    /** 窗口长度（毫秒） */
    private long windowSizeMillis = 60_000;

    /** 允许迟到时长（毫秒），watermark = maxEventTime - allowedLateness */
    private long allowedLatenessMillis = 30_000;

    /** 同时打开的最大窗口数，超过则背压 */
    private int maxOpenWindows = 1024;

    /** 去重表最大条目数，超过则背压 */
    private int maxDedupEntries = 100_000;

    /** 保留的已确定窗口结果最大条数 */
    private int maxRetainedResults = 10_000;

    /** 最大来源数量（含默认来源），超过则对新来源背压 */
    private int maxSources = 4096;

    /** 标记去重表最大条目数，超过则背压 */
    private int maxMarkerEntries = 100_000;

    /** 检查点文件路径 */
    private String checkpointPath = "data/checkpoint.json";

    /** 是否要求启动时必须存在有效检查点 */
    private boolean checkpointRequired = false;

    public long getWindowSizeMillis() { return windowSizeMillis; }
    public void setWindowSizeMillis(long v) { this.windowSizeMillis = v; }
    public long getAllowedLatenessMillis() { return allowedLatenessMillis; }
    public void setAllowedLatenessMillis(long v) { this.allowedLatenessMillis = v; }
    public int getMaxOpenWindows() { return maxOpenWindows; }
    public void setMaxOpenWindows(int v) { this.maxOpenWindows = v; }
    public int getMaxDedupEntries() { return maxDedupEntries; }
    public void setMaxDedupEntries(int v) { this.maxDedupEntries = v; }
    public int getMaxRetainedResults() { return maxRetainedResults; }
    public void setMaxRetainedResults(int v) { this.maxRetainedResults = v; }
    public int getMaxSources() { return maxSources; }
    public void setMaxSources(int v) { this.maxSources = v; }
    public int getMaxMarkerEntries() { return maxMarkerEntries; }
    public void setMaxMarkerEntries(int v) { this.maxMarkerEntries = v; }
    public String getCheckpointPath() { return checkpointPath; }
    public void setCheckpointPath(String v) { this.checkpointPath = v; }
    public boolean isCheckpointRequired() { return checkpointRequired; }
    public void setCheckpointRequired(boolean v) { this.checkpointRequired = v; }
}
