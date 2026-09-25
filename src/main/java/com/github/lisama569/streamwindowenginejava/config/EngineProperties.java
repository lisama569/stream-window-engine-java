package com.github.lisama569.streamwindowenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 引擎配置项。详见 README。
 */
@ConfigurationProperties(prefix = "stream-engine")
public class EngineProperties {

    /** 滚动窗口大小（毫秒） */
    private long windowSizeMs = 60_000;

    /** 允许迟到时长（毫秒）：水位越过 windowEnd + allowedLatenessMs 后窗口状态被清理 */
    private long allowedLatenessMs = 30_000;

    /** 活跃 + 宽限窗口的最大数量，达到上限施加背压 */
    private int maxBufferedWindows = 10_000;

    /** 已见 eventId 去重索引的最大容量，达到上限施加背压 */
    private int maxSeenEventIds = 1_000_000;

    /** 已归档（已定版）窗口结果的最大保留数量，超出后淘汰最老窗口 */
    private int maxStoredResults = 10_000;

    /** 死信日志（超迟到事件）的最大容量，超出后丢弃最老记录并计数 */
    private int maxDeadLetters = 10_000;

    /** 检查点文件目录 */
    private String checkpointDir = "data/checkpoints";

    /** 启动时是否要求必须存在检查点（严格模式） */
    private boolean checkpointRequiredOnStartup = false;

    /** 每次成功摄取后是否立即落检查点（默认 true，保证重启后不重复计数） */
    private boolean checkpointOnIngest = true;

    public long getWindowSizeMs() {
        return windowSizeMs;
    }

    public void setWindowSizeMs(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    public long getAllowedLatenessMs() {
        return allowedLatenessMs;
    }

    public void setAllowedLatenessMs(long allowedLatenessMs) {
        this.allowedLatenessMs = allowedLatenessMs;
    }

    public int getMaxBufferedWindows() {
        return maxBufferedWindows;
    }

    public void setMaxBufferedWindows(int maxBufferedWindows) {
        this.maxBufferedWindows = maxBufferedWindows;
    }

    public int getMaxSeenEventIds() {
        return maxSeenEventIds;
    }

    public void setMaxSeenEventIds(int maxSeenEventIds) {
        this.maxSeenEventIds = maxSeenEventIds;
    }

    public int getMaxStoredResults() {
        return maxStoredResults;
    }

    public void setMaxStoredResults(int maxStoredResults) {
        this.maxStoredResults = maxStoredResults;
    }

    public int getMaxDeadLetters() {
        return maxDeadLetters;
    }

    public void setMaxDeadLetters(int maxDeadLetters) {
        this.maxDeadLetters = maxDeadLetters;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public void setCheckpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    public boolean isCheckpointRequiredOnStartup() {
        return checkpointRequiredOnStartup;
    }

    public void setCheckpointRequiredOnStartup(boolean checkpointRequiredOnStartup) {
        this.checkpointRequiredOnStartup = checkpointRequiredOnStartup;
    }

    public boolean isCheckpointOnIngest() {
        return checkpointOnIngest;
    }

    public void setCheckpointOnIngest(boolean checkpointOnIngest) {
        this.checkpointOnIngest = checkpointOnIngest;
    }
}
