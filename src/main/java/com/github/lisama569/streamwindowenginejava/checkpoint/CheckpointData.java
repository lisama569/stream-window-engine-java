package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;

import java.util.List;
import java.util.Map;

/**
 * 检查点内容：格式版本、水位、事件去重表、标记去重表、未关闭窗口状态、
 * 已确定结果与来源进度表。
 *
 * 兼容规则：
 * - 格式版本 1：没有 sourceProgress / markerDedupEntries 字段，反序列化得到 null。
 *   sourceProgress 为 null 表示“无来源进度信息（等价于单路来源）”，恢复时以
 *   watermark + allowedLateness 重建默认来源进度，语义与旧版一致；
 * - 格式版本 2：有 sourceProgress，但标记去重条目以 "M:" 前缀混在 dedupEntries
 *   里，恢复时按前缀拆分进独立的标记去重表；
 * - 格式版本 3：事件去重（dedupEntries）与标记去重（markerDedupEntries）
 *   各自独立成表，互不干扰；
 * - 版本 2 起 sourceProgress 一定非 null（可能为空表，表示快照时尚未注册任何
 *   来源，恢复后不虚构任何来源）。其它不兼容版本整体拒绝。
 */
public record CheckpointData(int formatVersion,
                             long watermark,
                             Map<String, Long> dedupEntries,
                             Map<Long, Map<String, long[]>> openWindows,
                             List<WindowResult> finalizedResults,
                             Map<String, Long> sourceProgress,
                             Map<String, Long> markerDedupEntries) {

    /**
     * 版本 1：无来源进度表、无独立标记去重表；
     * 版本 2：含来源进度表，标记去重条目以 "M:" 前缀混在 dedupEntries；
     * 版本 3：事件与标记去重分表。加载时三个版本均可接受。
     */
    public static final int CURRENT_FORMAT_VERSION = 3;
    public static final int MIN_SUPPORTED_FORMAT_VERSION = 1;

    public CheckpointData {
        // 保留 null 与空表的区别：null 表示 v1 旧格式（无来源信息），
        // 空表表示 v2+ 快照时确实没有任何来源，两者恢复语义不同，不能归一
        if (sourceProgress != null) {
            sourceProgress = Map.copyOf(sourceProgress);
        }
        // v1/v2 没有独立的标记去重表，反序列化得到 null，按空表处理；
        // v2 混在 dedupEntries 里的 "M:" 前缀条目由引擎恢复时拆分
        markerDedupEntries = markerDedupEntries == null
                ? Map.of() : Map.copyOf(markerDedupEntries);
    }
}
