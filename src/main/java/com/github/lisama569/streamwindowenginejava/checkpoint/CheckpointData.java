package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;

import java.util.List;
import java.util.Map;

/**
 * 检查点内容：格式版本、水位、去重表、未关闭窗口状态、已确定结果与来源进度表。
 *
 * 兼容规则：格式版本 1 的数据没有 sourceProgress 字段，反序列化时按
 * “无来源进度信息（等价于单路来源）”处理，恢复时以 watermark + allowedLateness
 * 重建默认来源进度，语义与旧版一致；其它不兼容版本整体拒绝。
 */
public record CheckpointData(int formatVersion,
                             long watermark,
                             Map<String, Long> dedupEntries,
                             Map<Long, Map<String, long[]>> openWindows,
                             List<WindowResult> finalizedResults,
                             Map<String, Long> sourceProgress) {

    /** 版本 1：无来源进度表；版本 2：含来源进度表。加载时两个版本均可接受。 */
    public static final int CURRENT_FORMAT_VERSION = 2;
    public static final int MIN_SUPPORTED_FORMAT_VERSION = 1;

    public CheckpointData {
        // 旧版本（v1）数据没有该字段，反序列化得到 null，按空表处理
        sourceProgress = sourceProgress == null ? Map.of() : Map.copyOf(sourceProgress);
    }
}
