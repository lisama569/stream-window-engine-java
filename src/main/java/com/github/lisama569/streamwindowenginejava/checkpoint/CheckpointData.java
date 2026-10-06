package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;

import java.util.List;
import java.util.Map;

/**
 * 检查点内容：格式版本、水位、去重表、未关闭窗口状态、已确定结果与来源进度表。
 *
 * 兼容规则（按格式版本严格区分，详见各版本常量）：
 * - v1：无 sourceProgress 字段，反序列化得到 null 按空表处理，恢复时以
 *   watermark + allowedLateness 重建默认来源进度，语义与旧版单路一致；
 * - v2：含来源进度表；空表是合法的空跑检查点，如实恢复，不套用 v1 的单路重建；
 * - v3：去重表键带域前缀（事件 "E:"、标记 "M:"）；v1/v2 的原始 eventId 条目
 *   在恢复时迁移为 "E:" 前缀形式；
 * - 其它不兼容版本整体拒绝，不做部分应用。
 */
public record CheckpointData(int formatVersion,
                             long watermark,
                             Map<String, Long> dedupEntries,
                             Map<Long, Map<String, long[]>> openWindows,
                             List<WindowResult> finalizedResults,
                             Map<String, Long> sourceProgress) {

    /**
     * 版本 1：无来源进度表，去重表为原始 eventId；
     * 版本 2：含来源进度表，去重表为原始 eventId + "M:" 前缀的标记条目；
     * 版本 3：去重表键全部带域前缀（事件 "E:"、标记 "M:"），两个命名空间互不相交。
     * 加载时三个版本均可接受，v1/v2 的去重键在恢复时迁移为带域前缀形式。
     */
    public static final int CURRENT_FORMAT_VERSION = 3;
    public static final int MIN_SUPPORTED_FORMAT_VERSION = 1;
    /** 自该版本起检查点携带来源进度表（v1 没有，恢复时按单路语义兼容重建）。 */
    public static final int SOURCE_PROGRESS_SINCE_VERSION = 2;
    /** 自该版本起去重表键带域前缀（之前的事件条目为原始 eventId，恢复时需迁移）。 */
    public static final int SCOPED_DEDUP_KEYS_SINCE_VERSION = 3;

    public CheckpointData {
        // 旧版本（v1）数据没有该字段，反序列化得到 null，按空表处理
        sourceProgress = sourceProgress == null ? Map.of() : Map.copyOf(sourceProgress);
    }
}
