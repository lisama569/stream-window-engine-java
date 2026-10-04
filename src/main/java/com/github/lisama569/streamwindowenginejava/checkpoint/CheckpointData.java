package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;

import java.util.List;
import java.util.Map;

/**
 * 检查点内容：格式版本、全局水位、事件去重表、未关闭窗口状态、已确定结果，
 * 以及多来源能力引入后的各来源水位与标记去重表。
 *
 * 兼容规则：格式版本 1（无来源状态）的检查点按清晰规则迁移——
 * 全局水位归入默认来源，标记去重表为空，恢复后与旧单路语义完全一致；
 * 其它不兼容版本明确拒绝，不做部分应用。
 */
public record CheckpointData(int formatVersion,
                             long watermark,
                             Map<String, Long> dedupEntries,
                             Map<Long, Map<String, long[]>> openWindows,
                             List<WindowResult> finalizedResults,
                             Map<String, Long> sourceWatermarks,
                             Map<String, Long> markerDedupEntries) {

    /** 当前格式版本：2 = 含来源水位与标记去重表。 */
    public static final int CURRENT_FORMAT_VERSION = 2;

    /** 可兼容迁移的最低格式版本：1 = 仅单路水位，无来源状态。 */
    public static final int MIN_COMPATIBLE_FORMAT_VERSION = 1;
}
