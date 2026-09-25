package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;

import java.util.List;
import java.util.Map;

/**
 * 检查点内容：格式版本、水位、去重表、未关闭窗口状态与已确定结果。
 */
public record CheckpointData(int formatVersion,
                             long watermark,
                             Map<String, Long> dedupEntries,
                             Map<Long, Map<String, long[]>> openWindows,
                             List<WindowResult> finalizedResults) {

    public static final int CURRENT_FORMAT_VERSION = 1;
}
