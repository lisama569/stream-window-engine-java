package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * 本地文件检查点存储。
 * 写入：序列化为 JSON 后计算 CRC32，连同格式版本写入临时文件，再原子重命名，避免半截文件。
 * 读取：文件不存在返回 empty；存在但无法解析、校验和不符或版本不兼容时抛
 * {@link CheckpointException}，调用方必须整体拒绝而不是部分应用。
 */
public class CheckpointStore {

    private final Path checkpointFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointStore(Path checkpointFile) {
        this.checkpointFile = checkpointFile;
    }

    public void save(CheckpointData data) {
        try {
            Files.createDirectories(checkpointFile.toAbsolutePath().getParent());
            byte[] payload = mapper.writeValueAsBytes(data);
            CRC32 crc = new CRC32();
            crc.update(payload);

            var envelope = mapper.createObjectNode();
            envelope.put("formatVersion", CheckpointData.CURRENT_FORMAT_VERSION);
            envelope.put("checksum", crc.getValue());
            envelope.put("payload", Base64.getEncoder().encodeToString(payload));

            Path tmp = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
            Files.write(tmp, mapper.writeValueAsBytes(envelope));
            Files.move(tmp, checkpointFile,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save checkpoint to " + checkpointFile, e);
        }
    }

    /** 文件不存在返回 empty；存在但损坏/版本不兼容抛 CheckpointException。 */
    public Optional<CheckpointData> load() {
        if (!Files.exists(checkpointFile)) {
            return Optional.empty();
        }
        JsonNode envelope;
        try {
            envelope = mapper.readTree(Files.readAllBytes(checkpointFile));
        } catch (IOException e) {
            throw new CheckpointException("checkpoint file is corrupted (unreadable): " + checkpointFile, e);
        }
        if (envelope == null || !envelope.hasNonNull("formatVersion")
                || !envelope.hasNonNull("checksum") || !envelope.hasNonNull("payload")) {
            throw new CheckpointException("checkpoint file is corrupted (missing fields): " + checkpointFile);
        }
        int version = envelope.get("formatVersion").asInt();
        if (version < CheckpointData.MIN_COMPATIBLE_FORMAT_VERSION
                || version > CheckpointData.CURRENT_FORMAT_VERSION) {
            throw new CheckpointException("checkpoint format version " + version
                    + " is incompatible with supported versions "
                    + CheckpointData.MIN_COMPATIBLE_FORMAT_VERSION + ".."
                    + CheckpointData.CURRENT_FORMAT_VERSION);
        }
        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(envelope.get("payload").asText());
        } catch (IllegalArgumentException e) {
            throw new CheckpointException("checkpoint file is corrupted (bad payload encoding)", e);
        }
        CRC32 crc = new CRC32();
        crc.update(payload);
        if (crc.getValue() != envelope.get("checksum").asLong()) {
            throw new CheckpointException("checkpoint checksum mismatch, refusing to apply: " + checkpointFile);
        }
        try {
            return Optional.of(migrate(mapper.readValue(payload, CheckpointData.class), version));
        } catch (IOException e) {
            throw new CheckpointException("checkpoint payload cannot be deserialized, refusing to apply", e);
        }
    }

    /**
     * 版本迁移：v1 检查点无来源状态，按清晰规则兼容——全局水位归入默认来源、
     * 标记去重表为空，恢复后与旧单路语义完全一致；v2 原样返回。
     * 迁移要么整体完成要么整体拒绝，绝不只恢复一半。
     */
    private static CheckpointData migrate(CheckpointData data, int version) {
        if (version == CheckpointData.CURRENT_FORMAT_VERSION
                && data.sourceWatermarks() != null && data.markerDedupEntries() != null) {
            return data;
        }
        Map<String, Long> sourceWatermarks = data.sourceWatermarks() != null
                ? data.sourceWatermarks()
                : Map.of(com.github.lisama569.streamwindowenginejava.core.SourceProgressTracker
                        .DEFAULT_SOURCE_ID, data.watermark());
        Map<String, Long> markerDedupEntries = data.markerDedupEntries() != null
                ? data.markerDedupEntries()
                : Map.of();
        return new CheckpointData(CheckpointData.CURRENT_FORMAT_VERSION,
                data.watermark(), data.dedupEntries(), data.openWindows(),
                data.finalizedResults(), sourceWatermarks, markerDedupEntries);
    }
}
