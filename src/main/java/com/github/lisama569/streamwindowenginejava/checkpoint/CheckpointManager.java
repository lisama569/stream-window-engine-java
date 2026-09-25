package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.Base64;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * 检查点管理器：本地文件原子写入（临时文件 + rename），
 * 加载时校验 magic、格式版本与 CRC32，任何异常都明确拒绝，绝不部分应用。
 */
@Component
public class CheckpointManager {

    public static final int FORMAT_VERSION = 1;
    public static final String MAGIC = "SWE-CHKPT";

    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String FILE_NAME = "checkpoint.json";

    private final EngineProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointManager(EngineProperties properties) {
        this.properties = properties;
    }

    private Path checkpointFile() {
        return Path.of(properties.getCheckpointDir()).resolve(FILE_NAME);
    }

    /** 原子写入检查点：先写临时文件，再原子 rename，避免半截文件被当作有效检查点 */
    public void save(CheckpointData data) {
        try {
            Path dir = Path.of(properties.getCheckpointDir());
            Files.createDirectories(dir);
            byte[] payload = mapper.writeValueAsBytes(data);
            CRC32 crc = new CRC32();
            crc.update(payload);
            var envelope = mapper.createObjectNode();
            envelope.put("magic", MAGIC);
            envelope.put("version", FORMAT_VERSION);
            envelope.put("crc32", crc.getValue());
            envelope.put("payload", Base64.getEncoder().encodeToString(payload));
            byte[] bytes = mapper.writeValueAsBytes(envelope);

            Path tmp = Files.createTempFile(dir, "checkpoint-", ".tmp");
            Files.write(tmp, bytes, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, checkpointFile(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.debug("checkpoint saved: watermark={}", data.getWatermark());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save checkpoint", e);
        }
    }

    /**
     * 加载最近一次有效检查点。
     * 文件不存在返回 empty；损坏抛 {@link CheckpointCorruptedException}；
     * 版本不兼容抛 {@link CheckpointVersionException}。绝不部分应用。
     */
    public Optional<CheckpointData> load() {
        Path file = checkpointFile();
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new CheckpointCorruptedException("cannot read checkpoint file: " + file, e);
        }
        JsonNode envelope;
        try {
            envelope = mapper.readTree(bytes);
        } catch (IOException e) {
            throw new CheckpointCorruptedException("checkpoint file is not valid JSON: " + file, e);
        }
        if (envelope == null || !envelope.hasNonNull("magic") || !MAGIC.equals(envelope.get("magic").asText())) {
            throw new CheckpointCorruptedException("checkpoint magic mismatch, refusing to apply: " + file);
        }
        int version = envelope.path("version").asInt(-1);
        if (version != FORMAT_VERSION) {
            throw new CheckpointVersionException(
                    "checkpoint format version " + version + " is not supported by engine version " + FORMAT_VERSION);
        }
        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(envelope.path("payload").asText(""));
        } catch (IllegalArgumentException e) {
            throw new CheckpointCorruptedException("checkpoint payload is not valid base64", e);
        }
        CRC32 crc = new CRC32();
        crc.update(payload);
        if (crc.getValue() != envelope.path("crc32").asLong(-1)) {
            throw new CheckpointCorruptedException("checkpoint CRC32 mismatch, file is corrupted: " + file);
        }
        try {
            CheckpointData data = mapper.readValue(payload, CheckpointData.class);
            log.info("checkpoint loaded: watermark={}, activeKeys={}, finalizedResults={}, seenEventIds={}",
                    data.getWatermark(), data.getActiveWindows().size(),
                    data.getFinalizedResults().size(), data.getSeenEventIds().size());
            return Optional.of(data);
        } catch (IOException e) {
            throw new CheckpointCorruptedException("checkpoint payload cannot be deserialized", e);
        }
    }
}
