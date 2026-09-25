package com.github.lisama569.streamwindowenginejava.checkpoint;

/** 检查点文件存在但无法解析或校验和错误：明确拒绝，绝不部分应用。 */
public class CheckpointCorruptedException extends CheckpointException {

    public CheckpointCorruptedException(String message) {
        super(message);
    }

    public CheckpointCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
