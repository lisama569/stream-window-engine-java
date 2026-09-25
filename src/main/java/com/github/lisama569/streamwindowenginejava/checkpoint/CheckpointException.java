package com.github.lisama569.streamwindowenginejava.checkpoint;

/**
 * 检查点缺失、损坏或版本不兼容时抛出；引擎必须明确拒绝而不是部分应用。
 */
public class CheckpointException extends RuntimeException {
    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
