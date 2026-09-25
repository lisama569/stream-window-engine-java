package com.github.lisama569.streamwindowenginejava.checkpoint;

/**
 * 检查点相关异常的基类：缺失、损坏、版本不兼容均为其子类，可明确区分。
 */
public class CheckpointException extends RuntimeException {

    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
