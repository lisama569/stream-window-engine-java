package com.github.lisama569.streamwindowenginejava.checkpoint;

/** 检查点格式版本与当前引擎不兼容：明确拒绝。 */
public class CheckpointVersionException extends CheckpointException {

    public CheckpointVersionException(String message) {
        super(message);
    }
}
