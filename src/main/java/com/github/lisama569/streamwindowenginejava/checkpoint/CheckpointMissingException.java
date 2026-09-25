package com.github.lisama569.streamwindowenginejava.checkpoint;

/** 严格模式下启动时未发现任何检查点：明确拒绝。 */
public class CheckpointMissingException extends CheckpointException {

    public CheckpointMissingException(String message) {
        super(message);
    }
}
