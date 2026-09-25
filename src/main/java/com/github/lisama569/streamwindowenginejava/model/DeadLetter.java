package com.github.lisama569.streamwindowenginejava.model;

/**
 * 死信记录：超出允许迟到范围而被拒绝的事件，进入有界死信日志以便观测与审计。
 */
public record DeadLetter(Event event, String reason) {
}
