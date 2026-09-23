package com.github.highcumontoa.sagaorchestratorjava.saga.web;

import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 将可区分的拒绝原因映射为不同的 HTTP 状态码与 reason 字段，调用方据此区分失败原因。 */
@RestControllerAdvice
public class SagaExceptionHandler {

    @ExceptionHandler(SagaException.class)
    public ResponseEntity<Map<String, String>> handle(SagaException ex) {
        HttpStatus status = switch (ex.getReason()) {
            case UNKNOWN_INSTANCE -> HttpStatus.NOT_FOUND;
            case ILLEGAL_TRANSITION, CONCURRENT_CONFLICT -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(Map.of(
                "reason", ex.getReason().name(),
                "message", ex.getMessage()));
    }
}
