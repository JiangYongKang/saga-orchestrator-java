package com.github.highcumontoa.sagaorchestratorjava.saga.web;

import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 把被拒绝的操作翻译成带明确 reason 的 HTTP 响应，便于调用方区分失败原因。 */
@RestControllerAdvice
public class SagaExceptionHandler {

    @ExceptionHandler(SagaRejectedException.class)
    public ResponseEntity<Map<String, Object>> handle(SagaRejectedException ex) {
        RejectReason reason = ex.getReason();
        HttpStatus status = switch (reason) {
            case UNKNOWN_INSTANCE -> HttpStatus.NOT_FOUND;
            case CONCURRENT_CONFLICT, VERSION_CONFLICT -> HttpStatus.CONFLICT;
            case ILLEGAL_STATE, CONFLICTING_DEFINITION -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status).body(Map.of(
                "rejected", true,
                "reason", reason.name(),
                "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
