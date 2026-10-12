package com.hexaware.protocolbridge.controller;

import com.hexaware.protocolbridge.bridge.OsbProtocolBridge;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Enterprise Fault & Exception Bridging (RFC 7807 Problem Details).
 * Translates JMS/Enterprise failures and timeouts into standardized HTTP REST responses.
 */
@RestControllerAdvice
public class GlobalBridgeExceptionHandler {

    @ExceptionHandler(OsbProtocolBridge.BridgeTimeoutException.class)
    public ProblemDetail handleTimeoutException(OsbProtocolBridge.BridgeTimeoutException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.GATEWAY_TIMEOUT, ex.getMessage());
        problem.setTitle("Protocol Bridge SLA Timeout");
        problem.setType(URI.create("https://hexaware.com/errors/bridge-timeout"));
        problem.setProperty("timestamp", Instant.now());
        problem.setProperty("faultStage", "JMS_REPLY_CORRELATION");
        problem.setProperty("recommendation", "Retry with exponential backoff or use asynchronous bridging endpoint.");
        return problem;
    }

    @ExceptionHandler(OsbProtocolBridge.BridgeException.class)
    public ProblemDetail handleBridgeException(OsbProtocolBridge.BridgeException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, ex.getMessage());
        problem.setTitle("Protocol Bridge Error");
        problem.setType(URI.create("https://hexaware.com/errors/bridge-failure"));
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidationException(MethodArgumentNotValidException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed on inbound REST request");
        problem.setTitle("Invalid Request Payload");
        problem.setType(URI.create("https://hexaware.com/errors/validation-error"));

        Map<String, String> errors = new HashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            errors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        problem.setProperty("fieldErrors", errors);
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGenericException(Exception ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
        problem.setTitle("Internal Server Error");
        problem.setType(URI.create("https://hexaware.com/errors/internal"));
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }
}
