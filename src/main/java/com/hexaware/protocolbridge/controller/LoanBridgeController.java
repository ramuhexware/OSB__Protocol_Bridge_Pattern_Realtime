package com.hexaware.protocolbridge.controller;

import com.hexaware.protocolbridge.model.AsyncLoanSubmissionResponse;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import com.hexaware.protocolbridge.service.LoanBridgeService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Inbound Protocol Adapter Controller.
 * Acts as the OSB Inbound HTTP Proxy Service:
 * - Exposes REST endpoints to modern web/mobile consumers
 * - Delegates all business execution and mediation to the Service Layer (LoanBridgeService)
 */
@RestController
@RequestMapping("/api/v1/bridge")
public class LoanBridgeController {

    private final LoanBridgeService loanBridgeService;

    public LoanBridgeController(LoanBridgeService loanBridgeService) {
        this.loanBridgeService = loanBridgeService;
    }

    /**
     * Synchronous Request-Reply Protocol Bridge Endpoint.
     * Web/mobile client initiates a synchronous HTTP POST.
     * Delegates to the service layer to coordinate mediation and correlation.
     */
    @PostMapping("/loan-application/sync")
    public CompletableFuture<ResponseEntity<LoanDecisionResponse>> submitSyncLoanApplication(
            @Valid @RequestBody LoanApplicationRequest request,
            @RequestHeader(value = "X-Correlation-ID", required = false) String clientCorrelationId) {

        return loanBridgeService.processSyncLoanApplication(request, clientCorrelationId)
                .thenApply(ResponseEntity::ok);
    }

    /**
     * Asynchronous Ingestion Protocol Bridge Endpoint.
     * Accepts request, delegates fire-and-forget ingestion to the service layer,
     * and returns HTTP 202 Accepted with correlation tracking ticket.
     */
    @PostMapping("/loan-application/async")
    public ResponseEntity<AsyncLoanSubmissionResponse> submitAsyncLoanApplication(
            @Valid @RequestBody LoanApplicationRequest request,
            @RequestHeader(value = "X-Correlation-ID", required = false) String clientCorrelationId) {

        AsyncLoanSubmissionResponse response = loanBridgeService.processAsyncLoanApplication(request, clientCorrelationId);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * Returns recent protocol bridge hop audit logs
     */
    @GetMapping("/audit-logs")
    public List<BridgeAuditLog> getAuditLogs() {
        return loanBridgeService.getAuditLogs();
    }

    /**
     * Returns real-time metrics telemetry
     */
    @GetMapping("/metrics")
    public BridgeMetrics getMetrics() {
        return loanBridgeService.getMetrics();
    }
}
