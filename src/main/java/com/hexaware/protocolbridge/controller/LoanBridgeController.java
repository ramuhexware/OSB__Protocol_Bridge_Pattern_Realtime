package com.hexaware.protocolbridge.controller;

import com.hexaware.protocolbridge.bridge.AuditAndMetricsService;
import com.hexaware.protocolbridge.bridge.OsbProtocolBridge;
import com.hexaware.protocolbridge.bridge.SyncToAsyncBridgeManager;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Inbound Protocol Adapter Controller.
 * Acts as the OSB Inbound HTTP Proxy Service:
 * - Exposes REST endpoints to modern web/mobile consumers
 * - Mediates calls across the OSB Protocol Bridge
 */
@RestController
@RequestMapping("/api/v1/bridge")
public class LoanBridgeController {

    private final OsbProtocolBridge bridge;
    private final AuditAndMetricsService auditService;
    private final SyncToAsyncBridgeManager syncToAsyncBridgeManager;

    public LoanBridgeController(
            OsbProtocolBridge bridge,
            AuditAndMetricsService auditService,
            SyncToAsyncBridgeManager syncToAsyncBridgeManager) {
        this.bridge = bridge;
        this.auditService = auditService;
        this.syncToAsyncBridgeManager = syncToAsyncBridgeManager;
    }

    /**
     * Synchronous Request-Reply Protocol Bridge Endpoint.
     * Web/mobile client initiates a synchronous HTTP POST.
     * The bridge transforms it, dispatches over JMS Artemis, awaits the
     * asynchronous correlated decision from the AUS engine, and responds with HTTP 200.
     */
    @PostMapping("/loan-application/sync")
    public CompletableFuture<ResponseEntity<LoanDecisionResponse>> submitSyncLoanApplication(
            @Valid @RequestBody LoanApplicationRequest request,
            @RequestHeader(value = "X-Correlation-ID", required = false) String clientCorrelationId) {

        return bridge.bridgeSyncLoanApplication(request, clientCorrelationId)
                .thenApply(ResponseEntity::ok);
    }

    /**
     * Asynchronous Ingestion Protocol Bridge Endpoint.
     * Accepts request, pushes into JMS Request Queue, immediately returns HTTP 202 Accepted
     * with correlation tracking ticket. Decision is pushed via WebSocket or polling.
     */
    @PostMapping("/loan-application/async")
    public ResponseEntity<Map<String, Object>> submitAsyncLoanApplication(
            @Valid @RequestBody LoanApplicationRequest request,
            @RequestHeader(value = "X-Correlation-ID", required = false) String clientCorrelationId) {

        String correlationId = (clientCorrelationId != null && !clientCorrelationId.isBlank())
                ? clientCorrelationId
                : "LP-ASYNC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        // Fire-and-forget bridge to JMS queue
        bridge.bridgeSyncLoanApplication(request, correlationId); // begins pipeline

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "status", "ACCEPTED_FOR_UNDERWRITING",
                "correlationId", correlationId,
                "protocolTransition", "HTTP_REST -> JMS_QUEUE",
                "notificationChannel", "/topic/live-events",
                "timestamp", Instant.now()
        ));
    }

    /**
     * Returns recent protocol bridge hop audit logs
     */
    @GetMapping("/audit-logs")
    public List<BridgeAuditLog> getAuditLogs() {
        return auditService.getRecentAuditLogs();
    }

    /**
     * Returns real-time metrics telemetry
     */
    @GetMapping("/metrics")
    public BridgeMetrics getMetrics() {
        return auditService.getMetricsSnapshot(syncToAsyncBridgeManager.getActivePendingCount());
    }
}
