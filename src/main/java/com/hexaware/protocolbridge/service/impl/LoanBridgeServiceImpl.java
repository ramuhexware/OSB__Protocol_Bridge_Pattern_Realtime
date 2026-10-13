package com.hexaware.protocolbridge.service.impl;

import com.hexaware.protocolbridge.bridge.AuditAndMetricsService;
import com.hexaware.protocolbridge.bridge.OsbProtocolBridge;
import com.hexaware.protocolbridge.bridge.SyncToAsyncBridgeManager;
import com.hexaware.protocolbridge.model.AsyncLoanSubmissionResponse;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import com.hexaware.protocolbridge.service.LoanBridgeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Service Layer implementation for the OSB Protocol Bridge application.
 * Encapsulates business orchestration, correlation ID generation,
 * async ingestion tracking, and metrics aggregation.
 */
@Service
public class LoanBridgeServiceImpl implements LoanBridgeService {

    private static final Logger log = LoggerFactory.getLogger(LoanBridgeServiceImpl.class);

    private final OsbProtocolBridge bridge;
    private final AuditAndMetricsService auditService;
    private final SyncToAsyncBridgeManager syncToAsyncBridgeManager;

    public LoanBridgeServiceImpl(
            OsbProtocolBridge bridge,
            AuditAndMetricsService auditService,
            SyncToAsyncBridgeManager syncToAsyncBridgeManager) {
        this.bridge = bridge;
        this.auditService = auditService;
        this.syncToAsyncBridgeManager = syncToAsyncBridgeManager;
    }

    @Override
    public CompletableFuture<LoanDecisionResponse> processSyncLoanApplication(
            LoanApplicationRequest request,
            String clientCorrelationId) {
        log.debug("Processing synchronous loan application across OSB Protocol Bridge");
        return bridge.bridgeSyncLoanApplication(request, clientCorrelationId);
    }

    @Override
    public AsyncLoanSubmissionResponse processAsyncLoanApplication(
            LoanApplicationRequest request,
            String clientCorrelationId) {

        String correlationId = (clientCorrelationId != null && !clientCorrelationId.isBlank())
                ? clientCorrelationId
                : "LP-ASYNC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        log.debug("Processing asynchronous fire-and-forget loan submission for correlation [{}]", correlationId);

        // Fire-and-forget bridge into JMS request queue pipeline
        bridge.bridgeSyncLoanApplication(request, correlationId);

        return new AsyncLoanSubmissionResponse(
                "ACCEPTED_FOR_UNDERWRITING",
                correlationId,
                "HTTP_REST -> JMS_QUEUE",
                "/topic/live-events",
                Instant.now()
        );
    }

    @Override
    public List<BridgeAuditLog> getAuditLogs() {
        return auditService.getRecentAuditLogs();
    }

    @Override
    public BridgeMetrics getMetrics() {
        int activePendingCount = syncToAsyncBridgeManager.getActivePendingCount();
        return auditService.getMetricsSnapshot(activePendingCount);
    }
}
