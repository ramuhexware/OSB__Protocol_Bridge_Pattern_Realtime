package com.hexaware.protocolbridge.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Outbound Client Response DTO (HTTP REST / JSON protocol).
 * Translated from the JMS Canonical Decision by the OSB Protocol Bridge Outbound Adapter.
 */
public record LoanDecisionResponse(
        // Business Decision
        String correlationId,
        String evaluationReferenceId,
        String decisionStatus,      // "ACCEPT", "REFER_ELIGIBLE", "REFER_INELIGIBLE", "DECLINE"
        String riskGrade,
        BigDecimal approvedAmount,
        BigDecimal recommendedRate,
        BigDecimal ltvPercentage,
        BigDecimal dtiPercentage,
        List<String> conditions,
        String message,

        // Protocol Bridge Telemetry
        BridgeHopSummary bridgeHopSummary
) {
    public record BridgeHopSummary(
            String clientProtocol,       // "HTTP/1.1 REST (JSON)"
            String enterpriseProtocol,   // "JMS 2.0 (Artemis Queue)"
            String requestQueue,
            String replyQueue,
            long totalBridgeLatencyMs,
            long backendProcessingMs,
            long queueHopTransitMs,
            Instant clientCompletedAt
    ) {}
}
