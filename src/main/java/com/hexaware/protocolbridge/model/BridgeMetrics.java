package com.hexaware.protocolbridge.model;

import java.util.Map;

/**
 * Operational telemetry metrics snapshot for the Protocol Bridge.
 */
public record BridgeMetrics(
        long totalRequestsReceived,
        long totalSuccessBridged,
        long totalTimeouts,
        long totalErrors,
        int activePendingCorrelations,
        double averageLatencyMs,
        long minLatencyMs,
        long maxLatencyMs,
        Map<String, Long> requestsByDecisionStatus,
        long uptimeSeconds
) {}
