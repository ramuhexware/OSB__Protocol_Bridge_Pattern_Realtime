package com.hexaware.protocolbridge.bridge;

import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real-time Telemetry and Audit Logging Service for OSB Protocol Bridge Pattern.
 * Collects message hop audit trails, computes latency statistics, and broadcasts
 * live telemetry over WebSocket STOMP.
 */
@Service
public class AuditAndMetricsService {

    private final SimpMessagingTemplate messagingTemplate;
    private final ConcurrentLinkedDeque<BridgeAuditLog> auditLogs = new ConcurrentLinkedDeque<>();
    private static final int MAX_AUDIT_LOGS = 100;

    // Metrics Counters
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalSuccess = new AtomicLong(0);
    private final AtomicLong totalTimeouts = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // Latency Tracking
    private final AtomicLong latencySum = new AtomicLong(0);
    private final AtomicLong latencyCount = new AtomicLong(0);
    private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatency = new AtomicLong(0);

    private final Map<String, AtomicLong> decisionsCounter = new ConcurrentHashMap<>();

    public AuditAndMetricsService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    /**
     * Records a protocol hop stage and broadcasts to live WebSocket topic
     */
    public void recordHop(
            String correlationId,
            BridgeAuditLog.HopStage stage,
            String protocol,
            String details,
            String payloadSnippet,
            Long durationFromIngressMs) {

        BridgeAuditLog log = new BridgeAuditLog(
                UUID.randomUUID().toString(),
                correlationId,
                Instant.now(),
                stage,
                protocol,
                details,
                payloadSnippet,
                durationFromIngressMs
        );

        auditLogs.addFirst(log);
        while (auditLogs.size() > MAX_AUDIT_LOGS) {
            auditLogs.pollLast();
        }

        // Broadcast to WebSocket clients subscribed to /topic/audit-stream
        try {
            messagingTemplate.convertAndSend("/topic/audit-stream", log);
        } catch (Exception ignored) {
            // Broker may not have active listeners yet
        }
    }

    public void incrementTotalRequests() {
        totalRequests.incrementAndGet();
        broadcastMetrics();
    }

    public void recordSuccess(long latencyMs, String decisionStatus) {
        totalSuccess.incrementAndGet();
        latencySum.addAndGet(latencyMs);
        latencyCount.incrementAndGet();

        minLatency.accumulateAndGet(latencyMs, Math::min);
        maxLatency.accumulateAndGet(latencyMs, Math::max);

        if (decisionStatus != null) {
            decisionsCounter.computeIfAbsent(decisionStatus, k -> new AtomicLong(0)).incrementAndGet();
        }
        broadcastMetrics();
    }

    public void recordTimeout() {
        totalTimeouts.incrementAndGet();
        broadcastMetrics();
    }

    public void recordError() {
        totalErrors.incrementAndGet();
        broadcastMetrics();
    }

    public List<BridgeAuditLog> getRecentAuditLogs() {
        return new ArrayList<>(auditLogs);
    }

    public BridgeMetrics getMetricsSnapshot(int activeCorrelations) {
        long count = latencyCount.get();
        double avg = count == 0 ? 0.0 : (double) latencySum.get() / count;
        long min = minLatency.get() == Long.MAX_VALUE ? 0 : minLatency.get();
        long max = maxLatency.get();

        Map<String, Long> decisionMap = new ConcurrentHashMap<>();
        decisionsCounter.forEach((k, v) -> decisionMap.put(k, v.get()));

        long uptimeSec = ManagementFactory.getRuntimeMXBean().getUptime() / 1000;

        return new BridgeMetrics(
                totalRequests.get(),
                totalSuccess.get(),
                totalTimeouts.get(),
                totalErrors.get(),
                activeCorrelations,
                Math.round(avg * 100.0) / 100.0,
                min,
                max,
                Collections.unmodifiableMap(decisionMap),
                uptimeSec
        );
    }

    private void broadcastMetrics() {
        try {
            // Note: active correlations will be supplied via controller poll or scheduled push
            messagingTemplate.convertAndSend("/topic/metrics-update", getMetricsSnapshot(0));
        } catch (Exception ignored) {
        }
    }
}
