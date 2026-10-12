package com.hexaware.protocolbridge.bridge;

import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.CanonicalUnderwritingDecision;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Synchronous-to-Asynchronous Protocol Bridge Manager.
 * Implements the classic OSB Request-Reply Correlator pattern:
 * 1. Registers an asynchronous CompletableFuture keyed by JMSCorrelationID.
 * 2. Inbound listener on the Reply Queue matches arriving responses to pending callers.
 * 3. Enforces SLA timeouts and cleans up orphan correlations.
 */
@Component
public class SyncToAsyncBridgeManager {

    private static final Logger log = LoggerFactory.getLogger(SyncToAsyncBridgeManager.class);

    private final ConcurrentHashMap<String, PendingCorrelation> pendingCorrelations = new ConcurrentHashMap<>();
    private final AuditAndMetricsService auditService;
    private final MessageConverter messageConverter;

    @Value("${bridge.timeout-ms:6000}")
    private long bridgeTimeoutMs;

    public SyncToAsyncBridgeManager(AuditAndMetricsService auditService, MessageConverter messageConverter) {
        this.auditService = auditService;
        this.messageConverter = messageConverter;
    }

    private record PendingCorrelation(
            String correlationId,
            Instant registeredAt,
            CompletableFuture<CanonicalUnderwritingDecision> future
    ) {}

    /**
     * Registers a correlation ID to await asynchronous reply with SLA timeout
     */
    public CompletableFuture<CanonicalUnderwritingDecision> registerPendingRequest(String correlationId) {
        CompletableFuture<CanonicalUnderwritingDecision> future = new CompletableFuture<>();
        pendingCorrelations.put(correlationId, new PendingCorrelation(correlationId, Instant.now(), future));

        // Enforce SLA timeout using Java 21 CompletableFuture orTimeout
        future.orTimeout(bridgeTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete((result, ex) -> {
                    // Always remove on completion or timeout to prevent memory leak
                    pendingCorrelations.remove(correlationId);

                    if (ex instanceof TimeoutException) {
                        log.warn("SLA Timeout reached for correlation ID [{}], removing from bridge correlator.", correlationId);
                        auditService.recordTimeout();
                        auditService.recordHop(
                                correlationId,
                                BridgeAuditLog.HopStage.TIMEOUT_FAULT,
                                "OSB_CORRELATOR",
                                "Timeout SLA exceeded (" + bridgeTimeoutMs + "ms). Discarding correlation waiter.",
                                null,
                                bridgeTimeoutMs
                        );
                    }
                });

        return future;
    }

    /**
     * JMS Listener consuming asynchronous replies from the Reply Queue.
     * Correlates replies by JMSCorrelationID and fulfills the waiting CompletableFuture.
     */
    @JmsListener(
            destination = "${bridge.reply-queue:freddie.lpa.loan.reply.queue}",
            containerFactory = "queueListenerFactory"
    )
    public void onReplyMessage(Message message) {
        try {
            String correlationId = message.getJMSCorrelationID();
            if (correlationId == null || correlationId.isBlank()) {
                log.error("Received message on reply queue without JMSCorrelationID. Dropping.");
                return;
            }

            Object payload = messageConverter.fromMessage(message);
            if (!(payload instanceof CanonicalUnderwritingDecision decision)) {
                log.error("Unexpected payload type [{}] on reply queue for correlationId [{}]",
                        payload != null ? payload.getClass().getName() : "null", correlationId);
                return;
            }

            PendingCorrelation pending = pendingCorrelations.remove(correlationId);
            if (pending != null) {
                long duration = Duration.between(pending.registeredAt(), Instant.now()).toMillis();
                log.info("Correlated reply matched for ID [{}] in {} ms. Fulfilling sync caller.", correlationId, duration);

                auditService.recordHop(
                        correlationId,
                        BridgeAuditLog.HopStage.CORRELATION_RESOLVED,
                        "JMS_ARTEMIS_REPLY_QUEUE",
                        "JMSCorrelationID matched successfully. Status=" + decision.decisionStatus(),
                        "RiskGrade=" + decision.riskGrade() + ", ApprovedAmount=" + decision.approvedAmount(),
                        duration
                );

                // Complete the future, waking up the waiting synchronous HTTP thread
                pending.future().complete(decision);
            } else {
                log.warn("Received reply for unknown or already timed-out correlation ID [{}]. Dropping.", correlationId);
                auditService.recordHop(
                        correlationId,
                        BridgeAuditLog.HopStage.BRIDGE_ERROR,
                        "JMS_REPLY_LISTENER",
                        "Orphan reply received (correlation ID expired or unknown).",
                        decision.toString(),
                        null
                );
            }

        } catch (JMSException e) {
            log.error("JMS error processing reply message", e);
            auditService.recordError();
        }
    }

    public int getActivePendingCount() {
        return pendingCorrelations.size();
    }
}
