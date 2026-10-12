package com.hexaware.protocolbridge.bridge;

import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.CanonicalLoanMessage;
import com.hexaware.protocolbridge.model.CanonicalUnderwritingDecision;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Core OSB Protocol Bridge Implementation.
 * Orchestrates the bridging pipeline:
 * 1. Protocol termination & header extraction (HTTP/REST)
 * 2. Data transformation (JSON -> Enterprise Canonical XML/Model)
 * 3. Transport protocol switch (HTTP -> JMS)
 * 4. Request-Reply correlation over asynchronous queues
 * 5. Reverse transformation (Enterprise Decision -> HTTP/JSON Response)
 * 6. Fault & timeout translation
 */
@Service
public class OsbProtocolBridge {

    private static final Logger log = LoggerFactory.getLogger(OsbProtocolBridge.class);

    private final JmsTemplate jmsTemplate;
    private final JmsTemplate topicJmsTemplate;
    private final ProtocolTransformer transformer;
    private final SyncToAsyncBridgeManager syncToAsyncBridgeManager;
    private final AuditAndMetricsService auditService;

    @Value("${bridge.request-queue:freddie.lpa.loan.request.queue}")
    private String requestQueueName;

    @Value("${bridge.reply-queue:freddie.lpa.loan.reply.queue}")
    private String replyQueueName;

    @Value("${bridge.notification-topic:freddie.lpa.notifications.topic}")
    private String notificationTopicName;

    public OsbProtocolBridge(
            @org.springframework.beans.factory.annotation.Qualifier("jmsTemplate") JmsTemplate jmsTemplate,
            @org.springframework.beans.factory.annotation.Qualifier("topicJmsTemplate") JmsTemplate topicJmsTemplate,
            ProtocolTransformer transformer,
            SyncToAsyncBridgeManager syncToAsyncBridgeManager,
            AuditAndMetricsService auditService) {
        this.jmsTemplate = jmsTemplate;
        this.topicJmsTemplate = topicJmsTemplate;
        this.transformer = transformer;
        this.syncToAsyncBridgeManager = syncToAsyncBridgeManager;
        this.auditService = auditService;
    }

    /**
     * Executes the Synchronous Request-Reply Protocol Bridge pattern.
     * Takes an HTTP REST request, bridges it over JMS, waits asynchronously for
     * response, and returns an HTTP-compatible response DTO.
     */
    public CompletableFuture<LoanDecisionResponse> bridgeSyncLoanApplication(
            LoanApplicationRequest request,
            String clientCorrelationId) {

        Instant ingressTime = Instant.now();
        String correlationId = (clientCorrelationId != null && !clientCorrelationId.isBlank())
                ? clientCorrelationId
                : "LP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        auditService.incrementTotalRequests();

        // 1. Stage: HTTP Ingress
        auditService.recordHop(
                correlationId,
                BridgeAuditLog.HopStage.HTTP_INGRESS,
                "HTTP/1.1 REST (JSON)",
                "Inbound POST request received from HTTP client. Borrower: " + request.borrowerName(),
                "LoanAmount=$" + request.loanAmount() + ", FICO=" + request.creditScore(),
                0L
        );

        // 2. Stage: Protocol & Payload Transformation
        CanonicalLoanMessage canonicalMsg = transformer.toCanonicalMessage(request, correlationId);
        auditService.recordHop(
                correlationId,
                BridgeAuditLog.HopStage.PAYLOAD_TRANSFORM_TO_CANONICAL,
                "OSB_TRANSFORMER",
                "Transformed HTTP JSON to Enterprise Canonical Model. Computed LTV=" + canonicalMsg.loanToValueRatio() + "%, DTI=" + canonicalMsg.debtToIncomeRatio() + "%",
                canonicalMsg.toString(),
                Duration.between(ingressTime, Instant.now()).toMillis()
        );

        // 3. Stage: Register Synchronous-to-Asynchronous Correlation Waiter
        CompletableFuture<CanonicalUnderwritingDecision> replyFuture =
                syncToAsyncBridgeManager.registerPendingRequest(correlationId);

        // 4. Stage: Dispatch onto JMS Request Queue with Transport Properties
        try {
            jmsTemplate.convertAndSend(
                    requestQueueName,
                    canonicalMsg,
                    new MessagePostProcessor() {
                        @Override
                        public Message postProcessMessage(Message message) throws JMSException {
                            message.setJMSCorrelationID(correlationId);
                            // Set dynamic JMSReplyTo queue
                            Queue replyQueue = message.getJMSReplyTo() != null ?
                                    (Queue) message.getJMSReplyTo() :
                                    message.getJMSDestination() != null ?
                                            null : null;
                            // Set enterprise transport headers
                            message.setStringProperty("X_SOURCE_PROTOCOL", "HTTP_REST");
                            message.setStringProperty("X_TARGET_ENGINE", "FREDDIE_MAC_LPA");
                            message.setLongProperty("X_INGRESS_TIMESTAMP", ingressTime.toEpochMilli());
                            return message;
                        }
                    }
            );

            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.JMS_REQUEST_DISPATCH,
                    "JMS_ARTEMIS",
                    "Dispatched to queue [" + requestQueueName + "] with JMSCorrelationID=" + correlationId,
                    "Target=JMS_QUEUE, DeliveryMode=NON_PERSISTENT",
                    Duration.between(ingressTime, Instant.now()).toMillis()
            );

            // Also publish event onto Notification Topic (Topic Protocol Bridge)
            publishNotificationEvent("SUBMITTED", correlationId, "Loan application bridged to underwriting queue.");

        } catch (Exception ex) {
            log.error("Failed to dispatch JMS message for correlation [{}]", correlationId, ex);
            auditService.recordError();
            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.BRIDGE_ERROR,
                    "JMS_PRODUCER",
                    "Failed to enqueue to " + requestQueueName + ": " + ex.getMessage(),
                    null,
                    Duration.between(ingressTime, Instant.now()).toMillis()
            );
            return CompletableFuture.failedFuture(ex);
        }

        // 5. Stage: Chain Response Transformation upon Future Completion
        return replyFuture.handle((decision, throwable) -> {
            if (throwable != null) {
                Throwable cause = (throwable instanceof CompletionException) ? throwable.getCause() : throwable;
                if (cause instanceof TimeoutException) {
                    throw new BridgeTimeoutException("Gateway Timeout: Backend AUS Engine did not respond within SLA window for correlation " + correlationId);
                }
                throw new BridgeException("Bridge Error while awaiting JMS response: " + cause.getMessage(), cause);
            }

            // Reverse Transformation: Canonical Decision to HTTP/REST Response
            LoanDecisionResponse response = transformer.toClientResponse(
                    decision,
                    ingressTime,
                    requestQueueName,
                    replyQueueName
            );

            long totalLatency = Duration.between(ingressTime, Instant.now()).toMillis();
            auditService.recordSuccess(totalLatency, decision.decisionStatus());

            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.PAYLOAD_TRANSFORM_TO_REST,
                    "OSB_TRANSFORMER",
                    "Converted Canonical Underwriting Decision to Outbound REST DTO. Status=" + decision.decisionStatus(),
                    response.toString(),
                    totalLatency
            );

            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.HTTP_EGRESS_COMPLETED,
                    "HTTP/1.1 REST (JSON)",
                    "Returned HTTP 200 OK to caller. Total Bridge Latency: " + totalLatency + "ms",
                    "Decision=" + decision.decisionStatus() + ", Ref=" + decision.evaluationReferenceId(),
                    totalLatency
            );

            publishNotificationEvent("COMPLETED", correlationId, "Decision bridged to client: " + decision.decisionStatus());
            return response;
        });
    }

    /**
     * Publishes lifecycle event to the JMS Notification Topic
     */
    public void publishNotificationEvent(String eventType, String correlationId, String message) {
        try {
            topicJmsTemplate.send(notificationTopicName, session -> {
                Message msg = session.createTextMessage("{\"event\":\"" + eventType + "\",\"correlationId\":\"" + correlationId + "\",\"message\":\"" + message + "\"}");
                msg.setJMSCorrelationID(correlationId);
                msg.setStringProperty("EVENT_TYPE", eventType);
                return msg;
            });
        } catch (Exception ex) {
            log.warn("Could not publish notification event to topic {}: {}", notificationTopicName, ex.getMessage());
        }
    }

    // Custom Exceptions for RFC 7807 Fault Bridging
    public static class BridgeTimeoutException extends RuntimeException {
        public BridgeTimeoutException(String message) {
            super(message);
        }
    }

    public static class BridgeException extends RuntimeException {
        public BridgeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
