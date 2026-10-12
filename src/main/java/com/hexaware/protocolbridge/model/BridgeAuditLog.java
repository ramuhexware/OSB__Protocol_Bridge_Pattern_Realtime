package com.hexaware.protocolbridge.model;

import java.time.Instant;

/**
 * Audit log entry capturing granular transitions across the protocol bridge hops.
 */
public record BridgeAuditLog(
        String id,
        String correlationId,
        Instant timestamp,
        HopStage stage,
        String protocol,
        String details,
        String payloadSnippet,
        Long durationFromIngressMs
) {
    public enum HopStage {
        HTTP_INGRESS,
        PAYLOAD_TRANSFORM_TO_CANONICAL,
        JMS_REQUEST_DISPATCH,
        BACKEND_ENGINE_PROCESSING,
        JMS_REPLY_DISPATCH,
        CORRELATION_RESOLVED,
        PAYLOAD_TRANSFORM_TO_REST,
        HTTP_EGRESS_COMPLETED,
        TIMEOUT_FAULT,
        BRIDGE_ERROR
    }
}
