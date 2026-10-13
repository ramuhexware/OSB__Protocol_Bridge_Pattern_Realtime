package com.hexaware.protocolbridge.model;

import java.time.Instant;

/**
 * Outbound response DTO for asynchronous loan application ingestion.
 * Returned immediately with HTTP 202 Accepted and correlation tracking ticket.
 */
public record AsyncLoanSubmissionResponse(
        String status,
        String correlationId,
        String protocolTransition,
        String notificationChannel,
        Instant timestamp
) {
}
