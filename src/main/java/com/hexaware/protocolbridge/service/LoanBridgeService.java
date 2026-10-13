package com.hexaware.protocolbridge.service;

import com.hexaware.protocolbridge.model.AsyncLoanSubmissionResponse;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service Layer interface for the OSB Protocol Bridge Application.
 * Decouples presentation/controller layer from underlying messaging,
 * telemetry, and protocol transformation subsystems.
 */
public interface LoanBridgeService {

    /**
     * Executes synchronous loan application processing across the OSB Protocol Bridge.
     * Takes an inbound request, coordinates asynchronous JMS dispatch and reply correlation,
     * and returns a CompletableFuture containing the loan decision.
     *
     * @param request the loan application details
     * @param clientCorrelationId optional caller-provided correlation identifier
     * @return CompletableFuture resolving to the loan decision response
     */
    CompletableFuture<LoanDecisionResponse> processSyncLoanApplication(
            LoanApplicationRequest request,
            String clientCorrelationId);

    /**
     * Executes asynchronous loan application ingestion.
     * Assigns correlation tracking, dispatches message to enterprise queue,
     * and returns tracking metadata immediately.
     *
     * @param request the loan application details
     * @param clientCorrelationId optional caller-provided correlation identifier
     * @return submission ticket with status and tracking identifiers
     */
    AsyncLoanSubmissionResponse processAsyncLoanApplication(
            LoanApplicationRequest request,
            String clientCorrelationId);

    /**
     * Retrieves recent bridge hop audit log entries.
     *
     * @return list of historical audit trail records
     */
    List<BridgeAuditLog> getAuditLogs();

    /**
     * Retrieves aggregated bridge telemetry and performance metrics snapshot.
     *
     * @return current metrics snapshot including active correlation count
     */
    BridgeMetrics getMetrics();
}
