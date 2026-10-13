package com.hexaware.protocolbridge.service;

import com.hexaware.protocolbridge.bridge.AuditAndMetricsService;
import com.hexaware.protocolbridge.bridge.OsbProtocolBridge;
import com.hexaware.protocolbridge.bridge.SyncToAsyncBridgeManager;
import com.hexaware.protocolbridge.model.AsyncLoanSubmissionResponse;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import com.hexaware.protocolbridge.service.impl.LoanBridgeServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoanBridgeServiceTest {

    @Mock
    private OsbProtocolBridge bridge;

    @Mock
    private AuditAndMetricsService auditService;

    @Mock
    private SyncToAsyncBridgeManager syncToAsyncBridgeManager;

    private LoanBridgeService loanBridgeService;

    @BeforeEach
    void setUp() {
        loanBridgeService = new LoanBridgeServiceImpl(bridge, auditService, syncToAsyncBridgeManager);
    }

    private LoanApplicationRequest createSampleRequest() {
        return new LoanApplicationRequest(
                "Alice Walker",
                "111-22-3333",
                new BigDecimal("300000.00"),
                new BigDecimal("400000.00"),
                new BigDecimal("10000.00"),
                new BigDecimal("2000.00"),
                760,
                "PURCHASE",
                "SINGLE_FAMILY",
                360,
                false
        );
    }

    @Test
    @DisplayName("processSyncLoanApplication should delegate to bridge and return CompletableFuture")
    void shouldDelegateSyncLoanApplicationToBridge() {
        LoanApplicationRequest request = createSampleRequest();
        String correlationId = "CORR-SYNC-123";

        LoanDecisionResponse expectedResponse = new LoanDecisionResponse(
                correlationId,
                "EVAL-1234",
                "ACCEPT",
                "LOW_RISK_PRIME",
                new BigDecimal("300000.00"),
                new BigDecimal("6.125"),
                new BigDecimal("75.00"),
                new BigDecimal("20.00"),
                List.of("Condition 1"),
                "Approved",
                new LoanDecisionResponse.BridgeHopSummary(
                        "HTTP/1.1 REST (JSON)",
                        "JMS 2.0 (Artemis Queue)",
                        "req.q",
                        "rep.q",
                        50L,
                        30L,
                        20L,
                        Instant.now()
                )
        );

        when(bridge.bridgeSyncLoanApplication(request, correlationId))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        CompletableFuture<LoanDecisionResponse> future =
                loanBridgeService.processSyncLoanApplication(request, correlationId);

        assertThat(future).isCompletedWithValue(expectedResponse);
        verify(bridge).bridgeSyncLoanApplication(request, correlationId);
    }

    @Test
    @DisplayName("processAsyncLoanApplication should use client correlation ID when provided")
    void shouldProcessAsyncLoanWithProvidedCorrelationId() {
        LoanApplicationRequest request = createSampleRequest();
        String clientCorrelationId = "CLIENT-ASYNC-001";

        AsyncLoanSubmissionResponse response =
                loanBridgeService.processAsyncLoanApplication(request, clientCorrelationId);

        assertThat(response).isNotNull();
        assertThat(response.correlationId()).isEqualTo("CLIENT-ASYNC-001");
        assertThat(response.status()).isEqualTo("ACCEPTED_FOR_UNDERWRITING");
        assertThat(response.protocolTransition()).isEqualTo("HTTP_REST -> JMS_QUEUE");
        assertThat(response.notificationChannel()).isEqualTo("/topic/live-events");
        assertThat(response.timestamp()).isNotNull();

        verify(bridge).bridgeSyncLoanApplication(request, "CLIENT-ASYNC-001");
    }

    @Test
    @DisplayName("processAsyncLoanApplication should generate correlation ID when client correlation ID is null or blank")
    void shouldGenerateCorrelationIdWhenNullOrBlank() {
        LoanApplicationRequest request = createSampleRequest();

        AsyncLoanSubmissionResponse response =
                loanBridgeService.processAsyncLoanApplication(request, "   ");

        assertThat(response.correlationId()).startsWith("LP-ASYNC-");

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(bridge).bridgeSyncLoanApplication(eq(request), captor.capture());
        assertThat(captor.getValue()).isEqualTo(response.correlationId());
    }

    @Test
    @DisplayName("getAuditLogs should delegate to auditService")
    void shouldRetrieveAuditLogsFromAuditService() {
        BridgeAuditLog log = new BridgeAuditLog(
                "log-1",
                "corr-1",
                Instant.now(),
                BridgeAuditLog.HopStage.HTTP_INGRESS,
                "HTTP",
                "Details",
                "Payload",
                10L
        );
        when(auditService.getRecentAuditLogs()).thenReturn(List.of(log));

        List<BridgeAuditLog> logs = loanBridgeService.getAuditLogs();

        assertThat(logs).containsExactly(log);
        verify(auditService).getRecentAuditLogs();
    }

    @Test
    @DisplayName("getMetrics should coordinate activePendingCount from bridge manager and snapshot from auditService")
    void shouldRetrieveMetricsSnapshotWithActiveCorrelations() {
        when(syncToAsyncBridgeManager.getActivePendingCount()).thenReturn(3);

        BridgeMetrics expectedMetrics = new BridgeMetrics(
                10, 8, 1, 1, 3, 42.5, 20, 100, Map.of("ACCEPT", 8L), 300
        );
        when(auditService.getMetricsSnapshot(3)).thenReturn(expectedMetrics);

        BridgeMetrics actualMetrics = loanBridgeService.getMetrics();

        assertThat(actualMetrics).isEqualTo(expectedMetrics);
        verify(syncToAsyncBridgeManager).getActivePendingCount();
        verify(auditService).getMetricsSnapshot(3);
    }
}
