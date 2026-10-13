package com.hexaware.protocolbridge.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hexaware.protocolbridge.model.AsyncLoanSubmissionResponse;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.BridgeMetrics;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import com.hexaware.protocolbridge.service.LoanBridgeService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LoanBridgeController.class)
class LoanBridgeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private LoanBridgeService loanBridgeService;

    private LoanApplicationRequest createSampleRequest() {
        return new LoanApplicationRequest(
                "Bob Martin",
                "444-55-6666",
                new BigDecimal("250000.00"),
                new BigDecimal("350000.00"),
                new BigDecimal("8000.00"),
                new BigDecimal("1500.00"),
                740,
                "PURCHASE",
                "SINGLE_FAMILY",
                360,
                false
        );
    }

    @Test
    @DisplayName("POST /sync should delegate to LoanBridgeService.processSyncLoanApplication and return HTTP 200")
    void shouldDelegateSyncRequestToService() throws Exception {
        LoanApplicationRequest request = createSampleRequest();
        LoanDecisionResponse decisionResponse = new LoanDecisionResponse(
                "CORR-SYNC-999",
                "REF-888",
                "ACCEPT",
                "LOW_RISK_PRIME",
                new BigDecimal("250000.00"),
                new BigDecimal("6.250"),
                new BigDecimal("71.43"),
                new BigDecimal("18.75"),
                List.of(),
                "Approved",
                new LoanDecisionResponse.BridgeHopSummary(
                        "HTTP/1.1 REST (JSON)", "JMS 2.0 (Artemis Queue)", "req.q", "rep.q", 40L, 25L, 15L, Instant.now()
                )
        );

        when(loanBridgeService.processSyncLoanApplication(any(LoanApplicationRequest.class), eq("CORR-SYNC-999")))
                .thenReturn(CompletableFuture.completedFuture(decisionResponse));

        MvcResult mvcResult = mockMvc.perform(post("/api/v1/bridge/loan-application/sync")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-ID", "CORR-SYNC-999")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.correlationId").value("CORR-SYNC-999"))
                .andExpect(jsonPath("$.decisionStatus").value("ACCEPT"));

        verify(loanBridgeService).processSyncLoanApplication(any(LoanApplicationRequest.class), eq("CORR-SYNC-999"));
    }

    @Test
    @DisplayName("POST /async should delegate to LoanBridgeService.processAsyncLoanApplication and return HTTP 202")
    void shouldDelegateAsyncRequestToService() throws Exception {
        LoanApplicationRequest request = createSampleRequest();
        AsyncLoanSubmissionResponse asyncResponse = new AsyncLoanSubmissionResponse(
                "ACCEPTED_FOR_UNDERWRITING",
                "LP-ASYNC-TEST1",
                "HTTP_REST -> JMS_QUEUE",
                "/topic/live-events",
                Instant.now()
        );

        when(loanBridgeService.processAsyncLoanApplication(any(LoanApplicationRequest.class), eq("LP-ASYNC-TEST1")))
                .thenReturn(asyncResponse);

        mockMvc.perform(post("/api/v1/bridge/loan-application/async")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-ID", "LP-ASYNC-TEST1")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED_FOR_UNDERWRITING"))
                .andExpect(jsonPath("$.correlationId").value("LP-ASYNC-TEST1"))
                .andExpect(jsonPath("$.protocolTransition").value("HTTP_REST -> JMS_QUEUE"));

        verify(loanBridgeService).processAsyncLoanApplication(any(LoanApplicationRequest.class), eq("LP-ASYNC-TEST1"));
    }

    @Test
    @DisplayName("GET /audit-logs should delegate to LoanBridgeService.getAuditLogs")
    void shouldDelegateAuditLogsToService() throws Exception {
        BridgeAuditLog log = new BridgeAuditLog(
                "id-1",
                "corr-1",
                Instant.now(),
                BridgeAuditLog.HopStage.HTTP_INGRESS,
                "HTTP",
                "Details",
                "Snippet",
                5L
        );
        when(loanBridgeService.getAuditLogs()).thenReturn(List.of(log));

        mockMvc.perform(get("/api/v1/bridge/audit-logs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].correlationId").value("corr-1"));

        verify(loanBridgeService).getAuditLogs();
    }

    @Test
    @DisplayName("GET /metrics should delegate to LoanBridgeService.getMetrics")
    void shouldDelegateMetricsToService() throws Exception {
        BridgeMetrics metrics = new BridgeMetrics(
                100, 95, 2, 3, 4, 35.5, 12, 120, Map.of("ACCEPT", 95L), 600
        );
        when(loanBridgeService.getMetrics()).thenReturn(metrics);

        mockMvc.perform(get("/api/v1/bridge/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRequestsReceived").value(100))
                .andExpect(jsonPath("$.activePendingCorrelations").value(4));

        verify(loanBridgeService).getMetrics();
    }
}
