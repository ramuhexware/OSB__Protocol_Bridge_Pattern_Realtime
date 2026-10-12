package com.hexaware.protocolbridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
class OsbProtocolBridgeIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Should successfully bridge HTTP REST to JMS Queue and back for Prime Loan Approval")
    void shouldBridgeSynchronouslyForPrimeLoanApproval() throws Exception {
        LoanApplicationRequest request = new LoanApplicationRequest(
                "John Doe",
                "123-45-6789",
                new BigDecimal("350000.00"),
                new BigDecimal("500000.00"), // LTV = 70%
                new BigDecimal("12000.00"),
                new BigDecimal("2500.00"),   // DTI = 20.8%
                780,                         // High FICO
                "PURCHASE",
                "SINGLE_FAMILY",
                360,
                false
        );

        MvcResult mvcResult = mockMvc.perform(post("/api/v1/bridge/loan-application/sync")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decisionStatus").value("ACCEPT"))
                .andExpect(jsonPath("$.riskGrade").value("LOW_RISK_PRIME"))
                .andExpect(jsonPath("$.approvedAmount").value(350000.00))
                .andExpect(jsonPath("$.bridgeHopSummary.clientProtocol").value("HTTP/1.1 REST (JSON)"))
                .andExpect(jsonPath("$.bridgeHopSummary.enterpriseProtocol").value("JMS 2.0 (Artemis Queue)"))
                .andExpect(jsonPath("$.bridgeHopSummary.requestQueue").value("freddie.lpa.loan.request.queue"))
                .andExpect(jsonPath("$.bridgeHopSummary.replyQueue").value("freddie.lpa.loan.reply.queue"));

        String content = mvcResult.getResponse().getContentAsString();
        LoanDecisionResponse response = objectMapper.readValue(content, LoanDecisionResponse.class);
        assertThat(response.correlationId()).isNotBlank();
        assertThat(response.recommendedRate()).isNotNull();
        assertThat(response.bridgeHopSummary().totalBridgeLatencyMs()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should bridge and decline high DTI loan applications")
    void shouldBridgeAndDeclineHighDtiLoan() throws Exception {
        LoanApplicationRequest request = new LoanApplicationRequest(
                "Jane Smith",
                "987-65-4321",
                new BigDecimal("400000.00"),
                new BigDecimal("450000.00"),
                new BigDecimal("5000.00"),
                new BigDecimal("3200.00"),   // DTI = 64% (>50% ceiling)
                720,
                "PURCHASE",
                "CONDO",
                360,
                false
        );

        MvcResult mvcResult = mockMvc.perform(post("/api/v1/bridge/loan-application/sync")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decisionStatus").value("DECLINE"))
                .andExpect(jsonPath("$.riskGrade").value("HIGH_DEBT_RISK"));
    }

    @Test
    @DisplayName("Should bridge asynchronously and return HTTP 202 Accepted")
    void shouldBridgeAsynchronously() throws Exception {
        LoanApplicationRequest request = new LoanApplicationRequest(
                "Robert Miller",
                "555-44-3322",
                new BigDecimal("280000.00"),
                new BigDecimal("350000.00"),
                new BigDecimal("9000.00"),
                new BigDecimal("2100.00"),
                710,
                "REFINANCE",
                "SINGLE_FAMILY",
                360,
                false
        );

        mockMvc.perform(post("/api/v1/bridge/loan-application/async")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED_FOR_UNDERWRITING"))
                .andExpect(jsonPath("$.correlationId").exists())
                .andExpect(jsonPath("$.protocolTransition").value("HTTP_REST -> JMS_QUEUE"));
    }
}
