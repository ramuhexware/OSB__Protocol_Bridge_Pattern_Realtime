package com.hexaware.protocolbridge.bridge;

import com.hexaware.protocolbridge.model.CanonicalLoanMessage;
import com.hexaware.protocolbridge.model.CanonicalUnderwritingDecision;
import com.hexaware.protocolbridge.model.LoanApplicationRequest;
import com.hexaware.protocolbridge.model.LoanDecisionResponse;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Protocol Transformer Component.
 * Simulates OSB Pipeline XQuery / XSLT / MFL Transformation Stages:
 * - Inbound: Translates HTTP/JSON DTO to Canonical Enterprise Format (CDM)
 * - Outbound: Translates Canonical Decision back to Client REST/JSON DTO
 */
@Component
public class ProtocolTransformer {

    private static final String BRIDGE_NODE_ID = "OSB-NODE-PROD-EAST-01";

    /**
     * Transforms Inbound HTTP REST LoanApplicationRequest into Canonical Enterprise Message
     */
    public CanonicalLoanMessage toCanonicalMessage(LoanApplicationRequest request, String correlationId) {
        String corrId = (correlationId != null && !correlationId.isBlank()) 
                ? correlationId 
                : "CORR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        // Calculate LTV = (loanAmount / propertyValue) * 100
        BigDecimal ltv = request.loanAmount()
                .divide(request.propertyValue(), 4, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .setScale(2, RoundingMode.HALF_UP);

        // Calculate DTI = (monthlyDebt / monthlyIncome) * 100
        BigDecimal dti = request.monthlyDebt()
                .divide(request.monthlyIncome(), 4, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .setScale(2, RoundingMode.HALF_UP);

        // Mask SSN for enterprise compliance (e.g., ***-**-1234)
        String rawSsn = request.borrowerSsn();
        String maskedSsn = (rawSsn != null && rawSsn.length() >= 4)
                ? "***-**-" + rawSsn.substring(rawSsn.length() - 4)
                : "***-**-0000";

        return new CanonicalLoanMessage(
                corrId,
                "HTTP_REST_JSON",
                "JMS_ARTEMIS_ENTERPRISE",
                Instant.now(),
                BRIDGE_NODE_ID,
                request.borrowerName(),
                maskedSsn,
                request.loanAmount(),
                request.propertyValue(),
                request.monthlyIncome(),
                request.monthlyDebt(),
                request.creditScore(),
                request.loanPurpose(),
                request.propertyType(),
                request.loanTermMonths(),
                ltv,
                dti,
                Boolean.TRUE.equals(request.simulateTimeout())
        );
    }

    /**
     * Transforms Outbound Backend Canonical Underwriting Decision into Client REST/JSON Response
     */
    public LoanDecisionResponse toClientResponse(
            CanonicalUnderwritingDecision decision,
            Instant ingressTime,
            String requestQueue,
            String replyQueue) {

        Instant now = Instant.now();
        long totalBridgeLatency = Duration.between(ingressTime, now).toMillis();
        long backendProcessing = decision.processingDurationMs();
        long queueTransit = Math.max(0, totalBridgeLatency - backendProcessing);

        String humanReadableMsg;
        switch (decision.decisionStatus()) {
            case "ACCEPT" -> humanReadableMsg = "Congratulations! Loan application automatically APPROVED by automated underwriting.";
            case "REFER_ELIGIBLE" -> humanReadableMsg = "Application referred for secondary manual review. Additional documentation required.";
            case "REFER_INELIGIBLE" -> humanReadableMsg = "Application referred due to credit/LTV guideline deviations.";
            case "DECLINE" -> humanReadableMsg = "Application declined based on automated risk assessment guidelines.";
            default -> humanReadableMsg = "Application evaluated with status: " + decision.decisionStatus();
        }

        LoanDecisionResponse.BridgeHopSummary hopSummary = new LoanDecisionResponse.BridgeHopSummary(
                "HTTP/1.1 REST (JSON)",
                "JMS 2.0 (Artemis Queue)",
                requestQueue,
                replyQueue,
                totalBridgeLatency,
                backendProcessing,
                queueTransit,
                now
        );

        return new LoanDecisionResponse(
                decision.correlationId(),
                decision.evaluationReferenceId(),
                decision.decisionStatus(),
                decision.riskGrade(),
                decision.approvedAmount(),
                decision.recommendedRate(),
                decision.ltv(),
                decision.dti(),
                decision.conditions(),
                humanReadableMsg,
                hopSummary
        );
    }
}
