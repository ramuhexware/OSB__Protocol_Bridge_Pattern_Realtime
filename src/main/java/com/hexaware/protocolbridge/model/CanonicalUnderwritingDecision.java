package com.hexaware.protocolbridge.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Enterprise Canonical Decision Message (returned over JMS Reply Queue).
 * Produced by the backend Automated Underwriting System (AUS / LPA).
 */
@JacksonXmlRootElement(localName = "CanonicalUnderwritingDecision")
public record CanonicalUnderwritingDecision(
        String correlationId,
        String evaluationReferenceId,
        String decisionStatus,      // "ACCEPT", "REFER_ELIGIBLE", "REFER_INELIGIBLE", "DECLINE"
        String riskGrade,           // "LOW_RISK", "MODERATE_RISK", "ELEVATED_RISK"
        BigDecimal approvedAmount,
        BigDecimal recommendedRate, // e.g. 6.25%
        BigDecimal ltv,
        BigDecimal dti,
        int creditScore,
        List<String> conditions,    // e.g. "Provide 2 years W2", "Homeowners Insurance Verification"
        Instant decisionTimestamp,
        String evaluatedByEngine,   // "LPA-AUS-Core-v4.2"
        long processingDurationMs
) {}
