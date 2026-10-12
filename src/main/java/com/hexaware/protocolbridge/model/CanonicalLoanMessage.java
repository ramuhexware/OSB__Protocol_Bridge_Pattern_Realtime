package com.hexaware.protocolbridge.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Enterprise Canonical Data Model (Bridged over JMS Transport).
 * Simulates OSB's Canonical Data Model (CDM) or MISMO mortgage standard format
 * used internally across enterprise asynchronous backends.
 */
@JacksonXmlRootElement(localName = "CanonicalLoanMessage")
public record CanonicalLoanMessage(
        String correlationId,
        String sourceProtocol,       // "HTTP_REST", "WEBSOCKET", etc.
        String targetProtocol,       // "JMS_ARTEMIS"
        Instant bridgeIngressTime,
        String bridgeNodeId,

        // Borrower Details
        String borrowerName,
        String maskedSsn,

        // Financial Variables
        BigDecimal loanAmount,
        BigDecimal propertyValue,
        BigDecimal monthlyIncome,
        BigDecimal monthlyDebt,
        int creditScore,
        String loanPurpose,
        String propertyType,
        int loanTermMonths,

        // Calculated metrics by bridge pre-processor
        BigDecimal loanToValueRatio, // LTV = loanAmount / propertyValue
        BigDecimal debtToIncomeRatio, // DTI = monthlyDebt / monthlyIncome
        
        boolean testTimeoutSimulation
) {}
