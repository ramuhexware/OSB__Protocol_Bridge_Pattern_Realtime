package com.hexaware.protocolbridge.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * Inbound Client Request DTO (HTTP REST / JSON protocol).
 * Represents a mortgage loan application submitted from modern web/mobile portals.
 */
public record LoanApplicationRequest(
        @NotBlank(message = "Borrower name is required")
        String borrowerName,

        @NotBlank(message = "SSN/Identifier is required")
        String borrowerSsn,

        @NotNull(message = "Loan amount is required")
        @DecimalMin(value = "10000.00", message = "Minimum loan amount is $10,000")
        BigDecimal loanAmount,

        @NotNull(message = "Property value is required")
        @DecimalMin(value = "15000.00", message = "Minimum property value is $15,000")
        BigDecimal propertyValue,

        @NotNull(message = "Monthly income is required")
        @DecimalMin(value = "500.00", message = "Minimum monthly income is $500")
        BigDecimal monthlyIncome,

        @NotNull(message = "Monthly debt is required")
        BigDecimal monthlyDebt,

        @Min(value = 300, message = "Minimum FICO score is 300")
        @Max(value = 850, message = "Maximum FICO score is 850")
        int creditScore,

        @NotBlank(message = "Loan purpose is required (PURCHASE / REFINANCE)")
        String loanPurpose,

        @NotBlank(message = "Property type is required (SINGLE_FAMILY / CONDO / MULTI_FAMILY)")
        String propertyType,

        Integer loanTermMonths, // e.g. 360 for 30yr, 180 for 15yr
        
        Boolean simulateTimeout // For testing fault bridging & timeout resilience
) {
    public LoanApplicationRequest {
        if (loanTermMonths == null) {
            loanTermMonths = 360;
        }
        if (simulateTimeout == null) {
            simulateTimeout = false;
        }
    }
}
