package com.hexaware.protocolbridge.backend;

import com.hexaware.protocolbridge.bridge.AuditAndMetricsService;
import com.hexaware.protocolbridge.model.BridgeAuditLog;
import com.hexaware.protocolbridge.model.CanonicalLoanMessage;
import com.hexaware.protocolbridge.model.CanonicalUnderwritingDecision;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Backend Enterprise Automated Underwriting System (AUS / LPA Engine Simulator).
 * Simulates a legacy/core enterprise underwriting engine connected via JMS:
 * - Listens asynchronously on the Request Queue
 * - Evaluates underwriting guidelines (FICO, LTV, DTI, Risk Tier)
 * - Returns Canonical Decision to the Reply Queue with matching JMSCorrelationID
 */
@Service
public class AusUnderwritingEngine {

    private static final Logger log = LoggerFactory.getLogger(AusUnderwritingEngine.class);

    private final JmsTemplate jmsTemplate;
    private final MessageConverter messageConverter;
    private final AuditAndMetricsService auditService;

    @Value("${bridge.reply-queue:freddie.lpa.loan.reply.queue}")
    private String replyQueueName;

    @Value("${bridge.simulate-processing-delay-ms:350}")
    private long defaultProcessingDelayMs;

    public AusUnderwritingEngine(
            JmsTemplate jmsTemplate,
            MessageConverter messageConverter,
            AuditAndMetricsService auditService) {
        this.jmsTemplate = jmsTemplate;
        this.messageConverter = messageConverter;
        this.auditService = auditService;
    }

    @JmsListener(
            destination = "${bridge.request-queue:freddie.lpa.loan.request.queue}",
            containerFactory = "queueListenerFactory"
    )
    public void processUnderwritingRequest(Message message) {
        long startTime = System.currentTimeMillis();

        try {
            String correlationId = message.getJMSCorrelationID();
            Object payload = messageConverter.fromMessage(message);

            if (!(payload instanceof CanonicalLoanMessage loanMsg)) {
                log.error("AUS Engine received unrecognized message type: {}", payload);
                return;
            }

            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.BACKEND_ENGINE_PROCESSING,
                    "ENTERPRISE_JMS_BACKEND",
                    "AUS Underwriting Engine started processing loan for: " + loanMsg.borrowerName(),
                    "FICO=" + loanMsg.creditScore() + ", LTV=" + loanMsg.loanToValueRatio() + "%, DTI=" + loanMsg.debtToIncomeRatio() + "%",
                    System.currentTimeMillis() - startTime
            );

            // Handle timeout simulation if requested
            if (loanMsg.testTimeoutSimulation()) {
                log.warn("Test timeout simulation flag detected for correlation [{}]. Sleeping for 8 seconds...", correlationId);
                Thread.sleep(8000);
            } else if (defaultProcessingDelayMs > 0) {
                // Realistic asynchronous processing latency
                Thread.sleep(defaultProcessingDelayMs);
            }

            // Execute Automated Underwriting Decision Rules
            CanonicalUnderwritingDecision decision = evaluateLoan(loanMsg, startTime);

            // Dispatch response back to Reply Queue with identical JMSCorrelationID
            jmsTemplate.convertAndSend(replyQueueName, decision, replyMsg -> {
                replyMsg.setJMSCorrelationID(correlationId);
                replyMsg.setStringProperty("X_PROCESSED_BY", "LPA-AUS-Core-v4.2");
                replyMsg.setLongProperty("X_PROCESSING_TIME_MS", decision.processingDurationMs());
                return replyMsg;
            });

            auditService.recordHop(
                    correlationId,
                    BridgeAuditLog.HopStage.JMS_REPLY_DISPATCH,
                    "JMS_ARTEMIS",
                    "AUS Engine dispatched decision to reply queue [" + replyQueueName + "]",
                    "DecisionStatus=" + decision.decisionStatus() + ", Ref=" + decision.evaluationReferenceId(),
                    System.currentTimeMillis() - startTime
            );

            log.info("AUS Engine completed decision for [{}] with status [{}] in {} ms",
                    correlationId, decision.decisionStatus(), decision.processingDurationMs());

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (JMSException e) {
            log.error("JMS Exception in AUS Engine processing", e);
            auditService.recordError();
        }
    }

    /**
     * Core Rule Engine for Mortgage Underwriting (LPA / AUS Standard Rules)
     */
    private CanonicalUnderwritingDecision evaluateLoan(CanonicalLoanMessage loan, long startTimestamp) {
        String evalId = "LPA-" + UUID.randomUUID().toString().substring(0, 10).toUpperCase();
        List<String> conditions = new ArrayList<>();
        String status;
        String riskGrade;
        BigDecimal baseRate = BigDecimal.valueOf(6.125); // Current baseline mortgage rate

        double ltv = loan.loanToValueRatio().doubleValue();
        double dti = loan.debtToIncomeRatio().doubleValue();
        int fico = loan.creditScore();

        // 1. Credit Score Evaluation
        if (fico < 620) {
            status = "DECLINE";
            riskGrade = "UNACCEPTABLE_RISK";
            conditions.add("Credit score below minimum 620 underwriting guideline threshold.");
        } else if (dti > 50.0) {
            status = "DECLINE";
            riskGrade = "HIGH_DEBT_RISK";
            conditions.add("Debt-to-Income (DTI) ratio (" + dti + "%) exceeds the 50.0% ceiling.");
        } else if (fico >= 740 && ltv <= 80.0 && dti <= 36.0) {
            // Prime Low-Risk Approval
            status = "ACCEPT";
            riskGrade = "LOW_RISK_PRIME";
            conditions.add("Standard borrower asset & income documentation.");
            conditions.add("Property appraisal verification.");
        } else if (fico >= 680 && ltv <= 95.0 && dti <= 45.0) {
            // Standard Conforming Approval
            status = "ACCEPT";
            riskGrade = "MODERATE_RISK";
            baseRate = baseRate.add(BigDecimal.valueOf(0.375));
            if (ltv > 80.0) {
                conditions.add("Borrower-paid or Lender-paid Private Mortgage Insurance (PMI) required for LTV > 80%.");
            }
            conditions.add("Provide 2 years consecutive W-2 statements and 30 days paystubs.");
            conditions.add("Verify 2 months post-closing principal, interest, taxes, insurance reserves.");
        } else if (fico >= 620 && ltv <= 97.0 && dti <= 50.0) {
            // Refer to Manual Underwriting
            status = "REFER_ELIGIBLE";
            riskGrade = "ELEVATED_RISK";
            baseRate = baseRate.add(BigDecimal.valueOf(0.875));
            conditions.add("Manual underwriter review required for high risk metrics.");
            conditions.add("Detailed letter of explanation for credit inquiries.");
            conditions.add("Full appraisal with interior and exterior inspection required.");
        } else {
            status = "REFER_INELIGIBLE";
            riskGrade = "GUIDELINE_EXCEEDING";
            conditions.add("Application metrics do not satisfy automated underwriting matrix.");
        }

        BigDecimal approvedAmount = "ACCEPT".equals(status) || "REFER_ELIGIBLE".equals(status)
                ? loan.loanAmount()
                : BigDecimal.ZERO;

        BigDecimal recommendedRate = baseRate.setScale(3, RoundingMode.HALF_UP);
        long processingDuration = System.currentTimeMillis() - startTimestamp;

        return new CanonicalUnderwritingDecision(
                loan.correlationId(),
                evalId,
                status,
                riskGrade,
                approvedAmount,
                recommendedRate,
                loan.loanToValueRatio(),
                loan.debtToIncomeRatio(),
                loan.creditScore(),
                conditions,
                Instant.now(),
                "LPA-AUS-Core-v4.2",
                processingDuration
        );
    }
}
