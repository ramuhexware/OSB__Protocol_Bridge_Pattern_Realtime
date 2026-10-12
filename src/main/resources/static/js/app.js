/* ==========================================================================
   OSB Protocol Bridge Cockpit - Client Controller & Real-Time Engine
   ========================================================================== */

const scenarios = {
    prime: {
        borrowerName: "Eleanor Vance",
        borrowerSsn: "123-45-7890",
        loanAmount: 380000,
        propertyValue: 500000,
        monthlyIncome: 14500,
        monthlyDebt: 2800,
        creditScore: 775,
        loanPurpose: "PURCHASE",
        propertyType: "SINGLE_FAMILY",
        loanTermMonths: 360,
        simulateTimeout: false
    },
    refer: {
        borrowerName: "Marcus Chen",
        borrowerSsn: "234-56-8901",
        loanAmount: 480000,
        propertyValue: 500000,
        monthlyIncome: 8500,
        monthlyDebt: 3740,
        creditScore: 665,
        loanPurpose: "PURCHASE",
        propertyType: "CONDO",
        loanTermMonths: 360,
        simulateTimeout: false
    },
    decline: {
        borrowerName: "Samantha Reed",
        borrowerSsn: "345-67-9012",
        loanAmount: 420000,
        propertyValue: 450000,
        monthlyIncome: 6000,
        monthlyDebt: 3400,
        creditScore: 610,
        loanPurpose: "REFINANCE",
        propertyType: "SINGLE_FAMILY",
        loanTermMonths: 360,
        simulateTimeout: false
    },
    timeout: {
        borrowerName: "David Sterling (SLA Timeout Test)",
        borrowerSsn: "999-00-1111",
        loanAmount: 350000,
        propertyValue: 480000,
        monthlyIncome: 12000,
        monthlyDebt: 2000,
        creditScore: 740,
        loanPurpose: "PURCHASE",
        propertyType: "SINGLE_FAMILY",
        loanTermMonths: 360,
        simulateTimeout: true
    }
};

let lastPayloadData = null;

function loadScenario(type) {
    const s = scenarios[type];
    if (!s) return;

    document.getElementById("borrowerName").value = s.borrowerName;
    document.getElementById("borrowerSsn").value = s.borrowerSsn;
    document.getElementById("loanAmount").value = s.loanAmount;
    document.getElementById("propertyValue").value = s.propertyValue;
    document.getElementById("monthlyIncome").value = s.monthlyIncome;
    document.getElementById("monthlyDebt").value = s.monthlyDebt;
    document.getElementById("creditScore").value = s.creditScore;
    document.getElementById("loanPurpose").value = s.loanPurpose;
    document.getElementById("propertyType").value = s.propertyType;
    document.getElementById("loanTermMonths").value = s.loanTermMonths;
    document.getElementById("simulateTimeout").checked = s.simulateTimeout;
}

function getFormData() {
    return {
        borrowerName: document.getElementById("borrowerName").value,
        borrowerSsn: document.getElementById("borrowerSsn").value,
        loanAmount: parseFloat(document.getElementById("loanAmount").value),
        propertyValue: parseFloat(document.getElementById("propertyValue").value),
        monthlyIncome: parseFloat(document.getElementById("monthlyIncome").value),
        monthlyDebt: parseFloat(document.getElementById("monthlyDebt").value),
        creditScore: parseInt(document.getElementById("creditScore").value),
        loanPurpose: document.getElementById("loanPurpose").value,
        propertyType: document.getElementById("propertyType").value,
        loanTermMonths: parseInt(document.getElementById("loanTermMonths").value),
        simulateTimeout: document.getElementById("simulateTimeout").checked
    };
}

async function animatePipelineHop(nodeId, delayMs = 200) {
    const el = document.getElementById(nodeId);
    if (el) {
        el.classList.add("active-pulse");
        await new Promise(r => setTimeout(r, delayMs));
        el.classList.remove("active-pulse");
    }
}

async function triggerPipelineAnimation() {
    await animatePipelineHop("node-client", 150);
    await animatePipelineHop("node-adapter-in", 150);
    await animatePipelineHop("node-bridge-core", 200);
    await animatePipelineHop("node-req-queue", 200);
    await animatePipelineHop("node-aus-engine", 350);
    await animatePipelineHop("node-reply-queue", 200);
    await animatePipelineHop("node-bridge-out", 180);
}

async function submitLoanApplication(isAsync = false) {
    const data = getFormData();
    const resultBox = document.getElementById("result-container");
    const syncBtn = document.getElementById("btn-submit-sync");
    const asyncBtn = document.getElementById("btn-submit-async");

    syncBtn.disabled = true;
    asyncBtn.disabled = true;

    resultBox.innerHTML = `
        <div style="text-align: center; color: var(--accent-cyan);">
            <div style="font-size: 1.5rem; animation: pulse 1s infinite;">⚡</div>
            <div style="font-weight: 600; margin-top: 8px;">Mediating Protocol Across Inbound REST ➔ JMS Queue...</div>
            <div style="font-size: 0.75rem; color: var(--text-dim); margin-top: 4px;">Awaiting correlated response from AUS Underwriting Engine</div>
        </div>
    `;

    // Start visual hop animation concurrently
    triggerPipelineAnimation();

    const endpoint = isAsync ? "/api/v1/bridge/loan-application/async" : "/api/v1/bridge/loan-application/sync";

    try {
        const response = await fetch(endpoint, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-Correlation-ID": "CORR-" + Math.random().toString(36).substring(2, 9).toUpperCase()
            },
            body: JSON.stringify(data)
        });

        const json = await response.json();
        lastPayloadData = json;
        renderPayloadInspector(json);

        if (response.ok) {
            if (isAsync) {
                renderAsyncResult(json);
            } else {
                renderSyncResult(json);
            }
        } else {
            renderErrorResult(json);
        }

    } catch (err) {
        renderErrorResult({
            title: "Network / Connection Error",
            detail: err.message,
            status: 500
        });
    } finally {
        syncBtn.disabled = false;
        asyncBtn.disabled = false;
        fetchMetrics();
        fetchAuditLogs();
    }
}

function renderSyncResult(res) {
    const box = document.getElementById("result-container");
    const statusClass = res.decisionStatus || "ACCEPT";

    let conditionsHtml = "";
    if (res.conditions && res.conditions.length > 0) {
        conditionsHtml = `
            <div style="margin-top: 10px;">
                <div style="font-size: 0.72rem; color: var(--text-dim); text-transform: uppercase;">Underwriting Conditions:</div>
                <ul style="font-size: 0.76rem; color: var(--text-muted); margin-left: 18px; margin-top: 4px;">
                    ${res.conditions.map(c => `<li>${c}</li>`).join("")}
                </ul>
            </div>
        `;
    }

    box.innerHTML = `
        <div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span class="decision-badge ${statusClass}">
                    <span>${statusClass === 'ACCEPT' ? '✓' : statusClass === 'DECLINE' ? '✗' : '⚠'}</span>
                    ${statusClass} - ${res.riskGrade || ''}
                </span>
                <span style="font-family: var(--font-mono); font-size: 0.75rem; color: var(--text-dim);">
                    ID: ${res.correlationId}
                </span>
            </div>

            <p style="font-size: 0.88rem; font-weight: 500; margin-bottom: 10px;">${res.message}</p>

            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 12px; font-family: var(--font-mono); font-size: 0.78rem;">
                <div style="background: rgba(255,255,255,0.03); padding: 8px; border-radius: 6px;">
                    <div style="color: var(--text-dim); font-size: 0.68rem;">APPROVED AMOUNT</div>
                    <div style="font-weight: 700; color: #34d399;">$${(res.approvedAmount || 0).toLocaleString()}</div>
                </div>
                <div style="background: rgba(255,255,255,0.03); padding: 8px; border-radius: 6px;">
                    <div style="color: var(--text-dim); font-size: 0.68rem;">LPA RATE</div>
                    <div style="font-weight: 700; color: var(--accent-cyan);">${res.recommendedRate}%</div>
                </div>
                <div style="background: rgba(255,255,255,0.03); padding: 8px; border-radius: 6px;">
                    <div style="color: var(--text-dim); font-size: 0.68rem;">LTV RATIO</div>
                    <div>${res.ltvPercentage}%</div>
                </div>
                <div style="background: rgba(255,255,255,0.03); padding: 8px; border-radius: 6px;">
                    <div style="color: var(--text-dim); font-size: 0.68rem;">DTI RATIO</div>
                    <div>${res.dtiPercentage}%</div>
                </div>
            </div>

            ${conditionsHtml}

            <!-- Protocol Hop Telemetry -->
            <div class="hop-telemetry-strip">
                <div class="hop-item">
                    <span>TOTAL BRIDGE SLA</span>
                    <strong style="color: var(--accent-emerald);">${res.bridgeHopSummary.totalBridgeLatencyMs} ms</strong>
                </div>
                <div class="hop-item">
                    <span>BACKEND AUS ENGINE</span>
                    <strong>${res.bridgeHopSummary.backendProcessingMs} ms</strong>
                </div>
                <div class="hop-item">
                    <span>QUEUE TRANSIT HOP</span>
                    <strong>${res.bridgeHopSummary.queueHopTransitMs} ms</strong>
                </div>
            </div>
        </div>
    `;
}

function renderAsyncResult(res) {
    const box = document.getElementById("result-container");
    box.innerHTML = `
        <div style="text-align: center;">
            <div style="font-size: 1.5rem; color: var(--accent-cyan);">📨</div>
            <div class="decision-badge" style="background: rgba(6, 182, 212, 0.15); border-color: var(--accent-cyan); color: #22d3ee; margin-top: 8px;">
                HTTP 202 ACCEPTED
            </div>
            <div style="font-size: 0.88rem; font-weight: 600; margin-top: 6px;">Asynchronous Ingestion Successful</div>
            <p style="font-size: 0.78rem; color: var(--text-muted); margin-top: 6px;">
                Message dispatched to queue. Tracking Ticket: <code style="color: #93c5fd;">${res.correlationId}</code>
            </p>
            <div style="font-size: 0.74rem; color: var(--text-dim); margin-top: 8px;">
                Notification channel: <strong>${res.notificationChannel}</strong>
            </div>
        </div>
    `;
}

function renderErrorResult(err) {
    const box = document.getElementById("result-container");
    const isTimeout = err.status === 504 || (err.title && err.title.includes("Timeout"));

    box.innerHTML = `
        <div style="text-align: center;">
            <div style="font-size: 1.5rem; color: var(--accent-rose);">⚠️</div>
            <div class="decision-badge DECLINE" style="margin-top: 8px;">
                ${err.status || 500} - ${err.title || "Bridge Fault"}
            </div>
            <div style="font-size: 0.88rem; font-weight: 500; margin-top: 8px; color: #fca5a5;">
                ${err.detail || "Error while bridging between protocols"}
            </div>
            ${err.recommendation ? `
                <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 8px;">
                    <em>Remediation:</em> ${err.recommendation}
                </div>
            ` : ''}
        </div>
    `;
}

function renderPayloadInspector(data) {
    const box = document.getElementById("payload-inspector-box");
    box.textContent = JSON.stringify(data, null, 2);
}

function switchTab(tabId) {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach(c => c.style.display = "none");

    const targetContent = document.getElementById("tab-" + tabId);
    if (targetContent) targetContent.style.display = "block";

    event.target.classList.add("active");
}

async function fetchMetrics() {
    try {
        const res = await fetch("/api/v1/bridge/metrics");
        if (!res.ok) return;
        const m = await res.json();

        document.getElementById("metric-total-requests").innerText = m.totalRequestsReceived;
        
        const successRate = m.totalRequestsReceived === 0 ? 100 : Math.round((m.totalSuccessBridged / m.totalRequestsReceived) * 100);
        document.getElementById("metric-success-rate").innerText = successRate + "%";
        document.getElementById("metric-success-count").innerText = `${m.totalSuccessBridged} successful / ${m.totalRequestsReceived}`;

        document.getElementById("metric-avg-latency").innerText = m.averageLatencyMs + " ms";
        document.getElementById("metric-minmax-latency").innerText = `Min: ${m.minLatencyMs}ms | Max: ${m.maxLatencyMs}ms`;
        document.getElementById("metric-active-correlations").innerText = m.activePendingCorrelations;
        document.getElementById("metric-timeouts").innerText = m.totalTimeouts;

    } catch (e) {
        // silent fallback
    }
}

async function fetchAuditLogs() {
    try {
        const res = await fetch("/api/v1/bridge/audit-logs");
        if (!res.ok) return;
        const logs = await res.json();
        const tbody = document.getElementById("audit-table-body");

        if (logs.length === 0) return;

        tbody.innerHTML = logs.slice(0, 15).map(l => {
            let pillClass = "info";
            if (l.stage.includes("COMPLETED") || l.stage.includes("RESOLVED")) pillClass = "success";
            else if (l.stage.includes("TIMEOUT")) pillClass = "warn";
            else if (l.stage.includes("ERROR") || l.stage.includes("FAULT")) pillClass = "danger";

            const elapsed = l.durationFromIngressMs !== null ? `${l.durationFromIngressMs} ms` : "-";

            return `
                <tr>
                    <td><span class="stage-pill ${pillClass}">${l.stage}</span></td>
                    <td style="color: var(--text-dim);">${l.protocol}</td>
                    <td style="color: #93c5fd;">${l.correlationId}</td>
                    <td style="color: var(--accent-emerald);">${elapsed}</td>
                    <td style="color: var(--text-muted); max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${l.details}">
                        ${l.details}
                    </td>
                </tr>
            `;
        }).join("");

    } catch (e) {
        // silent fallback
    }
}

// Initial boot & periodic auto-poll
window.addEventListener("DOMContentLoaded", () => {
    fetchMetrics();
    fetchAuditLogs();
    setInterval(fetchMetrics, 1500);
    setInterval(fetchAuditLogs, 1500);
});
