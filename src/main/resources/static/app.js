// DMS Frontend Application Logic (Vanilla JS)

let localNode = null;
let clusterNodes = [];
let allEvents = [];

document.addEventListener("DOMContentLoaded", () => {
  setupTabs();
  setupSSE();
  fetchCluster();
  fetchLedger();
  fetchLbConfig();
  fetchElectionStatus();
  fetchReplConfig();
  fetchFtStatus();
  fetchThreadStatus();
  fetchClockStatus();
  fetchMapReduceShards();

  // Periodic refresh
  setInterval(fetchCluster, 3000);
  setInterval(fetchLedger, 5000);
  setInterval(fetchLbConfig, 2000);
  setInterval(fetchElectionStatus, 3000);
  setInterval(fetchFtStatus, 3000);
  setInterval(fetchThreadStatus, 3000);
  setInterval(fetchClockStatus, 3000);
  setInterval(fetchMapReduceShards, 5000);

  // Event Listeners
  document.getElementById("btn-refresh-cluster").addEventListener("click", fetchCluster);
  document.getElementById("btn-refresh-ledger").addEventListener("click", fetchLedger);
  document.getElementById("btn-clear-log").addEventListener("click", clearLog);
  document.getElementById("log-filter-tag").addEventListener("change", renderFilteredLogs);

  document.getElementById("form-donate").addEventListener("submit", handleDonate);
  document.getElementById("form-branch-query").addEventListener("submit", handleBranchQuery);

  // Exp 6 Load Balancing Listeners
  document.querySelectorAll('input[name="lb-strategy-radio"]').forEach(r => {
    r.addEventListener("change", handleStrategyChange);
  });
  document.getElementById("btn-update-delay").addEventListener("click", handleUpdateDelay);
  document.getElementById("btn-send-burst").addEventListener("click", handleSendBurst);
  document.getElementById("btn-refresh-lb-conns").addEventListener("click", fetchLbConfig);
  document.getElementById("btn-clear-lb-results").addEventListener("click", () => {
    document.getElementById("lb-results-tbody").innerHTML = `<tr><td colspan="9" style="text-align:center;">Results cleared.</td></tr>`;
    document.getElementById("lb-distribution-summary").style.display = "none";
  });
  document.getElementById("input-burst-count").addEventListener("input", (e) => {
    const val = e.target.value;
    document.getElementById("btn-send-burst").textContent = `Send ${val} Requests`;
  });

  // Exp 4 Election Listeners
  const btnElection = document.getElementById("btn-trigger-election");
  if (btnElection) btnElection.addEventListener("click", handleTriggerElection);
  const btnRefreshElection = document.getElementById("btn-refresh-election");
  if (btnRefreshElection) btnRefreshElection.addEventListener("click", fetchElectionStatus);

  // Exp 5 Replication Listeners
  document.querySelectorAll('input[name="repl-mode-radio"]').forEach(r => {
    r.addEventListener("change", handleReplModeChange);
  });
  const btnSetReplDelay = document.getElementById("btn-set-repl-delay");
  if (btnSetReplDelay) btnSetReplDelay.addEventListener("click", handleSetReplDelay);
  const btnRunStaleDemo = document.getElementById("btn-run-stale-demo");
  if (btnRunStaleDemo) btnRunStaleDemo.addEventListener("click", handleRunStaleDemo);
  const btnRunConsistency = document.getElementById("btn-run-consistency");
  if (btnRunConsistency) btnRunConsistency.addEventListener("click", fetchConsistency);

  // Exp 8 Fault Tolerance Listeners
  const btnFtCrash = document.getElementById("btn-ft-crash");
  if (btnFtCrash) btnFtCrash.addEventListener("click", handleSimulateCrash);
  const btnFtResume = document.getElementById("btn-ft-resume");
  if (btnFtResume) btnFtResume.addEventListener("click", handleResumeNode);
  const btnFtManualSync = document.getElementById("btn-ft-manual-sync");
  if (btnFtManualSync) btnFtManualSync.addEventListener("click", handleManualSnapshotSync);
  const btnRefreshFt = document.getElementById("btn-refresh-ft");
  if (btnRefreshFt) btnRefreshFt.addEventListener("click", fetchFtStatus);

  // Exp 2 Multithreading Listeners
  const btnSetPoolSize = document.getElementById("btn-set-pool-size");
  if (btnSetPoolSize) btnSetPoolSize.addEventListener("click", handleSetPoolSize);
  const btnRunTpBurst = document.getElementById("btn-run-tp-burst");
  if (btnRunTpBurst) btnRunTpBurst.addEventListener("click", handleRunThreadBurst);
  const btnRunRace = document.getElementById("btn-run-race-demo");
  if (btnRunRace) btnRunRace.addEventListener("click", handleRunRaceDemo);
  const btnClearTp = document.getElementById("btn-clear-tp-results");
  if (btnClearTp) btnClearTp.addEventListener("click", clearTpResults);

  // Exp 3 Clocks Listeners
  const btnRefreshClock = document.getElementById("btn-refresh-clock");
  if (btnRefreshClock) btnRefreshClock.addEventListener("click", fetchClockStatus);
  const btnSetSkew = document.getElementById("btn-set-clock-skew");
  if (btnSetSkew) btnSetSkew.addEventListener("click", handleSetClockSkew);
  const btnResetClock = document.getElementById("btn-reset-clock");
  if (btnResetClock) btnResetClock.addEventListener("click", handleResetClock);
  const btnRunBerkeley = document.getElementById("btn-run-berkeley");
  if (btnRunBerkeley) btnRunBerkeley.addEventListener("click", handleRunBerkeleySync);

  // Exp 7 MapReduce Listeners
  const btnRunMr = document.getElementById("btn-run-mr");
  if (btnRunMr) btnRunMr.addEventListener("click", handleRunMapReduceJob);
});

// TAB NAVIGATION
function setupTabs() {
  const tabs = document.querySelectorAll("nav.tabs .tab-btn:not(.disabled)");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");

      const targetId = tab.getAttribute("data-tab");
      document.querySelectorAll(".tab-pane").forEach(pane => {
        pane.classList.remove("active");
      });
      const targetPane = document.getElementById(targetId);
      if (targetPane) {
        targetPane.classList.add("active");
      }
    });
  });
}

// FETCH CLUSTER STATE
async function fetchCluster() {
  try {
    const res = await fetch("/api/cluster");
    if (!res.ok) return;
    const data = await res.json();
    localNode = data.localNode;
    clusterNodes = data.nodes || [];
    const coordinatorId = data.coordinatorId;

    // Update Header
    if (localNode) {
      document.getElementById("hdr-node-id").textContent = `Node: ${localNode.nodeId}`;
      document.getElementById("hdr-node-host").textContent = `Host: ${localNode.host}:${localNode.serverPort} (RMI:${localNode.rmiPort})`;
      document.getElementById("hdr-node-os").textContent = `OS: ${localNode.os}`;
      document.getElementById("hdr-lamport").textContent = `Lamport: ${localNode.lamportTs}`;

      const isPrimary = localNode.nodeId === coordinatorId;
      const roleElem = document.getElementById("hdr-role");
      roleElem.textContent = isPrimary ? "Primary (Coord)" : "Backup";
      roleElem.className = isPrimary ? "badge badge-primary" : "badge badge-green";
    }

    // Render Cluster Table
    renderClusterTable(coordinatorId);
    updateNodeDropdowns();
  } catch (err) {
    console.error("Failed to fetch cluster state", err);
  }
}

function renderClusterTable(coordinatorId) {
  const tbody = document.getElementById("cluster-tbody");
  tbody.innerHTML = "";

  if (clusterNodes.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;">No nodes found</td></tr>`;
    return;
  }

  clusterNodes.forEach(node => {
    const tr = document.createElement("tr");
    const isSelf = localNode && node.nodeId === localNode.nodeId;
    const isPrimary = node.nodeId === coordinatorId;

    const statusBadge = node.alive
      ? `<span class="badge badge-green">ALIVE</span>`
      : `<span class="badge badge-red">DEAD</span>`;

    const roleBadge = isPrimary
      ? `<span class="badge badge-primary">PRIMARY</span>`
      : `<span class="badge">BACKUP</span>`;

    tr.innerHTML = `
      <td><strong>Node ${node.nodeId}</strong> ${isSelf ? '<span class="badge" style="font-size:0.65rem;">SELF</span>' : ''}</td>
      <td>${node.host}</td>
      <td>${node.serverPort}</td>
      <td>${node.rmiPort}</td>
      <td>${node.os || 'Unknown'}</td>
      <td><strong>${node.lamportTs}</strong></td>
      <td>${node.activeConnections}</td>
      <td>${roleBadge}</td>
      <td>${statusBadge}</td>
    `;
    tbody.appendChild(tr);
  });
}

function updateNodeDropdowns() {
  const donateSelect = document.getElementById("select-target-node");
  const querySelect = document.getElementById("query-target-node");

  const prevDonateVal = donateSelect.value;
  const prevQueryVal = querySelect.value;

  donateSelect.innerHTML = `<option value="">Auto / Local Node</option>`;
  querySelect.innerHTML = `<option value="">Local Node</option>`;

  clusterNodes.forEach(node => {
    if (node.alive) {
      const isSelf = localNode && node.nodeId === localNode.nodeId;
      const optDonate = document.createElement("option");
      optDonate.value = node.nodeId;
      optDonate.textContent = `Node ${node.nodeId} (${node.host}:${node.rmiPort}) ${isSelf ? '[Self]' : '[RMI Peer]'}`;
      donateSelect.appendChild(optDonate);

      const optQuery = document.createElement("option");
      optQuery.value = node.nodeId;
      optQuery.textContent = `Node ${node.nodeId} (${node.host}:${node.rmiPort}) ${isSelf ? '[Self]' : '[RMI Peer]'}`;
      querySelect.appendChild(optQuery);
    }
  });

  if (prevDonateVal) donateSelect.value = prevDonateVal;
  if (prevQueryVal) querySelect.value = prevQueryVal;
}

// DONATE SUBMISSION
async function handleDonate(e) {
  e.preventDefault();
  const donor = document.getElementById("input-donor").value.trim();
  const amount = parseFloat(document.getElementById("input-amount").value);
  const branch = document.getElementById("select-branch").value;
  const targetNodeVal = document.getElementById("select-target-node").value;
  const targetNodeId = targetNodeVal ? parseInt(targetNodeVal, 10) : null;

  const btn = document.getElementById("btn-submit-donation");
  btn.disabled = true;
  btn.textContent = "Processing via RMI...";

  const payload = {
    donor,
    amount,
    branch,
    targetNodeId,
    isolated: false
  };

  try {
    const res = await fetch("/api/donate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const receipt = await res.json();

    const box = document.getElementById("donate-result-box");
    box.style.display = "flex";

    const pathElem = document.getElementById("donate-trace-path");
    const isRemote = localNode && receipt.processingNodeId !== localNode.nodeId;
    if (isRemote) {
      pathElem.textContent = `TRACE: Client -> Gateway Node ${localNode.nodeId} (${localNode.host}) ==[Java RMI]==> Processing Node ${receipt.processingNodeId} (${receipt.host})`;
    } else {
      pathElem.textContent = `TRACE: Client -> Gateway Node ${receipt.processingNodeId} (${receipt.host}) [Local Execution]`;
    }

    document.getElementById("rec-status").textContent = receipt.status;
    document.getElementById("rec-node").textContent = `Node ${receipt.processingNodeId}`;
    document.getElementById("rec-host").textContent = receipt.host;
    document.getElementById("rec-os").textContent = receipt.os;
    document.getElementById("rec-thread").textContent = receipt.threadName;
    document.getElementById("rec-lamport").textContent = receipt.lamportTs;
    document.getElementById("rec-primary").textContent = `Node ${receipt.primaryId}`;
    document.getElementById("rec-msg").textContent = receipt.message;

    fetchCluster();
    fetchLedger();
  } catch (err) {
    alert("Donation submission error: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Submit Donation via RMI";
  }
}

// BRANCH TOTAL QUERY
async function handleBranchQuery(e) {
  e.preventDefault();
  const branch = document.getElementById("query-branch").value;
  const targetNodeVal = document.getElementById("query-target-node").value;
  const targetNodeId = targetNodeVal ? parseInt(targetNodeVal, 10) : null;

  const btn = document.getElementById("btn-query-total");
  btn.disabled = true;
  btn.textContent = "Querying RMI...";

  let url = `/api/branch-total?branch=${encodeURIComponent(branch)}`;
  if (targetNodeId) {
    url += `&targetNodeId=${targetNodeId}`;
  }

  try {
    const res = await fetch(url);
    const data = await res.json();

    const box = document.getElementById("ngo-result-box");
    box.style.display = "flex";

    document.getElementById("ngo-trace-path").textContent = `TRACE: ${data.callPath || 'RMI Query'}`;
    document.getElementById("ngo-total-val").textContent = `${data.branch} Total: ₹${data.total != null ? data.total : '0.00'}`;
  } catch (err) {
    alert("Query error: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Query Total via RMI";
  }
}

// FETCH LEDGER
async function fetchLedger() {
  try {
    const res = await fetch("/api/ledger");
    if (!res.ok) return;
    const donations = await res.json();

    const tbody = document.getElementById("ledger-tbody");
    tbody.innerHTML = "";

    if (!donations || donations.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;">No records committed to this node yet.</td></tr>`;
      return;
    }

    donations.forEach(d => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${d.id}</td>
        <td><strong>${escapeHtml(d.donor)}</strong></td>
        <td>₹${d.amount}</td>
        <td><span class="badge">${escapeHtml(d.branch)}</span></td>
        <td>${d.lamportTs}</td>
        <td><code>${escapeHtml(d.receiptId)}</code></td>
        <td>${escapeHtml(d.handledBy)}</td>
        <td><span class="badge badge-green">${escapeHtml(d.status)}</span></td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error("Failed to fetch ledger", err);
  }
}

// SERVER-SENT EVENTS
function setupSSE() {
  const evtSource = new EventSource("/api/events");

  evtSource.addEventListener("log", (e) => {
    try {
      const event = JSON.parse(e.data);
      allEvents.push(event);
      if (allEvents.length > 500) allEvents.shift();

      // Update header Lamport TS if higher
      if (localNode && event.lamportTs > localNode.lamportTs) {
        localNode.lamportTs = event.lamportTs;
        document.getElementById("hdr-lamport").textContent = `Lamport: ${event.lamportTs}`;
      }

      appendLogLine(event);
    } catch (err) {
      console.error("SSE parse error", err);
    }
  });

  evtSource.onerror = (err) => {
    console.warn("SSE connection error, retrying...", err);
  };
}

function appendLogLine(event) {
  const filter = document.getElementById("log-filter-tag").value;
  if (filter !== "ALL" && event.tag !== filter) {
    return;
  }

  const logWindow = document.getElementById("log-window");
  const line = document.createElement("div");
  line.className = "log-line";

  const tagClass = `log-tag-${event.tag}`;
  line.innerHTML = `<span class="${tagClass}">[${escapeHtml(event.tag)}]</span> <span style="color:#64748b;">node=${event.nodeId} lamport=${event.lamportTs}</span> ${escapeHtml(event.message)}`;

  logWindow.appendChild(line);

  if (document.getElementById("log-autoscroll").checked) {
    logWindow.scrollTop = logWindow.scrollHeight;
  }

  // Also route EXP4-ELECT to Bully election trace box
  if (event.tag === "EXP4-ELECT") {
    const traceBox = document.getElementById("election-trace-box");
    if (traceBox) {
      if (traceBox.textContent.includes("Waiting for election event...")) {
        traceBox.innerHTML = "";
      }
      const div = document.createElement("div");
      div.innerHTML = `<span style="color:var(--accent-amber); font-weight:600;">[EXP4-ELECT]</span> <span style="color:#64748b;">lamport=${event.lamportTs}</span> ${escapeHtml(event.message)}`;
      traceBox.appendChild(div);
      traceBox.scrollTop = traceBox.scrollHeight;
    }
  }
}

function renderFilteredLogs() {
  const logWindow = document.getElementById("log-window");
  logWindow.innerHTML = "";
  allEvents.forEach(evt => appendLogLine(evt));
}

function clearLog() {
  allEvents = [];
  document.getElementById("log-window").innerHTML = "";
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// EXP 6: LOAD BALANCING FUNCTIONS

async function fetchLbConfig() {
  try {
    const res = await fetch("/api/lb/config");
    if (!res.ok) return;
    const data = await res.json();

    // Update strategy radio if not focused
    if (data.strategy) {
      const radio = document.querySelector(`input[name="lb-strategy-radio"][value="${data.strategy}"]`);
      if (radio && !radio.matches(":focus")) {
        radio.checked = true;
      }
    }

    // Update delay input if not focused
    const delayInput = document.getElementById("input-lb-delay");
    if (delayInput && !delayInput.matches(":focus") && data.artificialDelayMs !== undefined) {
      delayInput.value = data.artificialDelayMs;
    }

    // Render connection counters
    renderLbCounters(data.activeConnections || {});
  } catch (err) {
    console.error("Failed to fetch LB config", err);
  }
}

function renderLbCounters(activeConns) {
  const container = document.getElementById("lb-counters-container");
  if (!container) return;

  const nodeIds = Object.keys(activeConns).map(k => parseInt(k, 10)).sort((a, b) => a - b);
  if (nodeIds.length === 0) {
    container.innerHTML = `<div style="color:var(--text-muted); font-size:0.8rem;">No live nodes detected.</div>`;
    return;
  }

  container.innerHTML = "";
  nodeIds.forEach(id => {
    const count = activeConns[id] || 0;
    const isSelf = localNode && localNode.nodeId === id;
    const isBusy = count > 0;

    const card = document.createElement("div");
    card.className = "receipt-item";
    card.style.display = "flex";
    card.style.flexDirection = "column";
    card.style.gap = "0.2rem";
    card.style.border = isBusy ? "1px solid var(--accent-amber)" : "1px solid rgba(255,255,255,0.08)";
    card.style.background = isBusy ? "rgba(245, 158, 11, 0.1)" : "#1e293b";

    card.innerHTML = `
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <span style="font-weight:700; font-size:0.85rem; color:var(--text-main);">Node ${id}</span>
        ${isSelf ? '<span class="badge" style="font-size:0.6rem; padding:0.1rem 0.3rem;">SELF</span>' : ''}
      </div>
      <div class="lbl">Active Connections</div>
      <div class="val" style="font-size:1.1rem; color:${isBusy ? 'var(--accent-amber)' : 'var(--accent-blue)'}; display:flex; align-items:center; gap:0.4rem;">
        ${isBusy ? '<span class="log-pulse" style="background:var(--accent-amber); box-shadow:0 0 6px var(--accent-amber);"></span>' : ''}
        <span>${count}</span>
      </div>
    `;
    container.appendChild(card);
  });
}

async function handleStrategyChange(e) {
  const strategy = e.target.value;
  try {
    await fetch("/api/lb/strategy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ strategy })
    });
    fetchLbConfig();
  } catch (err) {
    console.error("Failed to update LB strategy", err);
  }
}

async function handleUpdateDelay() {
  const delayInput = document.getElementById("input-lb-delay");
  const delayMs = parseInt(delayInput.value, 10) || 0;
  const btn = document.getElementById("btn-update-delay");
  btn.disabled = true;

  try {
    await fetch("/api/lb/delay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ delayMs })
    });
    fetchLbConfig();
  } catch (err) {
    console.error("Failed to update artificial delay", err);
  } finally {
    btn.disabled = false;
  }
}

async function handleSendBurst() {
  const countInput = document.getElementById("input-burst-count");
  const count = parseInt(countInput.value, 10) || 12;

  const strategyRadio = document.querySelector('input[name="lb-strategy-radio"]:checked');
  const strategy = strategyRadio ? strategyRadio.value : "LEAST_CONNECTIONS";

  const delayInput = document.getElementById("input-lb-delay");
  const delayMs = parseInt(delayInput.value, 10) || 0;

  const btn = document.getElementById("btn-send-burst");
  btn.disabled = true;
  btn.textContent = `Sending ${count} concurrent requests...`;

  try {
    const res = await fetch("/api/lb/burst", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count, strategy, delayMs })
    });
    const data = await res.json();

    renderLbResults(data);
    fetchCluster();
    fetchLbConfig();
    fetchLedger();
  } catch (err) {
    alert("Burst execution error: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = `Send ${count} Requests`;
  }
}

function renderLbResults(data) {
  // Update distribution summary
  const summaryBox = document.getElementById("lb-distribution-summary");
  const summaryText = document.getElementById("lb-distribution-text");
  if (summaryBox && summaryText && data.distribution) {
    summaryBox.style.display = "flex";
    const parts = [];
    for (const [nodeId, handledCount] of Object.entries(data.distribution)) {
      parts.push(`Node ${nodeId}: ${handledCount} requests (${Math.round((handledCount / data.count) * 100)}%)`);
    }
    summaryText.textContent = `Strategy: ${data.strategy} | Total: ${data.count} | ` + parts.join("  •  ");
  }

  // Render Handled-By Table
  const tbody = document.getElementById("lb-results-tbody");
  tbody.innerHTML = "";

  if (!data.results || data.results.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;">No results returned.</td></tr>`;
    return;
  }

  data.results.forEach(item => {
    const tr = document.createElement("tr");
    const r = item.receipt || {};

    tr.innerHTML = `
      <td><strong>#${item.index}</strong></td>
      <td><span class="badge badge-primary">Node ${r.processingNodeId !== undefined ? r.processingNodeId : '?'}</span></td>
      <td>${escapeHtml(r.host || 'N/A')}</td>
      <td>${escapeHtml(r.os || 'N/A')}</td>
      <td><code>${escapeHtml(r.threadName || 'N/A')}</code></td>
      <td><strong>${r.lamportTs !== undefined ? r.lamportTs : 0}</strong></td>
      <td>${item.latencyMs} ms</td>
      <td><span class="badge badge-green">${escapeHtml(r.status || 'OK')}</span></td>
      <td style="font-size:0.75rem; color:var(--text-muted);">${escapeHtml(r.message || '')}</td>
    `;
    tbody.appendChild(tr);
  });
}

// =============================================================
// EXP 4: BULLY ELECTION
// =============================================================
async function fetchElectionStatus() {
  try {
    const res = await fetch("/api/election/status");
    if (!res.ok) return;
    const data = await res.json();
    const isCoord = data.coordinatorId === data.localId;
    const coordBadge = document.getElementById("election-coord-badge");
    const localBadge = document.getElementById("election-local-badge");
    const roleText = document.getElementById("election-role-text");

    if (coordBadge) {
      coordBadge.textContent = `Node ${data.coordinatorId}`;
      coordBadge.className = isCoord ? "badge badge-primary" : "badge badge-green";
    }
    if (localBadge) {
      localBadge.textContent = `Node ${data.localId}`;
    }
    if (roleText) {
      roleText.innerHTML = isCoord
        ? `<strong style="color:var(--accent-blue)">PRIMARY COORDINATOR (Highest active ID)</strong>`
        : `<strong style="color:var(--accent-green)">BACKUP NODE (Reports to Primary Node ${data.coordinatorId})</strong>`;
    }
  } catch (err) {
    console.error("Failed to fetch election status", err);
  }
}

async function handleTriggerElection() {
  const btn = document.getElementById("btn-trigger-election");
  btn.disabled = true;
  btn.textContent = "Running Bully Election...";
  try {
    const res = await fetch("/api/election/start", { method: "POST" });
    const data = await res.json();
    const traceBox = document.getElementById("election-trace-box");
    if (traceBox) {
      traceBox.innerHTML = `<div>[${new Date().toLocaleTimeString()}] Election started by Node ${data.initiatorNodeId} (Algorithm: ${data.algorithm})...</div>`;
    }
    setTimeout(fetchElectionStatus, 2000);
    setTimeout(fetchCluster, 2000);
  } catch (err) {
    alert("Election trigger failed: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Trigger Bully Election";
  }
}

// =============================================================
// EXP 5: REPLICATION & CONSISTENCY
// =============================================================
async function fetchReplConfig() {
  try {
    const res = await fetch("/api/repl/config");
    if (!res.ok) return;
    const data = await res.json();
    const radios = document.querySelectorAll('input[name="repl-mode-radio"]');
    radios.forEach(r => {
      if (r.value === data.mode) r.checked = true;
    });
    const delayInput = document.getElementById("input-repl-delay");
    if (delayInput && !delayInput.matches(":focus")) {
      delayInput.value = data.delayMs;
    }
  } catch (err) {
    console.error("Failed to fetch repl config", err);
  }
}

async function handleReplModeChange(e) {
  const mode = e.target.value;
  try {
    await fetch("/api/repl/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode })
    });
    fetchReplConfig();
  } catch (err) {
    alert("Failed to update replication mode: " + err.message);
  }
}

async function handleSetReplDelay() {
  const delayMs = parseInt(document.getElementById("input-repl-delay").value, 10);
  const btn = document.getElementById("btn-set-repl-delay");
  btn.disabled = true;
  try {
    await fetch("/api/repl/delay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ delayMs })
    });
    fetchReplConfig();
  } catch (err) {
    alert("Failed to set replication delay: " + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function handleRunStaleDemo() {
  const btn = document.getElementById("btn-run-stale-demo");
  const resultDiv = document.getElementById("stale-demo-result");
  const delayMs = parseInt(document.getElementById("input-repl-delay").value, 10) || 2000;
  btn.disabled = true;
  btn.textContent = "Running Stale-Read Demo...";
  resultDiv.style.display = "block";
  resultDiv.innerHTML = `<div class="alert-box alert-amber">Step 1: Submitting async donation to Primary and testing immediate read on Backup...</div>`;

  try {
    const res = await fetch(`/api/repl/stale-read-demo?delayMs=${delayMs}`, { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      resultDiv.innerHTML = `<div class="alert-box alert-red">Demo Error: ${escapeHtml(data.error || "Execution failed")}</div>`;
      return;
    }

    resultDiv.innerHTML = `
      <div class="alert-box ${data.staleReadObserved ? '' : 'alert-amber'}">
        <div style="font-weight:700; margin-bottom:0.4rem; color:var(--accent-blue);">
          ✓ Deterministic Stale-Read Demonstration Complete (Receipt: ${escapeHtml(data.receiptId)})
        </div>
        <div style="display:flex; flex-direction:column; gap:0.25rem; font-size:0.8rem;">
          <div>• <strong>Step 1 (Immediate Read):</strong> Backup Node ${data.backupNodeId} count = <code>${data.immediateBackupCount}</code> <span class="badge badge-amber">STALE READ OBSERVED</span></div>
          <div>• <strong>Step 2 (Post-Delay Read after ${data.delayMs}ms):</strong> Backup Node ${data.backupNodeId} count = <code>${data.eventualBackupCount}</code> <span class="badge badge-green">EVENTUALLY CONSISTENT</span></div>
          <div style="margin-top:0.3rem; color:var(--text-muted);">${escapeHtml(data.explanation)}</div>
        </div>
      </div>
    `;
    fetchLedger();
    fetchConsistency();
  } catch (err) {
    resultDiv.innerHTML = `<div class="alert-box alert-red">Error running demo: ${escapeHtml(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Stale-Read Demo";
  }
}

async function fetchConsistency() {
  const btn = document.getElementById("btn-run-consistency");
  btn.disabled = true;
  btn.textContent = "Checking...";
  try {
    const res = await fetch("/api/repl/consistency");
    if (!res.ok) return;
    const data = await res.json();

    const badge = document.getElementById("cluster-consistency-badge");
    if (badge) {
      if (data.consistent) {
        badge.textContent = "ALL NODES CONSISTENT";
        badge.className = "badge badge-green";
      } else {
        badge.textContent = "INCONSISTENT / DRIFT";
        badge.className = "badge badge-red";
      }
    }

    const refElem = document.getElementById("consistency-ref-hash");
    if (refElem) {
      refElem.textContent = data.referenceHash ? data.referenceHash.substring(0, 16) + "..." : "N/A";
      refElem.title = data.referenceHash || "";
    }

    const tbody = document.getElementById("consistency-tbody");
    tbody.innerHTML = "";

    (data.nodes || []).forEach(node => {
      const tr = document.createElement("tr");
      const matchBadge = node.match
        ? `<span class="badge badge-green">MATCH</span>`
        : `<span class="badge badge-red">MISMATCH</span>`;
      const roleBadge = node.role === "PRIMARY"
        ? `<span class="badge badge-primary">PRIMARY</span>`
        : `<span class="badge badge-green">BACKUP</span>`;

      tr.innerHTML = `
        <td><strong>Node ${node.nodeId}</strong></td>
        <td>${escapeHtml(node.host || 'N/A')}</td>
        <td>${roleBadge}</td>
        <td>${node.count >= 0 ? node.count : 'N/A'}</td>
        <td><span class="code-hash" title="${escapeHtml(node.hash)}">${escapeHtml(node.hash ? node.hash.substring(0, 20) + '...' : 'N/A')}</span></td>
        <td>${matchBadge}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error("Consistency check failed", err);
  } finally {
    btn.disabled = false;
    btn.textContent = "Check Consistency";
  }
}

// =============================================================
// EXP 8: FAULT TOLERANCE
// =============================================================
async function fetchFtStatus() {
  try {
    const res = await fetch("/api/ft/status");
    if (!res.ok) return;
    const data = await res.json();

    const statusElem = document.getElementById("ft-local-status");
    const roleElem = document.getElementById("ft-local-badge-role");
    if (statusElem) {
      if (data.simulatedCrash) {
        statusElem.textContent = "SIMULATED CRASH (RMI Stopped)";
        statusElem.style.color = "var(--accent-red)";
      } else {
        statusElem.textContent = "HEALTHY / RUNNING";
        statusElem.style.color = "var(--accent-green)";
      }
    }
    if (roleElem) {
      roleElem.textContent = data.isPrimary ? `Node ${data.nodeId} (PRIMARY)` : `Node ${data.nodeId} (BACKUP)`;
      roleElem.className = data.isPrimary ? "badge badge-primary" : "badge badge-green";
    }

    // Render FT Heartbeat table
    renderFtMonitorTable();
  } catch (err) {
    console.error("Failed to fetch FT status", err);
  }
}

function renderFtMonitorTable() {
  const tbody = document.getElementById("ft-monitor-tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (clusterNodes.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;">No peers configured.</td></tr>`;
    return;
  }

  clusterNodes.forEach(node => {
    const tr = document.createElement("tr");
    const isPrimary = localNode && node.nodeId === localNode.coordinatorId;
    const isSelf = localNode && node.nodeId === localNode.nodeId;

    const healthBadge = node.alive
      ? `<span class="badge badge-green">HEALTHY (Alive)</span>`
      : `<span class="badge badge-red">DEAD (Timeout &gt; 6s)</span>`;

    const failoverStatus = !node.alive && isPrimary
      ? `<strong style="color:var(--accent-red)">TRIGGERED BULLY ELECTION</strong>`
      : (node.alive ? `Active Heartbeat (every 2s)` : `Failure suspected`);

    tr.innerHTML = `
      <td><strong>Node ${node.nodeId}</strong> ${isSelf ? '<span class="badge badge-primary">Self</span>' : ''}</td>
      <td>${node.host}:${node.serverPort}</td>
      <td>${isPrimary ? '<span class="badge badge-primary">PRIMARY</span>' : '<span class="badge badge-green">BACKUP</span>'}</td>
      <td>${healthBadge}</td>
      <td>${node.activeConnections || 0}</td>
      <td><span style="font-size:0.75rem;">${failoverStatus}</span></td>
    `;
    tbody.appendChild(tr);
  });
}

async function handleSimulateCrash() {
  if (!confirm("Are you sure you want to simulate a crash on this node? It will stop answering RMI calls and heartbeats.")) return;
  try {
    const res = await fetch("/api/ft/crash", { method: "POST" });
    const data = await res.json();
    alert(data.message);
    fetchFtStatus();
    fetchCluster();
  } catch (err) {
    alert("Simulate crash error: " + err.message);
  }
}

async function handleResumeNode() {
  try {
    const res = await fetch("/api/ft/resume", { method: "POST" });
    const data = await res.json();
    alert(data.message);
    fetchFtStatus();
    fetchCluster();
    fetchLedger();
  } catch (err) {
    alert("Resume error: " + err.message);
  }
}

async function handleManualSnapshotSync() {
  const btn = document.getElementById("btn-ft-manual-sync");
  btn.disabled = true;
  try {
    const res = await fetch("/api/ft/sync", { method: "POST" });
    const data = await res.json();
    document.getElementById("ft-sync-result").textContent = `Synchronized ${data.syncedDonations} donations from Primary Node ${data.primaryId}`;
    fetchLedger();
  } catch (err) {
    alert("Manual sync failed: " + err.message);
  } finally {
    btn.disabled = false;
  }
}

// =============================================================
// EXP 2: MULTITHREADING (ThreadPoolExecutor & Race Demo)
// =============================================================

async function fetchThreadStatus() {
  try {
    const res = await fetch("/api/threads/status");
    if (!res.ok) return;
    const data = await res.json();

    const coreElem = document.getElementById("tp-core-size");
    const activeElem = document.getElementById("tp-active-count");
    const queueElem = document.getElementById("tp-queue-size");
    const compElem = document.getElementById("tp-completed-count");

    if (coreElem) coreElem.textContent = data.corePoolSize;
    if (activeElem) activeElem.textContent = data.activeCount;
    if (queueElem) queueElem.textContent = data.queueSize;
    if (compElem) compElem.textContent = data.completedTaskCount;

    const inputPool = document.getElementById("input-pool-size");
    if (inputPool && !inputPool.matches(":focus") && data.corePoolSize !== undefined) {
      inputPool.value = data.corePoolSize;
    }
  } catch (err) {
    console.error("Failed to fetch thread status", err);
  }
}

async function handleSetPoolSize() {
  const input = document.getElementById("input-pool-size");
  const size = parseInt(input.value, 10);
  if (isNaN(size) || size < 1) {
    alert("Please enter a valid pool size >= 1");
    return;
  }

  const btn = document.getElementById("btn-set-pool-size");
  btn.disabled = true;
  try {
    const res = await fetch("/api/threads/pool-size", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ size })
    });
    if (!res.ok) throw new Error("Status " + res.status);
    fetchThreadStatus();
  } catch (err) {
    alert("Failed to update thread pool size: " + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function handleRunThreadBurst() {
  const countInput = document.getElementById("input-tp-burst-count");
  const delayInput = document.getElementById("input-tp-burst-delay");
  const count = parseInt(countInput.value, 10) || 8;
  const delayMs = parseInt(delayInput.value, 10) || 150;

  const btn = document.getElementById("btn-run-tp-burst");
  btn.disabled = true;
  btn.textContent = `Processing ${count} tasks...`;

  try {
    const res = await fetch("/api/threads/burst", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count, delayMs })
    });
    const results = await res.json();

    const tbody = document.getElementById("tp-results-tbody");
    tbody.innerHTML = "";

    if (!results || results.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;">No task results returned.</td></tr>`;
      return;
    }

    results.forEach(r => {
      const tr = document.createElement("tr");
      const timeStr = r.timestamp ? new Date(r.timestamp).toLocaleTimeString() : "N/A";
      tr.innerHTML = `
        <td><strong>#${r.taskIndex}</strong></td>
        <td><code>${escapeHtml(r.threadName)}</code></td>
        <td>${r.latencyMs} ms</td>
        <td style="color:var(--text-muted); font-size:0.8rem;">${timeStr}</td>
      `;
      tbody.appendChild(tr);
    });

    fetchThreadStatus();
  } catch (err) {
    alert("Thread burst error: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Burst Tasks";
  }
}

function clearTpResults() {
  const tbody = document.getElementById("tp-results-tbody");
  if (tbody) {
    tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;">No burst tasks submitted yet.</td></tr>`;
  }
}

async function handleRunRaceDemo() {
  const threadsInput = document.getElementById("input-race-threads");
  const incInput = document.getElementById("input-race-increments");
  const threads = parseInt(threadsInput.value, 10) || 10;
  const incrementsPerThread = parseInt(incInput.value, 10) || 10;

  const btn = document.getElementById("btn-run-race-demo");
  const resultsBox = document.getElementById("race-demo-results");
  btn.disabled = true;
  btn.textContent = "Simulating Race Condition...";
  resultsBox.style.display = "block";
  resultsBox.innerHTML = `<div style="color:var(--accent-amber);">Synchronizing threads at CyclicBarrier and executing concurrent read-sleep-write cycles...</div>`;

  try {
    const res = await fetch("/api/threads/race-demo", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ threads, incrementsPerThread })
    });
    const data = await res.json();

    const hasLostUpdates = data.unsafeLostUpdates > 0;
    resultsBox.innerHTML = `
      <div style="display:flex; flex-direction:column; gap:0.6rem;">
        <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid rgba(255,255,255,0.08); padding-bottom:0.4rem;">
          <span style="font-weight:700; color:var(--text-main);">Theoretical Expected Count:</span>
          <span style="font-family:var(--font-mono); font-size:1.1rem; font-weight:700; color:var(--accent-blue);">
            ${data.threads} threads &times; ${data.incrementsPerThread} inc = ${data.expected}
          </span>
        </div>

        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.75rem;">
          <div style="padding:0.6rem; background:#1e293b; border-radius:4px; border:1px solid ${hasLostUpdates ? 'var(--accent-red)' : 'var(--border-color)'};">
            <div style="font-size:0.75rem; color:var(--text-muted); margin-bottom:0.2rem;">Unsafe Counter (Read-Sleep-Write):</div>
            <div style="display:flex; align-items:baseline; justify-content:space-between;">
              <span style="font-size:1.25rem; font-weight:700; color:var(--accent-red); font-family:var(--font-mono);">${data.unsafeActual}</span>
              <span class="badge badge-red">${data.unsafeLostUpdates} LOST UPDATES</span>
            </div>
            <div style="font-size:0.7rem; color:#f87171; margin-top:0.3rem;">
              Context switches during sleep caused concurrent threads to overwrite each other.
            </div>
          </div>

          <div style="padding:0.6rem; background:#1e293b; border-radius:4px; border:1px solid var(--accent-green);">
            <div style="font-size:0.75rem; color:var(--text-muted); margin-bottom:0.2rem;">Safe Counter (AtomicInteger):</div>
            <div style="display:flex; align-items:baseline; justify-content:space-between;">
              <span style="font-size:1.25rem; font-weight:700; color:var(--accent-green); font-family:var(--font-mono);">${data.safeActual}</span>
              <span class="badge badge-green">0 LOST (EXACT)</span>
            </div>
            <div style="font-size:0.7rem; color:#4ade80; margin-top:0.3rem;">
              Hardware-level CAS (Compare-And-Swap) prevents lost updates.
            </div>
          </div>
        </div>

        <div style="font-size:0.75rem; color:var(--text-muted); line-height:1.4;">
          <strong>Conclusion:</strong> In a multi-threaded server, unprotected shared mutations lose data silently under concurrency. Thread-safe atomic primitives or locks are mandatory.
        </div>
      </div>
    `;
  } catch (err) {
    resultsBox.innerHTML = `<div class="alert-box alert-red">Race demo error: ${escapeHtml(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Race Demo";
  }
}

// =============================================================
// EXP 3: CLOCKS (Lamport & Berkeley Physical Sync)
// =============================================================

async function fetchClockStatus() {
  try {
    const res = await fetch("/api/clock/status");
    if (!res.ok) return;
    const data = await res.json();

    const lamportElem = document.getElementById("clock-lamport-val");
    const skewElem = document.getElementById("clock-skew-val");
    const offsetElem = document.getElementById("clock-offset-val");
    const virtElem = document.getElementById("clock-virtual-time");
    const sysElem = document.getElementById("clock-system-time");

    if (lamportElem) lamportElem.textContent = data.lamportTime;
    if (skewElem) skewElem.textContent = (data.artificialSkewMillis >= 0 ? "+" : "") + data.artificialSkewMillis + " ms";
    if (offsetElem) offsetElem.textContent = (data.physicalClockOffsetMillis >= 0 ? "+" : "") + data.physicalClockOffsetMillis + " ms";
    if (virtElem) virtElem.textContent = `${data.formattedVirtualTime} (${data.physicalVirtualMillis} ms)`;
    if (sysElem) sysElem.textContent = `${data.formattedSystemTime} (${data.physicalSystemMillis} ms)`;
  } catch (err) {
    console.error("Failed to fetch clock status", err);
  }
}

async function handleSetClockSkew() {
  const input = document.getElementById("input-clock-skew");
  const skewMs = parseInt(input.value, 10);
  if (isNaN(skewMs)) {
    alert("Please enter a valid skew in milliseconds");
    return;
  }

  const btn = document.getElementById("btn-set-clock-skew");
  btn.disabled = true;
  try {
    const res = await fetch("/api/clock/skew", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ skewMs })
    });
    if (!res.ok) throw new Error("Status " + res.status);
    fetchClockStatus();
  } catch (err) {
    alert("Failed to inject clock skew: " + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function handleResetClock() {
  const btn = document.getElementById("btn-reset-clock");
  btn.disabled = true;
  try {
    const res = await fetch("/api/clock/reset", { method: "POST" });
    if (!res.ok) throw new Error("Status " + res.status);
    document.getElementById("input-clock-skew").value = 0;
    fetchClockStatus();
  } catch (err) {
    alert("Failed to reset clock: " + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function handleRunBerkeleySync() {
  const btn = document.getElementById("btn-run-berkeley");
  btn.disabled = true;
  btn.textContent = "Synchronizing Cluster Clocks...";

  try {
    const res = await fetch("/api/clock/berkeley-sync", { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      alert("Berkeley sync error: " + (data.error || "Execution failed"));
      return;
    }

    const summaryBox = document.getElementById("berkeley-summary-box");
    if (summaryBox) {
      summaryBox.style.display = "block";
      summaryBox.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <strong>Berkeley Sync Completed:</strong> Coordinator Node ${data.coordinatorId} queried ${data.liveNodeCount} live node clocks.
          </div>
          <div style="font-family:var(--font-mono); color:var(--accent-blue);">
            Cluster Average: ${data.formattedAverageTime} (${data.averageClockMillis} ms)
          </div>
        </div>
      `;
    }

    const tbody = document.getElementById("berkeley-tbody");
    tbody.innerHTML = "";

    (data.nodeClocks || []).forEach(node => {
      const tr = document.createElement("tr");
      const isCoord = node.nodeId === data.coordinatorId;
      const offsetSign = node.relativeOffsetMillis >= 0 ? "+" : "";

      tr.innerHTML = `
        <td><strong>Node ${node.nodeId}</strong> ${isCoord ? '<span class="badge badge-primary">COORD</span>' : '<span class="badge">PEER</span>'}</td>
        <td style="font-family:var(--font-mono);">${node.clockBeforeMillis} ms</td>
        <td style="font-family:var(--font-mono); font-weight:700; color:${node.relativeOffsetMillis === 0 ? 'var(--text-muted)' : 'var(--accent-amber)'};">${offsetSign}${node.relativeOffsetMillis} ms</td>
        <td style="font-family:var(--font-mono); font-weight:700; color:var(--accent-green);">${node.clockAfterMillis} ms</td>
        <td><span class="badge badge-green">${escapeHtml(node.status)}</span></td>
      `;
      tbody.appendChild(tr);
    });

    fetchClockStatus();
  } catch (err) {
    alert("Berkeley sync failed: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Berkeley Sync";
  }
}

// =============================================================
// EXP 7: DISTRIBUTED MAPREDUCE (with SQL SUM Verification)
// =============================================================

async function fetchMapReduceShards() {
  try {
    const res = await fetch("/api/mr/shards");
    if (!res.ok) return;
    const shards = await res.json();

    const tbody = document.getElementById("mr-shards-tbody");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!shards || shards.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;">No live shards available.</td></tr>`;
      return;
    }

    shards.forEach(s => {
      const tr = document.createElement("tr");
      const isSelf = localNode && s.nodeId === localNode.nodeId;
      tr.innerHTML = `
        <td><strong>Node ${s.nodeId}</strong> ${isSelf ? '<span class="badge" style="font-size:0.65rem;">SELF</span>' : ''}</td>
        <td><span class="badge badge-primary">Shard ${s.shardIndex}</span></td>
        <td><code>${escapeHtml(s.shardingFormula)}</code></td>
        <td><strong>${s.assignedCount}</strong> donations</td>
        <td style="font-family:var(--font-mono); color:var(--accent-green);">₹${Number(s.shardTotal).toFixed(2)}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error("Failed to fetch MapReduce shards", err);
  }
}

async function handleRunMapReduceJob() {
  const btn = document.getElementById("btn-run-mr");
  btn.disabled = true;
  btn.textContent = "Executing Distributed MapReduce Job...";

  try {
    const res = await fetch("/api/mr/run", { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      alert("MapReduce error: " + (data.error || "Execution failed"));
      return;
    }

    // 1. Verification banner
    const banner = document.getElementById("mr-verification-banner");
    if (banner) {
      banner.style.display = "block";
      if (data.verified) {
        banner.className = "alert-box";
        banner.style.background = "rgba(34, 197, 94, 0.12)";
        banner.style.borderLeft = "4px solid var(--accent-green)";
        banner.innerHTML = `
          <strong style="color:var(--accent-green); font-size:1rem;">✓ MapReduce == SQL SUM Verified!</strong>
          <div style="font-size:0.8rem; color:var(--text-muted); margin-top:0.25rem;">
            Pure Java distributed MapReduce (across ${data.liveCount} live nodes) matched the Primary SQL SUM exactly across all branches!
          </div>
        `;
      } else {
        banner.className = "alert-box alert-red";
        banner.innerHTML = `
          <strong style="color:var(--accent-red); font-size:1rem;">⚠ Verification Mismatch!</strong>
          <div style="font-size:0.8rem; color:#fca5a5; margin-top:0.25rem;">
            Discrepancies found: ${escapeHtml((data.discrepancies || []).join("; "))}
          </div>
        `;
      }
    }

    // 2. Map & Shuffle outputs
    const mapOutputDiv = document.getElementById("mr-map-output");
    if (mapOutputDiv) {
      let html = `<div style="color:var(--text-muted); font-size:0.75rem; margin-bottom:0.3rem;">[Map Phase - Shard Totals Collected via RMI]</div>`;
      for (const [nodeId, branchTotals] of Object.entries(data.mapOutputs || {})) {
        html += `<div style="padding:0.35rem 0.5rem; background:#1e293b; border-radius:3px;">`;
        html += `<strong style="color:var(--accent-blue);">Node ${nodeId} Shard:</strong> `;
        const entries = Object.entries(branchTotals).map(([b, val]) => `${b}: ₹${Number(val).toFixed(2)}`);
        html += entries.length > 0 ? entries.join("  |  ") : "0 donations";
        html += `</div>`;
      }

      html += `<div style="color:var(--text-muted); font-size:0.75rem; margin:0.5rem 0 0.3rem 0;">[Shuffle Phase - Grouped By Key]</div>`;
      for (const [branch, valList] of Object.entries(data.shuffleOutputs || {})) {
        html += `<div style="padding:0.35rem 0.5rem; background:#1e293b; border-radius:3px;">`;
        html += `<strong style="color:var(--accent-amber);">${escapeHtml(branch)}:</strong> [${valList.map(v => "₹" + Number(v).toFixed(2)).join(", ")}]`;
        html += `</div>`;
      }
      mapOutputDiv.innerHTML = html;
    }

    // 3. Verification table
    const tbody = document.getElementById("mr-verification-tbody");
    tbody.innerHTML = "";

    const allBranches = Array.from(new Set([
      ...Object.keys(data.reduceOutputs || {}),
      ...Object.keys(data.sqlSums || {})
    ])).sort();

    if (allBranches.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;">No donation records in ledger.</td></tr>`;
    } else {
      allBranches.forEach(branch => {
        const mrVal = data.reduceOutputs && data.reduceOutputs[branch] !== undefined ? data.reduceOutputs[branch] : 0.0;
        const sqlVal = data.sqlSums && data.sqlSums[branch] !== undefined ? data.sqlSums[branch] : 0.0;
        const diff = Math.abs(mrVal - sqlVal);
        const isMatch = diff < 0.001;

        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><span class="badge">${escapeHtml(branch)}</span></td>
          <td style="font-family:var(--font-mono); font-weight:700;">₹${Number(mrVal).toFixed(2)}</td>
          <td style="font-family:var(--font-mono); font-weight:700; color:var(--accent-blue);">₹${Number(sqlVal).toFixed(2)}</td>
          <td>${isMatch ? '<span class="badge badge-green">MATCH (100%)</span>' : '<span class="badge badge-red">MISMATCH</span>'}</td>
        `;
        tbody.appendChild(tr);
      });
    }

    fetchMapReduceShards();
  } catch (err) {
    alert("Failed to run MapReduce job: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Execute MapReduce Job";
  }
}

