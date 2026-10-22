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

  // Periodic refresh
  setInterval(fetchCluster, 3000);
  setInterval(fetchLedger, 5000);
  setInterval(fetchLbConfig, 2000);

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
