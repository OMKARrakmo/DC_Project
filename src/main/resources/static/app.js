// DMS Frontend Application Logic (Vanilla JS)

let localNode = null;
let clusterNodes = [];
let allEvents = [];

document.addEventListener("DOMContentLoaded", () => {
  setupTabs();
  setupSSE();
  fetchCluster();
  fetchLedger();

  // Periodic refresh
  setInterval(fetchCluster, 3000);
  setInterval(fetchLedger, 5000);

  // Event Listeners
  document.getElementById("btn-refresh-cluster").addEventListener("click", fetchCluster);
  document.getElementById("btn-refresh-ledger").addEventListener("click", fetchLedger);
  document.getElementById("btn-clear-log").addEventListener("click", clearLog);
  document.getElementById("log-filter-tag").addEventListener("change", renderFilteredLogs);

  document.getElementById("form-donate").addEventListener("submit", handleDonate);
  document.getElementById("form-branch-query").addEventListener("submit", handleBranchQuery);
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
