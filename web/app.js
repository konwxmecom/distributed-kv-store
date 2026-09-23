const configuredGateway = new URLSearchParams(window.location.search).get("api");
const API_BASE = `${(configuredGateway || "http://127.0.0.1:8080").replace(/\/$/, "")}/api`;

let clusterState = null;

let entries = [];
let activities = [
  { icon: "●", text: "Waiting for gateway activity", meta: "start gateway to connect · now" }
];

const $ = (selector) => document.querySelector(selector);

function renderEntries() {
  const query = $("#searchInput").value.trim().toLowerCase();
  const filtered = entries.filter((entry) => `${entry.key} ${entry.value}`.toLowerCase().includes(query));
  $("#resultCount").textContent = `${filtered.length} ${filtered.length === 1 ? "key" : "keys"}`;
  $("#emptyState").hidden = filtered.length > 0;
  $("#dataRows").innerHTML = filtered.map((entry) => `
    <tr>
      <td>${escapeHtml(entry.key)}</td>
      <td title="${escapeHtml(entry.value)}">${escapeHtml(entry.value)}</td>
      <td>${escapeHtml(entry.updated)}</td>
      <td><span class="status">● committed</span></td>
      <td><div class="row-actions"><button data-edit="${escapeHtml(entry.key)}" title="Edit entry">edit</button><button data-delete="${escapeHtml(entry.key)}" title="Delete entry">delete</button></div></td>
    </tr>`).join("");
}

function renderNodes() {
  if (!clusterState) {
    $("#nodeList").innerHTML = '<div class="node-unavailable">Cluster topology is unavailable.</div>';
    return;
  }
  const nodeName = `node-${clusterState.node_id}`;
  const role = clusterState.is_leader ? "Leader" : "Follower";
  $("#nodeList").innerHTML = `<div class="node"><span class="node-icon">${clusterState.is_leader ? "L" : "F"}</span><div class="node-details"><strong>${nodeName}</strong><small>gateway target</small></div><span class="node-state"><i></i>${role}</span></div>`;
  $(".online-count").textContent = clusterState.is_leader ? "leader ready" : `leader node-${clusterState.leader_id}`;
  $("#commitSummary").textContent = String(clusterState.commit_index);
  $("#nodeSummary").textContent = role;
}

function renderActivity() {
  $("#activityList").innerHTML = activities.slice(0, 5).map((activity) => `
    <div class="activity"><span class="activity-mark">${activity.icon}</span><div class="activity-copy">${activity.text}<small>${activity.meta}</small></div></div>`).join("");
}

function escapeHtml(value) {
  return value.replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function addActivity(key, action) {
  activities.unshift({ icon: action === "deleted" ? "−" : "✓", text: `${action === "deleted" ? "Deleted" : "Committed"} <strong>${escapeHtml(key)}</strong>`, meta: "Raft gateway · just now" });
  renderActivity();
}

async function saveEntry(key, value, previousKey = null) {
  const response = await fetch(`${API_BASE}/entry`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key, value }) });
  if (!response.ok) throw new Error((await response.json()).error || "write failed");
  if (previousKey && previousKey !== key) {
    const deleteResponse = await fetch(`${API_BASE}/entry?key=${encodeURIComponent(previousKey)}`, { method: "DELETE" });
    if (!deleteResponse.ok) throw new Error("new value saved, but old key could not be removed");
  }
  const index = entries.findIndex((entry) => entry.key === (previousKey || key));
  const entry = { key, value, updated: "just now" };
  if (index >= 0) entries[index] = entry;
  else entries.unshift(entry);
  addActivity(key, "committed");
  renderEntries();
}

function openEditor(key = "") {
  const existing = entries.find((entry) => entry.key === key);
  $("#dialogTitle").textContent = existing ? "Edit entry" : "New entry";
  $("#dialogKey").value = existing?.key || "";
  $("#dialogValue").value = existing?.value || "";
  $("#editDialog").dataset.previousKey = existing?.key || "";
  $("#editDialog").showModal();
}

$("#searchInput").addEventListener("input", renderEntries);
$("#newEntryButton").addEventListener("click", () => openEditor());
$("#refreshButton").addEventListener("click", async (event) => {
  event.currentTarget.animate([{ transform: "rotate(0)" }, { transform: "rotate(360deg)" }], { duration: 450 });
  await loadFromGateway();
});

$("#dataRows").addEventListener("click", (event) => {
  const editKey = event.target.dataset.edit;
  const deleteKey = event.target.dataset.delete;
  if (editKey) openEditor(editKey);
  if (deleteKey) {
    if (!window.confirm(`Delete ${deleteKey}? This writes a delete through Raft.`)) return;
    fetch(`${API_BASE}/entry?key=${encodeURIComponent(deleteKey)}`, { method: "DELETE" }).then(async (response) => {
      if (!response.ok) throw new Error((await response.json()).error || "delete failed");
      entries = entries.filter((entry) => entry.key !== deleteKey);
      addActivity(deleteKey, "deleted");
      renderEntries();
    }).catch(showGatewayError);
  }
});

$("#editForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const key = $("#dialogKey").value.trim();
  const value = $("#dialogValue").value.trim();
  if (!key || !value) return;
  try {
    await saveEntry(key, value, $("#editDialog").dataset.previousKey);
    $("#editDialog").close();
  } catch (error) { showGatewayError(error); }
});

$("#entryForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const key = $("#keyInput").value.trim();
  const value = $("#valueInput").value.trim();
  if (!key || !value) return;
  try {
    await saveEntry(key, value);
    event.currentTarget.reset();
    $("#formNote").textContent = `Entry committed for ${key}.`;
  } catch (error) { showGatewayError(error); }
});

function rememberKey(key) {
  const keys = new Set(JSON.parse(localStorage.getItem("raft-kv-keys") || "[]"));
  keys.add(key);
  localStorage.setItem("raft-kv-keys", JSON.stringify([...keys]));
}

function forgetKey(key) {
  const keys = new Set(JSON.parse(localStorage.getItem("raft-kv-keys") || "[]"));
  keys.delete(key);
  localStorage.setItem("raft-kv-keys", JSON.stringify([...keys]));
}

function showGatewayError(error) {
  $("#formNote").textContent = `Gateway error: ${error.message}`;
}

async function loadFromGateway() {
  const pill = $("#connectionPill");
  try {
    const health = await fetch(`${API_BASE}/health`);
    if (!health.ok) throw new Error("Raft node is unavailable");
    pill.innerHTML = '<span class="pulse"></span> Live cluster';
    pill.classList.add("connected");

    const clusterResponse = await fetch(`${API_BASE}/cluster`);
    if (!clusterResponse.ok) throw new Error("cluster status is unavailable");
    clusterState = await clusterResponse.json();
    const leaderLabel = clusterState.is_leader ? `node-${clusterState.node_id}` : `node-${clusterState.leader_id || clusterState.node_id}`;
    $("#heroLeader").textContent = leaderLabel;
    $("#leaderMeta").innerHTML = `<i></i> ${clusterState.is_leader ? "healthy · leader" : "healthy · follower"} · term ${clusterState.current_term}`;
    $("#termMetric").textContent = String(clusterState.current_term ?? 0);
    $("#commitMetric").textContent = String(clusterState.commit_index ?? 0);
    $("#healthMetric").textContent = clusterState.is_leader ? "Ready" : "Follower";
    $("#healthDetail").textContent = `node-${clusterState.node_id} responding`;
    $("#snapshotMetric").textContent = "Live";
    renderNodes();

    const keysResponse = await fetch(`${API_BASE}/keys`);
    if (!keysResponse.ok) throw new Error("could not list keys from cluster");
    const keysData = await keysResponse.json();
    const keys = Array.isArray(keysData.keys) ? keysData.keys : [];

    const values = await Promise.all(keys.map(async (key) => {
      const response = await fetch(`${API_BASE}/entry?key=${encodeURIComponent(key)}`);
      const data = await response.json();
      return data.found ? { key, value: data.value, updated: "from Raft" } : null;
    }));
    entries = values.filter(Boolean);
    renderEntries();
    $("#formNote").textContent = clusterState.is_leader ? "Connected. Writes are ready." : `Connected. Write through node-${clusterState.leader_id}.`;
  } catch (error) {
    clusterState = null;
    pill.innerHTML = '<span class="pulse"></span> Gateway offline';
    pill.classList.remove("connected");
    $("#formNote").textContent = "Start gateway.py to connect this console to the cluster.";
    $("#healthMetric").textContent = "Offline";
    $("#healthDetail").textContent = "Gateway unavailable";
    $("#snapshotMetric").textContent = "Unknown";
    renderNodes();
    renderEntries();
  }
}

renderEntries();
renderNodes();
renderActivity();
loadFromGateway();
