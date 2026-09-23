const seedData = [
  { key: "feature.dark_mode", value: "enabled", updated: "2 min ago" },
  { key: "service.timeout_ms", value: "2500", updated: "18 min ago" },
  { key: "region.primary", value: "ap-south-1", updated: "41 min ago" },
  { key: "release.version", value: "2.4.1", updated: "1 hr ago" }
];

const nodes = [
  { name: "node-50051", address: "localhost:50051", role: "Leader", icon: "L" },
  { name: "node-50052", address: "localhost:50052", role: "Follower", icon: "F" },
  { name: "node-50053", address: "localhost:50053", role: "Follower", icon: "F" }
];

let entries = [...seedData];
let activities = [
  { icon: "✓", text: "Committed <strong>release.version</strong>", meta: "node-50051 · 1 hour ago" },
  { icon: "↗", text: "Replicated entry to quorum", meta: "term 18 · 1 hour ago" },
  { icon: "●", text: "Heartbeat acknowledged", meta: "all followers · 2 hours ago" },
  { icon: "⌁", text: "Snapshot completed", meta: "248 entries compacted · 3 hours ago" }
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
  $("#nodeList").innerHTML = nodes.map((node) => `
    <div class="node"><span class="node-icon">${node.icon}</span><div class="node-details"><strong>${node.name}</strong><small>${node.address}</small></div><span class="node-state"><i></i>${node.role}</span></div>`).join("");
}

function renderActivity() {
  $("#activityList").innerHTML = activities.slice(0, 5).map((activity) => `
    <div class="activity"><span class="activity-mark">${activity.icon}</span><div class="activity-copy">${activity.text}<small>${activity.meta}</small></div></div>`).join("");
}

function escapeHtml(value) {
  return value.replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function addActivity(key, action) {
  activities.unshift({ icon: action === "deleted" ? "−" : "✓", text: `${action === "deleted" ? "Deleted" : "Committed"} <strong>${escapeHtml(key)}</strong>`, meta: "local demo · just now" });
  renderActivity();
}

function saveEntry(key, value, previousKey = null) {
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
$("#refreshButton").addEventListener("click", (event) => {
  event.currentTarget.animate([{ transform: "rotate(0)" }, { transform: "rotate(360deg)" }], { duration: 450 });
  $("#formNote").textContent = "Dashboard refreshed just now.";
  setTimeout(() => { $("#formNote").textContent = "Writes are simulated locally in this console."; }, 2200);
});

$("#dataRows").addEventListener("click", (event) => {
  const editKey = event.target.dataset.edit;
  const deleteKey = event.target.dataset.delete;
  if (editKey) openEditor(editKey);
  if (deleteKey) {
    entries = entries.filter((entry) => entry.key !== deleteKey);
    addActivity(deleteKey, "deleted");
    renderEntries();
  }
});

$("#editForm").addEventListener("submit", (event) => {
  event.preventDefault();
  const key = $("#dialogKey").value.trim();
  const value = $("#dialogValue").value.trim();
  if (!key || !value) return;
  saveEntry(key, value, $("#editDialog").dataset.previousKey);
  $("#editDialog").close();
});

$("#entryForm").addEventListener("submit", (event) => {
  event.preventDefault();
  const key = $("#keyInput").value.trim();
  const value = $("#valueInput").value.trim();
  if (!key || !value) return;
  saveEntry(key, value);
  event.currentTarget.reset();
  $("#formNote").textContent = `Entry appended for ${key}.`;
  setTimeout(() => { $("#formNote").textContent = "Writes are simulated locally in this console."; }, 2600);
});

renderEntries();
renderNodes();
renderActivity();
