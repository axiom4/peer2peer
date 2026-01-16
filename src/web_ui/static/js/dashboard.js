// Dashboard Logic
async function downloadByManifestId() {
  const input = document.getElementById("manifestIdInput");
  const id = input.value.trim();

  if (!id) {
    alert("Please enter a Manifest ID.");
    return;
  }
  if (id.length !== 64) {
    alert("Invalid ID length. A valid Manifest Hash must be 64 characters.");
    return;
  }

  // Trigger stream download
  window.location.href = `/api/stream_id/${id}`;
}

async function loadManifests() {
  try {
    const res = await fetch("/api/manifests");
    const data = await res.json();
    const list = document.getElementById("manifestList");
    if (list) {
      list.innerHTML = "";
      if (data.length === 0) {
        list.innerHTML = '<li class="list-group-item text-center">No manifests found</li>';
        return;
      }
      data.forEach((item) => {
        const name = item.filename || item;
        const size = item.size ? formatBytes(item.size) : "N/A";
        const cleanName = name;

        const li = document.createElement("li");
        li.className = "list-group-item d-flex justify-content-between align-items-center";
        li.innerHTML = `
                        <div class="d-flex align-items-center flex-grow-1 overflow-hidden">
                            <span class="text-truncate fw-bold me-2" title="${cleanName}">${cleanName}</span>
                            <span class="badge bg-light text-secondary border">${size}</span>
                        </div>
                        <div class="btn-group ms-2">
                            <button class="btn btn-sm btn-outline-info" onclick="showDistributionGraph('${cleanName}')" title="View Map">Map</button>
                            <button class="btn btn-sm btn-outline-primary" onclick="downloadFile('${cleanName}')" title="Download to Server">Get</button>
                            <button class="btn btn-sm btn-outline-success" onclick="streamDownload('${cleanName}')" title="Download Directly to Browser">Stream</button>
                            <button class="btn btn-sm btn-outline-warning" onclick="repairFile('${cleanName}')" title="Check Health & Repair">Fix</button>
                            <button class="btn btn-sm btn-outline-danger" onclick="deleteManifest('${cleanName}')" title="Delete">Del</button>
                        </div>
                    `;
        list.appendChild(li);
      });
    }
  } catch (e) {
    console.error(e);
  }
}

function streamDownload(manifestName) {
  if (!confirm(`Stream download ${manifestName} directly? This depends on your browser to save the file.`)) return;
  window.location.href = `/api/stream/${manifestName}`;
}

async function deleteManifest(name) {
  if (!confirm(`Are you sure you want to delete ${name} and all its chunks from the network? This cannot be undone.`)) return;

  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.classList.add("active");
  statusDiv.innerHTML = `
        <div class="progress mb-2" style="height: 25px;">
            <div id="deleteBar" class="progress-bar progress-bar-striped progress-bar-animated bg-danger" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>
        </div>
        <div id="deleteStatusText" class="text-muted small">Starting deletion...</div>
    `;

  try {
    const res = await fetch(`/api/manifests/${name}`, { method: "DELETE" });
    const result = await res.json();

    if (result.status === "processing" && result.task_id) {
      const taskId = result.task_id;
      const progressBar = document.getElementById("deleteBar");
      const statusText = document.getElementById("deleteStatusText");

      const pollInterval = setInterval(async () => {
        try {
          const r = await fetch(`/api/progress/${taskId}`);
          const task = await r.json();

          if (task.status === "completed") {
            clearInterval(pollInterval);
            if (progressBar) {
              progressBar.style.width = "100%";
              progressBar.innerText = "Done";
              progressBar.classList.remove("progress-bar-animated");
              progressBar.classList.add("bg-success");
              progressBar.classList.remove("bg-danger");
            }
            if (statusText) statusText.innerText = task.message;

            // Refresh
            loadManifests();
            loadCatalog();

            setTimeout(() => {
              statusDiv.classList.remove("active");
              statusDiv.innerHTML = "";
            }, 3000);
          } else if (task.status === "error") {
            clearInterval(pollInterval);
            statusDiv.innerHTML = `<div class="alert alert-danger">${task.message}</div>`;
          } else {
            if (progressBar) {
              progressBar.style.width = task.percent + "%";
              progressBar.innerText = task.percent + "%";
            }
            if (statusText) statusText.innerText = task.message;
          }
        } catch (e) {
          console.error(e);
        }
      }, 800);
    } else if (res.ok) {
      // Fallback for legacy sync response
      loadManifests();
      loadCatalog();
      statusDiv.innerHTML = `<div class="alert alert-success">Deleted successfully</div>`;
      setTimeout(() => {
        statusDiv.classList.remove("active");
        statusDiv.innerHTML = "";
      }, 2000);
    } else {
      statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${result.error || result.message}</div>`;
    }
  } catch (e) {
    statusDiv.innerHTML = `<div class="alert alert-danger">Network error deleting manifest</div>`;
  }
}

// Global variable to track distribution network instance
let distNetwork = null;

async function showDistributionGraph(identifier, isPublic = false) {
  const modal = new bootstrap.Modal(document.getElementById("distModal"));
  modal.show();

  const graphContainer = document.getElementById("dist-graph");
  // Clear and show loading state
  graphContainer.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100 flex-column"><div class="spinner-border text-primary"></div><div class="mt-2 text-muted">Discovery in progress...</div></div>';

  try {
    const url = isPublic ? `/api/manifests/id/${identifier}` : `/api/manifests/${identifier}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error("Failed to load map data");
    const manifest = await res.json();

    // Clear loading spinner
    graphContainer.innerHTML = "";

    // 1. Identify Unique Hosts & Create Nodes
    const nodes = [];
    const edges = [];
    const ids = new Set();

    // Add Start Node
    nodes.push({
      id: "SOURCE",
      label: "START",
      color: "#FF9800",
      size: 30,
      shape: "database",
    });
    ids.add("SOURCE");

    const sortedChunks = manifest.chunks.sort((a, b) => a.index - b.index);
    const hostSet = new Set();

    // Collect all hosts
    sortedChunks.forEach((c) => {
      (c.locations || []).forEach((loc) => hostSet.add(loc));
    });

    // Add Host Nodes
    hostSet.forEach((loc) => {
      let label = loc.replace(/^https?:\/\//, ""); // Show IP:Port (Default/Legacy)

      // Handle LibP2P MultiAddr: /ip4/127.0.0.1/tcp/8001/p2p/Qm...
      if (loc.startsWith("/ip4/") || loc.startsWith("/ip6/")) {
        const parts = loc.split("/");
        // ["", "ip4", "127.0.0.1", "tcp", "8001", ...]
        const ipIdx = parts.indexOf("ip4") !== -1 ? parts.indexOf("ip4") + 1 : parts.indexOf("ip6") + 1;
        const tcpIdx = parts.indexOf("tcp") + 1;

        if (ipIdx > 0 && tcpIdx > 0 && parts[ipIdx] && parts[tcpIdx]) {
          label = `${parts[ipIdx]}:${parts[tcpIdx]}`;
        }
      }

      nodes.push({
        id: loc,
        label: label,
        color: "#4CAF50",
        size: 25,
        shape: "ellipse",
      });
      ids.add(loc);
    });

    // 2. Build Edges (Chunk Flow) - Aggregated
    let prevHosts = ["SOURCE"];

    // Map to aggregate edges: "from_to" -> { list of chunks }
    const edgeMap = new Map();

    sortedChunks.forEach((chunk, idx) => {
      const chunkLabel = `C${chunk.index}`;
      const locations = chunk.locations || [];

      if (locations.length > 0) {
        const uniqueCurrentHosts = [...new Set(locations)];

        prevHosts.forEach((prev) => {
          uniqueCurrentHosts.forEach((curr) => {
            const key = `${prev}->${curr}`;
            if (!edgeMap.has(key)) {
              edgeMap.set(key, []);
            }
            edgeMap.get(key).push(chunkLabel);
          });
        });

        prevHosts = uniqueCurrentHosts;
      } else {
        prevHosts = [];
      }
    });

    // Convert aggregated map to edges
    edgeMap.forEach((chunks, key) => {
      const [from, to] = key.split("->");

      // Summarize label if too long
      let label = "";
      if (chunks.length <= 3) {
        label = chunks.join(", ");
      } else {
        const first = chunks[0];
        const last = chunks[chunks.length - 1];
        label = `${first}..${last} (${chunks.length})`;
      }

      edges.push({
        from: from,
        to: to,
        label: label,
        arrows: "to",
        color: "#2196F3",
        font: { align: "horizontal", size: 10, background: "white" },
      });
    });

    const container = document.getElementById("dist-graph");
    const data = { nodes: nodes, edges: edges };

    const options = {
      layout: {
        hierarchical: false,
        improvedLayout: true,
        randomSeed: 2,
      },
      physics: {
        enabled: true,
        solver: "barnesHut",
        barnesHut: {
          gravitationalConstant: -2000,
          centralGravity: 0.3,
          springLength: 200,
          springConstant: 0.04,
          damping: 0.5,
          avoidOverlap: 0.5,
        },
        stabilization: {
          enabled: true,
          iterations: 100, // Reduced iterations from 1000 to prevent freezing
          updateInterval: 50,
          fit: true,
        },
      },
      edges: {
        smooth: {
          type: "dynamic", // Allow curving for multiple edges
          forceDirection: "none",
          roundness: 0.5,
        },
      },
      interaction: {
        dragNodes: true,
        zoomView: true,
        dragView: true,
      },
    };

    // Wait for modal and destroy previous
    setTimeout(() => {
      if (distNetwork) distNetwork.destroy();
      distNetwork = new vis.Network(container, data, options);
    }, 500);
  } catch (e) {
    document.getElementById("dist-graph").innerText = "Error loading map: " + e;
  }
}

async function downloadFile(manifestName) {
  try {
    const statusDiv = document.getElementById("uploadStatus"); // Reusing status div

    // Show progress bar
    statusDiv.innerHTML = `
        <div class="progress mb-2" style="height: 25px;">
            <div id="downloadBar" class="progress-bar progress-bar-striped progress-bar-animated bg-info text-dark" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>
        </div>
        <div id="downloadStatusText" class="text-muted small">Preparing download for ${manifestName}...</div>
      `;

    const res = await fetch("/api/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ manifest: manifestName }),
    });
    const result = await res.json();

    if (result.status === "processing" && result.task_id) {
      const taskId = result.task_id;
      const progressBar = document.getElementById("downloadBar");
      const statusText = document.getElementById("downloadStatusText");

      const pollInterval = setInterval(async () => {
        try {
          const r = await fetch(`/api/progress/${taskId}`);
          const task = await r.json();

          if (task.status === "completed") {
            clearInterval(pollInterval);
            if (progressBar) {
              progressBar.style.width = "100%";
              progressBar.innerText = "Ready";
              progressBar.classList.remove("progress-bar-animated");
              progressBar.classList.add("bg-success", "text-white");
            }
            statusDiv.innerHTML = `<div class="alert alert-success">Download ready!</div>`;

            // Trigger browser download if URL provided
            if (task.download_url) {
              const a = document.createElement("a");
              a.href = task.download_url;
              a.download = manifestName.replace(".manifest", "");
              document.body.appendChild(a);
              a.click();
              document.body.removeChild(a);
            }

            setTimeout(() => {
              statusDiv.innerHTML = "";
            }, 3000);
          } else if (task.status === "error") {
            clearInterval(pollInterval);
            statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${task.message}</div>`;
          } else {
            // Update progress
            if (progressBar) {
              progressBar.style.width = task.percent + "%";
              progressBar.innerText = task.percent + "%";
            }
            if (statusText) statusText.innerText = task.message;
          }
        } catch (e) {
          console.error("Polling error", e);
        }
      }, 1000);
    } else if (result.status === "ok" && result.download_url) {
      // Legacy/Fast path (if sync)
      statusDiv.innerHTML = `<div class="alert alert-success">Download started!</div>`;

      // Trigger browser download
      const a = document.createElement("a");
      a.href = result.download_url;
      a.download = manifestName.replace(".manifest", "");
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      // Hide alert after 3s
      setTimeout(() => {
        statusDiv.innerHTML = "";
      }, 3000);
    } else {
      statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${result.message}</div>`;
    }
  } catch (e) {
    alert("Error triggering download: " + e);
  }
}

// --- Catalog ---
async function loadCatalog() {
  const tbody = document.getElementById("catalogList");
  if (!tbody) return;

  tbody.innerHTML = '<tr><td colspan="4" class="text-center"><div class="spinner-border spinner-border-sm text-primary"></div> Searching Network...</td></tr>';

  try {
    const res = await fetch("/api/catalog");
    const data = await res.json();

    if (data.error) {
      tbody.innerHTML = `<tr><td colspan="4" class="text-center text-danger">Error: ${data.error}</td></tr>`;
      return;
    }

    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No public files found in discovery.</td></tr>';
      return;
    }

    tbody.innerHTML = "";
    data.forEach((item) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
                <td class="fw-bold">${item.name}</td>
                <td><code class="user-select-all small">${item.id}</code></td>
                <td>${formatBytes(item.size || 0)}</td>
                <td style="white-space: nowrap;">
                    <button class="btn btn-sm btn-outline-info" onclick="showDistributionGraph('${item.id}', true)" title="View Map"><i class="fas fa-project-diagram"></i></button>
                    <button class="btn btn-sm btn-primary" onclick="window.location.href='/api/stream_id/${item.id}'" title="Download"><i class="fas fa-download"></i></button>
                    <button class="btn btn-sm btn-outline-danger" onclick="deleteManifest('${item.id}')" title="Delete from Network"><i class="fas fa-trash"></i></button>
                </td>
             `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="4" class="text-center text-danger">Network Error: ${e}</td></tr>`;
  }
}

async function repairFile(manifestName) {
  if (!confirm(`Run health check and repair on ${manifestName}?`)) return;

  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.innerHTML = `
        <div class="progress mb-2" style="height: 25px;">
            <div id="repairBar" class="progress-bar progress-bar-striped progress-bar-animated bg-warning text-dark" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>
        </div>
        <div id="repairStatusText" class="text-muted small">Initiating repair...</div>
    `;

  try {
    const res = await fetch("/api/repair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ manifest: manifestName }),
    });
    const result = await res.json();

    if (result.status === "processing" && result.task_id) {
      const taskId = result.task_id;
      const progressBar = document.getElementById("repairBar");
      const statusText = document.getElementById("repairStatusText");

      const pollInterval = setInterval(async () => {
        try {
          const r = await fetch(`/api/progress/${taskId}`);
          const task = await r.json();

          if (task.status === "completed") {
            clearInterval(pollInterval);
            if (progressBar) {
              progressBar.style.width = "100%";
              progressBar.innerText = "Done";
              progressBar.classList.remove("progress-bar-animated");
              progressBar.classList.add("bg-success", "text-white");
              progressBar.classList.remove("bg-warning", "text-dark");
            }
            if (statusText) statusText.innerText = task.message;

            loadManifests(); // refresh if locations changed
            setTimeout(() => {
              statusDiv.innerHTML = "";
            }, 5000);
          } else if (task.status === "error") {
            clearInterval(pollInterval);
            statusDiv.innerHTML = `<div class="alert alert-danger">${task.message}</div>`;
          } else {
            if (progressBar) {
              progressBar.style.width = task.percent + "%";
              progressBar.innerText = task.percent + "%";
            }
            if (statusText) statusText.innerText = task.message;
          }
        } catch (e) {
          console.error(e);
        }
      }, 1000);
    } else {
      statusDiv.innerHTML = `<div class="alert alert-danger">Failed to start repair: ${result.message}</div>`;
    }
  } catch (e) {
    statusDiv.innerHTML = `<div class="alert alert-danger">Network Error</div>`;
  }
}

// --- Upload ---

function triggerUpload() {
  document.getElementById("fileInput").click();
}

function handleFileSelect(event) {
  const file = event.target.files[0];
  if (file) {
    uploadFile(file);
  }
  // Reset so same file can be selected again
  event.target.value = "";
}

function handleDragOver(event) {
  event.preventDefault();
  event.stopPropagation();
  event.currentTarget.classList.add("bg-secondary", "bg-opacity-10", "border-primary");
}

function handleDragLeave(event) {
  event.preventDefault();
  event.stopPropagation();
  event.currentTarget.classList.remove("bg-secondary", "bg-opacity-10", "border-primary");
}

function handleDrop(event) {
  event.preventDefault();
  event.stopPropagation();
  event.currentTarget.classList.remove("bg-secondary", "bg-opacity-10", "border-primary");

  const dt = event.dataTransfer;
  const files = dt.files;

  if (files.length > 0) {
    uploadFile(files[0]);
  }
}

function uploadFile(file) {
  const replicas = document.getElementById("replicasInput").value || 5;
  const compression = document.getElementById("compressionCheck").checked;

  const formData = new FormData();
  formData.append("file", file);
  formData.append("redundancy", replicas);
  formData.append("compression", compression);

  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.classList.add("active");
  // Reset status with a progress bar
  statusDiv.innerHTML = `
        <div class="progress mb-2" style="height: 25px;">
            <div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>
        </div>
        <div id="statusText" class="text-muted small">Starting upload...</div>
    `;

  const xhr = new XMLHttpRequest();
  xhr.open("POST", "/api/upload", true);

  xhr.upload.onprogress = (event) => {
    if (event.lengthComputable) {
      const percent = Math.round((event.loaded / event.total) * 100);
      const progressBar = document.getElementById("progressBar");
      const statusText = document.getElementById("statusText");

      // UX: Map Upload (0-100%) to first 40% of total progress bar
      // This reserves 60% for the server-side distribution/processing
      const visualPercent = Math.round(percent * 0.4);

      if (progressBar) {
        progressBar.style.width = visualPercent + "%";
        progressBar.innerText = visualPercent + "%";
        progressBar.setAttribute("aria-valuenow", visualPercent);
      }

      if (statusText) {
        if (percent < 100) {
          statusText.innerText = `Uploading: ${formatBytes(event.loaded)} / ${formatBytes(event.total)}`;
        } else {
          statusText.innerText = "Upload complete. Sending to server...";
        }
      }
    }
  };

  xhr.onload = () => {
    if (xhr.status === 200) {
      try {
        const result = JSON.parse(xhr.responseText);

        if (result.status === "processing" && result.task_id) {
          // Start polling for progress
          const taskId = result.task_id;
          const progressBar = document.getElementById("progressBar");
          const statusText = document.getElementById("statusText");

          // Switch visual style to indicate phase change
          if (progressBar) {
            progressBar.classList.remove("bg-primary");
            progressBar.classList.add("bg-success", "progress-bar-striped", "progress-bar-animated");
          }

          const pollInterval = setInterval(async () => {
            try {
              const res = await fetch(`/api/progress/${taskId}`);
              const task = await res.json();

              if (task.status === "completed") {
                clearInterval(pollInterval);

                // Make sure bar is at 100%
                if (progressBar) {
                  progressBar.style.width = "100%";
                  progressBar.innerText = "100%";
                  progressBar.setAttribute("aria-valuenow", 100);
                  progressBar.classList.remove("progress-bar-animated");
                }
                if (statusText) {
                  statusText.className = "text-success fw-bold";
                  // Use the backend message which contains the ID
                  statusText.innerText = task.message;
                }

                // Try to extract Manifest ID for better visibility
                const match = task.message.match(/Manifest ID: ([a-f0-9]{64})/i);
                if (match && match[1]) {
                  const manifestId = match[1];
                  const alertHtml = `
                            <div class="alert alert-success mt-2">
                                <strong>Build Success!</strong><br>
                                Manifest ID: <code class="user-select-all">${manifestId}</code><br>
                                <small class="text-muted">(Share this ID for direct download)</small>
                            </div>
                         `;
                  statusDiv.innerHTML += alertHtml;
                }

                loadManifests(); // Update local manifests
                loadCatalog(); // Update public catalog

                // Auto-hide after 5 seconds
                setTimeout(() => {
                  statusDiv.classList.remove("active");
                  statusDiv.innerHTML = "";
                }, 5000);
              } else if (task.status === "error") {
                clearInterval(pollInterval);
                statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${task.message}</div>`;
              } else {
                // Update progress
                if (progressBar) {
                  let p = task.percent;
                  // Map Distribution (0-100%) to remaining 60% of total bar (40 -> 100)
                  let visualP = 40 + Math.round(p * 0.6);

                  progressBar.style.width = visualP + "%";
                  progressBar.innerText = visualP + "%";
                  progressBar.setAttribute("aria-valuenow", visualP);
                }
                if (statusText) {
                  statusText.innerText = task.message;
                }
              }
            } catch (e) {
              console.error("Polling error", e);
            }
          }, 500);
        } else if (result.status === "ok") {
          statusDiv.innerHTML = `<div class="alert alert-success">${result.message}</div>`;
          loadCatalog(); // Refresh catalog
        } else {
          statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${result.message}</div>`;
        }
      } catch (e) {
        statusDiv.innerHTML = `<div class="alert alert-danger">Invalid server response</div>`;
      }
    } else {
      statusDiv.innerHTML = `<div class="alert alert-danger">Upload Failed (Status ${xhr.status})</div>`;
    }
  };

  xhr.onerror = () => {
    statusDiv.innerHTML = `<div class="alert alert-danger">Network Error during upload</div>`;
  };

  xhr.send(formData);
}

document.addEventListener("DOMContentLoaded", function () {
  console.log("Initializing Dashboard...");
  loadCatalog();
});

// Refresh catalog periodically
setInterval(loadCatalog, 30000);

function filterCatalog() {
  const input = document.getElementById("searchCatalog");
  const filter = input.value.toLowerCase();
  const tbody = document.getElementById("catalogList");
  const tr = tbody.getElementsByTagName("tr");

  for (let i = 0; i < tr.length; i++) {
    const tdName = tr[i].getElementsByTagName("td")[0];
    const tdId = tr[i].getElementsByTagName("td")[1];
    if (tdName || tdId) {
      const txtName = tdName.textContent || tdName.innerText;
      const txtId = tdId.textContent || tdId.innerText;
      if (txtName.toLowerCase().indexOf(filter) > -1 || txtId.toLowerCase().indexOf(filter) > -1) {
        tr[i].style.display = "";
      } else {
        tr[i].style.display = "none";
      }
    }
  }
}
