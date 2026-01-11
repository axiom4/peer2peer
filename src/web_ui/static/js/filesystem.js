// --- Filesystem Logic ---
let currentFsPath = "/";

// Global variable to track distribution network instance
let distNetwork = null;

async function showDistributionGraph(identifier, isPublic = true) {
  const modal = new bootstrap.Modal(document.getElementById("distModal"));
  modal.show();

  const graphContainer = document.getElementById("dist-graph");
  // Clear and show loading state
  graphContainer.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100 flex-column"><div class="spinner-border text-primary"></div><div class="mt-2 text-muted">Discovery in progress...</div></div>';

  try {
    // FS entries have an ID, which is the manifest hash. So we can use /api/manifests/id/{id}
    // Note: In Filesystem view, we always identify files by ID (hash), so isPublic=true logic (using ID endpoint) is correct.
    const url = `/api/manifests/id/${identifier}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error("Failed to load map data");
    const manifest = await res.json();

    // Clear loading spinner
    graphContainer.innerHTML = "";

    const nodes = [];
    const edges = [];
    const ids = new Set();
    const sortedChunks = manifest.chunks.sort((a, b) => a.index - b.index);

    // Collect all unique Hosts first to create specific nodes
    const hostSet = new Set();
    sortedChunks.forEach((c) => {
      (c.locations || []).forEach((loc) => hostSet.add(loc));
    });

    // Add Hosts as Nodes
    hostSet.forEach((loc) => {
      // Show full authority (IP:Port) instead of just Port to avoid ambiguity
      const label = loc.replace(/^https?:\/\//, "");
      nodes.push({
        id: loc,
        label: label,
        color: "#4CAF50",
        size: 25,
        shape: "ellipse",
      });
      ids.add(loc);
    });

    // Add Start Node (Source)
    nodes.push({ id: "SOURCE", label: "START", color: "#FF9800", size: 30, shape: "database" });
    ids.add("SOURCE");

    // Build edges based on sequence: SOURCE -> Chunks(0) -> Chunks(1) ...
    let prevHosts = ["SOURCE"];

    sortedChunks.forEach((chunk, idx) => {
      const chunkLabel = `C${chunk.index}`;
      const currentHosts = chunk.locations || [];

      if (currentHosts.length > 0) {
        const uniqueCurr = [...new Set(currentHosts)];

        if (prevHosts.length > 0) {
          // Full Mesh Connection: Any node holding Chunk N can transition to Any node holding Chunk N+1
          // This correctly visualizes the redundancy and available paths.
          prevHosts.forEach((prev) => {
            uniqueCurr.forEach((curr) => {
              // Avoid duplicate edges if multiple chunks follow same path? 
              // uniqueCurr is unique per step. prevHours is unique.
              // So we just add edges.
              edges.push({
                from: prev,
                to: curr,
                label: chunkLabel, // e.g. "C1"
                arrows: "to",
                color: "#2196F3",
                font: { align: "horizontal", size: 10, background: "white" },
              });
            });
          });
        }
        prevHosts = uniqueCurr;
      } else {
        prevHosts = [];
      }
    });

    const container = document.getElementById("dist-graph");
    const data = { nodes: nodes, edges: edges };

    const options = {
      layout: {
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
          damping: 0.09,
          avoidOverlap: 0.5,
        },
        stabilization: {
          iterations: 1000,
        },
      },
      edges: {
        smooth: {
          type: "continuous",
        },
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

// Init
document.addEventListener("DOMContentLoaded", function () {
  console.log("Initializing Filesystem...");
  loadFilesystem("/")
    .then(() => console.log("Filesystem loaded"))
    .catch((e) => console.error("FS Load Error:", e));

  // Init Tree
  refreshTreeRoot();
});

// --- Tree Logic ---

// Refresh the currently active tree node (node corresponding to currentFsPath)
// This ensures that if we added/removed folders, the tree reflects it WITHOUT collapsing everything.
async function refreshActiveTreeNode() {
  const activeItem = document.querySelector(`.tree-item[data-path="${currentFsPath}"]`);
  if (activeItem) {
    const parentDiv = activeItem.parentElement;
    const group = parentDiv.nextElementSibling; // .tree-group
    // Only reload if it is already expanded (has .show class)
    // or if we want to force expansion? Maybe just reload if known.
    // Usually we just want to update content.
    if (group && group.classList.contains("show")) {
      await loadTreePath(currentFsPath, group);
    } else {
      // If not expanded, we might want to at least ensure the arrow is correct?
      // But loadTreePath handles content.
      // If it was empty and now has content, we might want to update the toggle icon?
      // But toggle logic handles lazy load.
    }
  }
}

async function refreshTreeRoot() {
  const rootContainer = document.getElementById("fsTreeRoot");
  rootContainer.innerHTML = "";

  // Creates the Root Node
  const rootDiv = document.createElement("div");
  rootDiv.innerHTML = `
        <div class="d-flex align-items-center">
            <span class="tree-toggle" onclick="toggleTreeNode(event, '/', this)"><i class="fas fa-chevron-right"></i></span>
            <div class="tree-item flex-grow-1" onclick="loadFilesystem('/')" data-path="/">
                <i class="fas fa-hdd text-warning me-2"></i> Root
            </div>
        </div>
        <div class="tree-group" id="group-root"></div>
    `;
  rootContainer.appendChild(rootDiv);

  // Auto-expand root
  const toggleBtn = rootDiv.querySelector(".tree-toggle");
  toggleTreeNode(null, "/", toggleBtn);
}

async function toggleTreeNode(ev, path, toggleEl) {
  if (ev) ev.stopPropagation();
  const group = toggleEl.parentElement.nextElementSibling; // The div.tree-group
  const isExpanded = group.classList.contains("show");

  if (isExpanded) {
    group.classList.remove("show");
    toggleEl.innerHTML = '<i class="fas fa-chevron-right"></i>';
  } else {
    group.classList.add("show");
    toggleEl.innerHTML = '<i class="fas fa-chevron-down"></i>';
    if (group.children.length === 0) {
      // Lazy load
      await loadTreePath(path, group);
    }
  }
}

async function loadTreePath(path, container) {
  container.innerHTML = '<div class="text-muted small ps-4">Loading...</div>';
  try {
    const res = await fetch(`/api/fs/ls?path=${encodeURIComponent(path)}`);
    const data = await res.json();
    container.innerHTML = "";

    if (!data.entries) return;

    // Filter directories only
    const dirs = data.entries.filter((e) => e.type === "directory");
    dirs.sort((a, b) => a.name.localeCompare(b.name));

    if (dirs.length === 0) {
      container.innerHTML = '<div class="text-muted small ps-4"><i>Empty</i></div>';
      return;
    }

    dirs.forEach((dir) => {
      const dirPath = path === "/" ? `/${dir.name}` : `${path}/${dir.name}`;
      const node = document.createElement("div");
      node.innerHTML = `
                <div class="d-flex align-items-center">
                    <span class="tree-toggle" onclick="toggleTreeNode(event, '${dirPath}', this)"><i class="fas fa-chevron-right"></i></span>
                    <div class="tree-item flex-grow-1" onclick="loadFilesystem('${dirPath}')" data-path="${dirPath}">
                        <i class="fas fa-folder text-warning me-2"></i> ${dir.name}
                    </div>
                </div>
                <div class="tree-group"></div>
              `;
      container.appendChild(node);
    });
  } catch (e) {
    container.innerHTML = `<div class="text-danger small ps-4">Error</div>`;
  }
}

async function loadFilesystem(path = "/") {
  currentFsPath = path;
  renderBreadcrumbs(path);

  // Highlight active tree item
  document.querySelectorAll(".tree-item").forEach((el) => el.classList.remove("active"));
  const activeItem = document.querySelector(`.tree-item[data-path="${path}"]`);
  if (activeItem) activeItem.classList.add("active");

  const tbody = document.getElementById("fsFileList");
  if (document.getElementById("fsLoading")) document.getElementById("fsLoading").style.display = "inline-block";

  try {
    const res = await fetch(`/api/fs/ls?path=${encodeURIComponent(path)}`);
    const data = await res.json();

    tbody.innerHTML = "";

    if (data.path !== "/") {
      const parentPath = currentFsPath.substring(0, currentFsPath.lastIndexOf("/")) || "/";
      const tr = document.createElement("tr");

      // Allow dropping on parent folder ".."
      tr.setAttribute("ondragover", "handleDragOver(event)");
      tr.setAttribute("ondrop", "handleDrop(event, '..')");
      tr.setAttribute("ondragleave", "handleDragLeave(event)");

      tr.innerHTML = `
             <td><i class="fas fa-folder-open text-warning fa-lg"></i></td>
             <td><a href="#" onclick="loadFilesystem('${parentPath}'); return false;" class="text-decoration-none text-dark fw-bold">..</a></td>
             <td>Dir</td>
             <td>-</td>
             <td></td>
           `;
      tbody.appendChild(tr);
    }

    if (data.entries && data.entries.length > 0) {
      data.entries.sort((a, b) => {
        if (a.type === b.type) return a.name.localeCompare(b.name);
        return a.type === "directory" ? -1 : 1;
      });

      data.entries.forEach((entry) => {
        const tr = document.createElement("tr");
        const icon = entry.type === "directory" ? "fa-folder text-warning" : "fa-file-alt text-secondary";

        // Drag and Drop attributes
        tr.setAttribute("draggable", "true");
        tr.setAttribute("ondragstart", `handleDragStart(event, '${entry.name}', '${entry.type}')`);

        // If directory, it can accept drops
        if (entry.type === "directory") {
          tr.setAttribute("ondragover", "handleDragOver(event)");
          tr.setAttribute("ondrop", `handleDrop(event, '${entry.name}')`);
          tr.setAttribute("ondragleave", "handleDragLeave(event)");
        }

        let nameLink = entry.name;
        if (entry.type === "directory") {
          const nextPath = path === "/" ? `/${entry.name}` : `${path}/${entry.name}`;
          nameLink = `<a href="#" onclick="loadFilesystem('${nextPath}'); return false;" class="fw-bold text-decoration-none text-dark">${entry.name}</a>`;
        }

        tr.innerHTML = `
                 <td><i class="fas ${icon} fa-lg"></i></td>
                 <td>${nameLink}</td>
                 <td>${entry.type}</td>
                 <td>${entry.type === "file" ? formatBytes(entry.size) : "-"}</td>
                 <td>
                    <div class="btn-group">
                    ${
                      entry.type === "file"
                        ? `<button class="btn btn-sm btn-outline-primary" onclick="window.location.href='/api/stream_id/${entry.id}'"><i class="fas fa-download"></i></button>`
                        : ""
                    }
                    ${
                      entry.type === "file"
                        ? `<button class="btn btn-sm btn-outline-info" onclick="showDistributionGraph('${entry.id}')" title="Chunk Map"><i class="fas fa-project-diagram"></i></button>`
                        : ""
                    }
                    <button class="btn btn-sm btn-outline-danger" onclick="deleteFsEntry('${entry.name}')"><i class="fas fa-trash"></i></button>
                    </div>
                 </td>
               `;
        tbody.appendChild(tr);
      });
    } else {
      if (tbody.children.length === 0) tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Empty directory</td></tr>';
    }
  } catch (e) {
    console.error(e);
    tbody.innerHTML = `<tr><td colspan="5" class="text-center text-danger">Error: ${e}</td></tr>`;
  } finally {
    if (document.getElementById("fsLoading")) document.getElementById("fsLoading").style.display = "none";
  }
}

function renderBreadcrumbs(path) {
  const nav = document.getElementById("fsBreadcrumbs");
  nav.innerHTML = "";

  const parts = path.split("/").filter((p) => p);

  const rootLi = document.createElement("li");
  rootLi.className = "breadcrumb-item";
  if (parts.length === 0) {
    rootLi.classList.add("active");
    rootLi.innerText = "Root";
  } else {
    rootLi.innerHTML = `<a href="#" onclick="loadFilesystem('/'); return false;">Root</a>`;
  }
  nav.appendChild(rootLi);

  let accum = "";
  parts.forEach((part, idx) => {
    accum += "/" + part;
    accum = accum.replace("//", "/");

    const li = document.createElement("li");
    li.className = "breadcrumb-item";
    if (idx === parts.length - 1) {
      li.classList.add("active");
      li.innerText = part;
    } else {
      li.innerHTML = `<a href="#" onclick="loadFilesystem('${accum}'); return false;">${part}</a>`;
    }
    nav.appendChild(li);
  });
}

async function deleteFsEntry(name) {
  if (!confirm(`Are you sure you want to delete '${name}'?`)) return;

  // Show Deletion Progress
  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.className = "active";
  statusDiv.innerHTML = `
        <div class="d-flex justify-content-between align-items-center mb-2">
            <strong class="small"><i class="fas fa-trash me-2"></i>Deleting '${name}'...</strong>
        </div>
        <div class="progress mb-2" style="height: 15px;">
            <div id="deleteProgressBar" class="progress-bar progress-bar-striped progress-bar-animated bg-danger" role="progressbar" style="width: 5%;">5%</div>
        </div>
        <div id="deleteStatusText" class="text-muted small" style="min-height: 1.2em;">Initializing...</div>
    `;

  try {
    const res = await fetch("/api/fs/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        path: currentFsPath,
        name: name,
      }),
    });

    const result = await res.json();

    if (res.ok && result.status === "processing" && result.task_id) {
      // Poll for progress
      const taskId = result.task_id;
      const progressBar = document.getElementById("deleteProgressBar");
      const statusText = document.getElementById("deleteStatusText");

      const pollInterval = setInterval(async () => {
        try {
          const taskRes = await fetch(`/api/progress/${taskId}`);
          const task = await taskRes.json();

          if (task.status === "completed") {
            clearInterval(pollInterval);
            if (progressBar) {
              progressBar.style.width = "100%";
              progressBar.innerText = "100%";
              progressBar.classList.remove("progress-bar-animated");
              progressBar.classList.add("bg-success");
            }
            if (statusText) statusText.innerHTML = '<span class="text-success">Deleted!</span>';

            setTimeout(() => statusDiv.classList.remove("active"), 1500);
            loadFilesystem(currentFsPath);
            refreshActiveTreeNode();
          } else if (task.status === "error") {
            clearInterval(pollInterval);
            if (statusText) statusText.innerText = "Error: " + task.message;
            if (progressBar) progressBar.classList.add("bg-danger");
            setTimeout(() => statusDiv.classList.remove("active"), 3000);
          } else {
            // Update progress
            if (progressBar) {
              progressBar.style.width = task.progress + "%";
              progressBar.innerText = task.progress + "%";
            }
            if (statusText) {
              // Use innerHTML to handle potential icons or long text truncation
              // Truncate to avoid layout shift
              let msg = task.message;
              if (msg.length > 50) msg = msg.substring(0, 47) + "...";
              statusText.innerHTML = `<span class="text-truncate d-block" style="max-width: 100%;">${msg}</span>`;
            }
          }
        } catch (e) {
          console.error(e);
        }
      }, 500);
    } else if (res.ok && result.status === "ok") {
      // Fallback for immediate success (legacy or empty)
      statusDiv.innerHTML = `<div class="text-success small fw-bold">Deleted immediately.</div>`;
      setTimeout(() => statusDiv.classList.remove("active"), 1000);
      loadFilesystem(currentFsPath);
      refreshActiveTreeNode();
    } else {
      throw new Error(result.error || result.message);
    }
  } catch (e) {
    statusDiv.innerHTML = `
         <div class="mb-2"><strong class="small text-danger">Error</strong></div>
         <div class="small text-danger">${e}</div>
      `;
    setTimeout(() => statusDiv.classList.remove("active"), 3000);
  }
}

async function promptMkdir() {
  const name = prompt("Enter folder name:");
  if (!name) return;

  try {
    const res = await fetch("/api/fs/mkdir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        path: currentFsPath,
        name: name,
      }),
    });
    const result = await res.json();
    if (res.ok) {
      loadFilesystem(currentFsPath);
      refreshActiveTreeNode();
    } else {
      alert("Error: " + result.error);
    }
  } catch (e) {
    alert("Error: " + e);
  }
}

function triggerFsUpload() {
  const input = document.getElementById("fileInput");
  input.click();
}

function handleFileInputChange(event) {
  const file = event.target.files[0];
  if (file) {
    uploadFileDirectly(file);
  }
  // Reset input so same file can be selected again if needed
  event.target.value = "";
}

async function addFileToFs(manifestId, filename, size, targetPath = null) {
  const path = targetPath || currentFsPath;
  try {
    const res = await fetch("/api/fs/add_file", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        path: path,
        name: filename,
        id: manifestId,
        size: size,
      }),
    });
    const result = await res.json();
    if (res.ok) {
      if (path === currentFsPath) loadFilesystem(currentFsPath);
    } else {
      console.warn("Error adding to filesystem: " + result.error);
    }
  } catch (e) {
    console.error(e);
  }
}

// --- Drag and Drop Logic ---

function handleDragStart(ev, name, type) {
  ev.stopPropagation();
  ev.dataTransfer.setData("text/plain", JSON.stringify({ name: name, type: type, srcPath: currentFsPath }));
  ev.dataTransfer.effectAllowed = "move";
}

function handleDragOver(ev) {
  ev.preventDefault();
  ev.stopPropagation(); // Stop bubbling to card
  ev.dataTransfer.dropEffect = "move";
  ev.currentTarget.classList.add("table-active");
}

function handleDragLeave(ev) {
  ev.currentTarget.classList.remove("table-active");
}

async function handleDrop(ev, destName) {
  ev.preventDefault();
  ev.stopPropagation(); // Stop bubbling to card
  ev.currentTarget.classList.remove("table-active");

  const rawData = ev.dataTransfer.getData("text/plain");
  if (!rawData) return;

  try {
    const data = JSON.parse(rawData);
    const sourceName = data.name;

    if (sourceName === destName) return;

    let destPath = currentFsPath;
    if (destName === "..") {
      destPath = currentFsPath.substring(0, currentFsPath.lastIndexOf("/")) || "/";
    } else {
      destPath = (currentFsPath === "/" ? "" : currentFsPath) + "/" + destName;
    }

    const confirmMove = confirm(`Move '${sourceName}' to '${destName}'?`);
    if (!confirmMove) return;

    const payload = {
      source_path: currentFsPath,
      source_name: sourceName,
      dest_path: destPath,
    };

    const res = await fetch("/api/fs/move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const result = await res.json();
    if (res.ok) {
      loadFilesystem(currentFsPath);
      refreshActiveTreeNode();
    } else {
      alert("Move failed: " + (result.error || "Unknown error"));
    }
  } catch (e) {
    console.error("Drop error", e);
  }
}

// External File Drop (Upload with Folder Support)
function handleFileDragOver(ev) {
  ev.preventDefault();
  ev.stopPropagation(); // Ensure event doesn't bubble
  ev.dataTransfer.dropEffect = "copy";
  document.getElementById("fsCard").classList.add("border-primary", "border-3");
}

function handleFileDragLeave(ev) {
  ev.preventDefault();
  ev.stopPropagation();
  document.getElementById("fsCard").classList.remove("border-primary", "border-3");
}

// --- Upload Logic ---

// Core upload function (No UI)
function performUpload(file, targetPath, onProgress) {
  return new Promise((resolve, reject) => {
    const replicas = document.getElementById("replicasInput").value || 5;
    const compression = document.getElementById("compressionCheck").checked;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("redundancy", replicas);
    formData.append("compression", compression);

    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/upload", true);

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        const percent = Math.round((event.loaded / event.total) * 100);
        // Upload phase is 40% of total process
        onProgress(Math.round(percent * 0.4), "Uploading...");
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        try {
          const result = JSON.parse(xhr.responseText);
          if (result.status === "processing" && result.task_id) {
            const taskId = result.task_id;

            const pollInterval = setInterval(async () => {
              try {
                const res = await fetch(`/api/progress/${taskId}`);
                const task = await res.json();

                if (task.status === "completed") {
                  clearInterval(pollInterval);
                  onProgress(100, "Completed");

                  const match = task.message.match(/Manifest ID: ([a-f0-9]{64})/i);
                  if (match && match[1]) {
                    // Register in FS
                    await addFileToFs(match[1], file.name, file.size, targetPath);
                    resolve(match[1]);
                  } else {
                    resolve(true);
                  }
                } else if (task.status === "error") {
                  clearInterval(pollInterval);
                  reject(new Error(task.message));
                } else {
                  // Processing phase (40-100%)
                  let p = task.percent;
                  let visualP = 40 + Math.round(p * 0.6);
                  onProgress(visualP, task.message);
                }
              } catch (e) {
                clearInterval(pollInterval);
                reject(e);
              }
            }, 500);
          } else {
            reject(new Error("Invalid server response"));
          }
        } catch (e) {
          reject(e);
        }
      } else {
        reject(new Error("Upload failed: " + xhr.status));
      }
    };

    xhr.onerror = () => reject(new Error("Network error"));
    xhr.send(formData);
  });
}

function uploadFileDirectly(file, targetPath = null) {
  const tPath = targetPath || currentFsPath;
  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.classList.add("active");

  // Reset UI
  statusDiv.innerHTML = `
            <div class="d-flex justify-content-between align-items-center mb-2">
                <strong class="small text-truncate" style="max-width: 250px;" title="${file.name}">Uploading ${file.name}</strong>
                <button type="button" class="btn-close btn-close-sm" onclick="document.getElementById('uploadStatus').classList.remove('active')"></button>
            </div>
            <div class="progress mb-2" style="height: 15px;">
                <div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"></div>
            </div>
            <div id="statusText" class="text-muted small text-truncate" style="font-size: 0.8em; max-width: 100%;">Starting...</div>
          `;

  return performUpload(file, tPath, (percent, msg) => {
    const pb = document.getElementById("progressBar");
    const st = document.getElementById("statusText");
    if (pb) {
      pb.style.width = percent + "%";
      pb.innerText = percent + "%";
      if (percent === 100) pb.classList.add("bg-success");
    }
    if (st) st.innerText = msg;
  })
    .then(() => {
      setTimeout(() => statusDiv.classList.remove("active"), 1500);
    })
    .catch((e) => {
      statusDiv.innerHTML = `<div class="alert alert-danger mb-0 p-2 small">${e.message}</div>`;
    });
}

// Batch Upload Logic
async function processBatchQueue(tasks) {
  if (!tasks || tasks.length === 0) {
    document.getElementById("uploadStatus").classList.remove("active");
    return;
  }

  const statusDiv = document.getElementById("uploadStatus");
  statusDiv.classList.add("active");

  const totalTasks = tasks.length;
  let completed = 0;

  // Calculate total size for weighted progress
  const totalBytes = tasks.reduce((acc, t) => acc + t.file.size, 0);
  let processedBytes = 0;

  statusDiv.innerHTML = `
          <div class="d-flex justify-content-between align-items-center mb-2">
              <strong class="small">Batch Upload (${totalTasks} files)</strong>
              <button type="button" class="btn-close btn-close-sm" onclick="document.getElementById('uploadStatus').classList.remove('active')"></button>
          </div>
          <div class="progress mb-2" style="height: 15px;">
              <div id="batchProgressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%;"></div>
          </div>
          <div id="batchStatusText" class="text-muted small text-truncate" style="font-size: 0.8em;">Starting batch...</div>
        `;

  for (let i = 0; i < tasks.length; i++) {
    const task = tasks[i];
    const currentFileType = task.file.name;

    document.getElementById("batchStatusText").innerText = `(${i + 1}/${totalTasks}) Uploading ${task.file.name}...`;

    try {
      await performUpload(task.file, task.targetPath, (percent, msg) => {
        // Calculate overall progress
        // Current file contribution
        const currentFileDone = (percent / 100) * task.file.size;
        const totalDone = processedBytes + currentFileDone;
        const totalPercent = Math.round((totalDone / totalBytes) * 100);

        const pb = document.getElementById("batchProgressBar");
        if (pb) {
          pb.style.width = totalPercent + "%";
          pb.innerText = totalPercent + "%";
        }
      });
      processedBytes += task.file.size;
      completed++;
    } catch (e) {
      console.error(`Failed to upload ${task.file.name}: ${e}`);
      // Treat as processed (skip)
      processedBytes += task.file.size;
    }
  }

  // Finalize
  const pb = document.getElementById("batchProgressBar");
  if (pb) {
    pb.style.width = "100%";
    pb.innerText = "Done";
    pb.classList.remove("progress-bar-animated");
    pb.classList.add("bg-success");
  }
  document.getElementById("batchStatusText").innerText = `Batch completed. ${completed}/${totalTasks} uploaded.`;

  setTimeout(() => statusDiv.classList.remove("active"), 2000);

  // Refresh UI
  loadFilesystem(currentFsPath);
  refreshActiveTreeNode();
}

// --- Helper Functions ---

function getAllEntries(dirReader) {
  return new Promise((resolve, reject) => {
    let entriesList = [];
    const read = () => {
      dirReader.readEntries(
        (results) => {
          if (!results.length) {
            resolve(entriesList);
          } else {
            entriesList = entriesList.concat(results);
            read();
          }
        },
        (e) => reject(e)
      );
    };
    read();
  });
}

async function ensureDirectory(fullPath) {
  if (!fullPath || fullPath === "/") return;
  const lastSlash = fullPath.lastIndexOf("/");
  const parentPath = fullPath.substring(0, lastSlash) || "/";
  const newName = fullPath.substring(lastSlash + 1);

  try {
    // Optimistic Mkdir
    await fetch("/api/fs/mkdir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: parentPath, name: newName }),
    });
  } catch (e) {
    console.warn("Mkdir error", e);
  }
}

async function handleFileDrop(ev) {
  ev.preventDefault();
  ev.stopPropagation(); // Stop bubbling
  document.getElementById("fsCard").classList.remove("border-primary", "border-3");

  const statusDiv = document.getElementById("uploadStatus");
  if (!statusDiv) {
    alert("Error: UI Status Component missing. Please refresh the page.");
    return;
  }

  const items = ev.dataTransfer.items;
  if (!items || items.length === 0) return;

  // CRITICAL: Capture entries IMMEDIATELY before any blocking call
  const entries = [];
  try {
    for (let i = 0; i < items.length; i++) {
      let entry = null;
      if (items[i].webkitGetAsEntry) {
        entry = items[i].webkitGetAsEntry();
      } else if (items[i].getAsEntry) {
        entry = items[i].getAsEntry();
      }
      if (entry) {
        entries.push(entry);
      }
    }
  } catch (e) {
    console.error("DnD access error:", e);
  }

  if (entries.length === 0) return;

  if (!confirm(`Upload ${entries.length} items (and subfolders) to current folder?`)) return;

  // Show "Scanning" status immediately
  statusDiv.classList.add("active");
  statusDiv.innerHTML = `
        <div class="d-flex align-items-center">
            <div class="spinner-border spinner-border-sm me-2" role="status"></div>
            <div>Scanning directory structure...</div>
        </div>
    `;

  const batchTasks = [];

  // Recursive collector
  async function collect(item) {
    try {
      if (item.isFile) {
        return new Promise((resolve) => {
          item.file(
            (f) => {
              const relativeFolder = item.fullPath.substring(0, item.fullPath.lastIndexOf("/"));
              const targetDir = (currentFsPath === "/" ? "" : currentFsPath) + relativeFolder;
              ensureDirectory(targetDir).then(() => {
                batchTasks.push({ file: f, targetPath: targetDir });
                resolve();
              });
            },
            (err) => {
              console.warn("File read error:", err);
              resolve();
            }
          );
        });
      } else if (item.isDirectory) {
        const dirPath = (currentFsPath === "/" ? "" : currentFsPath) + item.fullPath;
        await ensureDirectory(dirPath);
        const dirReader = item.createReader();
        const entries = await getAllEntries(dirReader);
        for (const entry of entries) {
          await collect(entry);
        }
      }
    } catch (e) {
      console.error("Collect error:", e);
    }
  }

  try {
    for (const entry of entries) {
      await collect(entry);
    }

    if (batchTasks.length > 0) {
      processBatchQueue(batchTasks);
    } else {
      statusDiv.innerHTML = `<div class="alert alert-warning mb-0">No files found to upload.</div>`;
      setTimeout(() => statusDiv.classList.remove("active"), 3000);
    }
  } catch (e) {
    statusDiv.innerHTML = `<div class="alert alert-danger mb-0">Scan Failed: ${e.message}</div>`;
    setTimeout(() => statusDiv.classList.remove("active"), 5000);
  }
}
