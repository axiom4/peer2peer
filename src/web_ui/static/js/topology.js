// Topology Logic
let network = null;

async function pruneOrphans() {
  if (!confirm("Are you sure you want to prune orphan chunks? This action is distributed across the network.")) return;

  const btn = document.querySelector('button[onclick="pruneOrphans()"]');
  btn.disabled = true;

  try {
    const res = await fetch("/api/prune", { method: "POST" });
    const data = await res.json();

    // Show feedback
    const statusDiv = document.getElementById("statusArea");
    const alert = document.createElement("div");
    alert.className = `alert alert-${data.status === "ok" ? "success" : "danger"} alert-dismissible fade show`;
    alert.innerHTML = `
             ${data.message || "Prune command sent."}
             <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
         `;
    statusDiv.appendChild(alert);

    // Auto hide
    setTimeout(() => alert.remove(), 5000);
  } catch (e) {
    console.error(e);
    alert("Error sending prune command");
  } finally {
    btn.disabled = false;
  }
}

async function loadGraph() {
  const container = document.getElementById("network-graph");

  try {
    const res = await fetch("/api/network");
    const data = await res.json();

    if (!data.nodes || data.nodes.length === 0) {
      container.innerHTML = '<div class="d-flex justify-content-center align-items-center h-100 text-muted">No nodes found in the network.</div>';
      return;
    } else {
      container.innerHTML = ""; // Clear
    }

    const options = {
      nodes: {
        shape: "icon",
        icon: {
          face: "'Font Awesome 5 Free'",
          weight: "900",
          code: "\uf233",
          size: 35,
          color: "#0d6efd",
        },
        font: { size: 14, face: "arial", background: "rgba(255,255,255,0.8)" },
        shadow: true,
      },
      edges: {
        arrows: { to: { enabled: true, scaleFactor: 0.5 } },
        color: { color: "#6c757d", highlight: "#0d6efd", opacity: 0.6 },
        smooth: { type: "continuous" },
        width: 1.5,
      },
      physics: {
        enabled: true,
        solver: "barnesHut",
        barnesHut: {
          gravitationalConstant: -8000,
          centralGravity: 0.2, // Reduced to let nodes fly out more
          springLength: 300, // Increased significantly to spread nodes
          springConstant: 0.01,
          damping: 0.09,
          avoidOverlap: 0.3,
        },
        stabilization: {
          enabled: true,
          iterations: 1000,
          updateInterval: 50,
          fit: true,
        },
        adaptiveTimestep: true,
      },
    };

    const nodes = data.nodes.map((n) => ({
      id: n.id,
      label: n.id.replace("http://", "").replace(":", "\n"),
      title: n.id,
    }));

    const edges = data.edges;

    network = new vis.Network(container, { nodes, edges }, options);
  } catch (e) {
    console.error("Failed to load graph", e);
    container.innerHTML = '<div class="alert alert-danger m-3">Failed to load graph data</div>';
  }
}

// Init
document.addEventListener("DOMContentLoaded", function () {
  loadGraph();
});
