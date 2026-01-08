# System Operation and Logic

This document details the internal mechanisms used to distribute data across the network and reconstruct it, ensuring security, space efficiency, and high availability.

## 1. Data Distribution (Upload)

The upload process transforms a single file into many optimized, anonymous fragments scattered across the network.

### A. Preparation (Sharding Pipeline)

When a user uploads a file, the `ShardManager` executes an in-memory transformation pipeline:

1. **Block Reading**: The file is segmented into fixed-size chunks (1MB).
2. **Compression**: Each chunk is individually compressed using **Zlib** (faster, better for streaming) or optionally **LZMA**.

   > _Goal_: Reduce network traffic and disk space.

3. **Encryption**: The compressed chunk is encrypted with the Fernet algorithm (AES-128 in CBC mode with SHA256 HMAC).

   > _Key_: A unique key is generated for each upload session.

4. **Binary Packing**: The encrypted payload (normally Base64 url-safe) is decoded into **Raw Binary Bytes** before sending.

   > _Optimization_: Avoids the typical 33% Base64 overhead, saving pure bits on disk.

5. **Hashing**: The SHA-256 hash of the final blob is computed to generate the unique `chunk_id`.

### B. Distribution Strategy (Redundancy & Scattering)

The `DistributionStrategy` manages data dispersion:

1. **Beacon Discovery**: The client listens for UDP broadcast beacons (Port 9999) and queries the **DHT** to map active nodes.
2. **Node Selection**: For each chunk, **5 distinct nodes** are selected randomly (Replication Factor $N=5$).

   > This redundancy ensures data survival even if 4 out of 5 custodian nodes go offline simultaneously.

3. **Parallel Upload**: Blobs are uploaded in parallel via HTTP/1.1 (Keep-Alive) pooling to maximize throughput.

### C. Manifest Creation

At the end of the upload, a `.manifest` (JSON) file is generated on the client side:

- Contains the decryption key (required to read the data).
- Contains the **Merkle Root** for file integrity verification.
- Contains the ordered list of chunk hashes.
- **Compression Mode**: Explicitly stores if compression was enabled/disabled to ensure correct reconstruction.
- **Privacy**: Does not contain original file names or node IP addresses (location is found dynamically during restore via Network Query/DHT).

---

## 2. Data Reconstruction (Download)

The client does not know the data location a priori; it must discover it.

### A. Network Discovery (Hybrid Strategy)

The client uses a hybrid approach to find chunks:

1. **DHT Lookup (Primary)**: Queries the Distributed Hash Table to find nodes that announced holding the chunk.
2. **UDP Broadcast (Fallback)**: If DHT fails, broadcasts a `QUERY_CHUNK` message to the entire subnet.
3. **HTTP Retrieval**: The client performs a direct HTTP GET from one of the active nodes found.

This removes the need for "Flooding" or "Crawling" via HTTP, making discovery instant and scalable.

### B. Restore Pipeline (Reverse Engineering)

The received blob undergoes the reverse process of upload:

1. **Binary Unpack**: Reads raw bytes.
2. **Decryption**: Uses the Fernet key from the manifest.
3. **Decompression**: Expands data via Zlib/LZMA (based on manifest flag).
4. **Assembly**: Writes the byte stream to the correct position in the final file.

### C. Direct Streaming (Memory-Only)

For web downloads, the system bypasses disk writing:

- **Generator Pipeline**: Chunks are fetched, decrypted, and yielded one by one in memory.
- **HTTP Stream**: The backend streams these bytes directly to the browser (`application/octet-stream`).
- **No Temp Files**: Reconstructed file is never saved to the server's disk, complying with strict ephemeral storage requirements.

## Flow Diagram (Sequence)

```mermaid
sequenceDiagram
   autonumber
   participant Client as 👤 Client (App)
   participant Engine as ⚙️ Processing Engine
   participant Network as 🌐 P2P Network (50+ Nodes)
   participant Manifest as 📜 Manifest File

   Note over Client, Network: 📤 Phase 1: Distribution (Secure Pipeline)
   Client->>Engine: Stream File (4MB Chunks)

   loop For each Chunk
      Engine->>Engine: 1. Compress (Zlib)
      Engine->>Engine: 2. Encrypt (Fernet Key)
      Engine->>Engine: 3. Binary Pack (Base64 Decode)
      Engine->>Network: 4. Parallel PUT /chunk/{id} (To 5 Nodes)
      Network-->>Client: 200 OK (x5)
   end

   Client->>Manifest: Save Key + ChunkHashes + CompressionMode

   Note over Client, Network: 📥 Phase 2: Restore (Zero-Knowledge)
   Client->>Manifest: Load Manifest (Key + IDs)
   Client->>Network: Discover Peers (DHT + UDP)

   loop For each Chunk
      Client->>Network: DHT/UDP Query (Who has {id}?)
      Network-->>Client: Owner URL
      Client->>Network: GET /chunk/{id} (To Node X)
      Network-->>Client: Raw Binary Stream

      Client->>Engine: 1. Decrypt (Fernet)

      Client->>Engine: 2. Decompress (Zlib/LZMA)
      Engine->>Client: Write Original Bytes / Stream
   end
```

## 3. Redundancy Strategy (Cluster Resilience)

The system is designed to ensure data availability even in case of massive disconnections (high churn rate).

### Replication Topology (5x)

The architecture uses a **Simple Replication** model with factor $N=5$.
Unlike Erasure Coding systems (e.g., Reed-Solomon) which require CPU-intensive reconstruction, simple replication favors read latency and immediate resilience.

- **Node Selection**: During upload, the client selects 5 random nodes.
  - **Kademlia DHT**: Nodes also announce themselves to a valid ID space, allowing closer-node selection in future iterations.
- **High Availability**: This ensures data is accessible as long as at least **1 of the 5 custodian nodes** remains online and reachable.
- **Implicit Load Balancing**: Since each chunk chooses a different set of 5 nodes, storage and bandwidth load is evenly distributed across the cluster (Statistical Load Balancing).

```mermaid
graph TD
   classDef active fill:#198754,stroke:#333,stroke-width:2px,color:white;
   classDef offline fill:#dc3545,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:white;
   classDef chunk fill:#0d6efd,stroke:#333,stroke-width:2px,color:white;

   File["📂 Original File"] --> Split["✂️ Split, Compress & Encrypt"]
   Split --> Chunk1["📦 Chunk #X"]:::chunk

   subgraph "Storage Cluster (Mesh)"
      direction LR
      Chunk1 -- Replica 1 --> N1["🖥️ Node 04"]:::active
      Chunk1 -- Replica 2 --> N2["🖥️ Node 12"]:::active
      Chunk1 -- Replica 3 --> N3["❌ Node 27 (OFFLINE)"]:::offline
      Chunk1 -- Replica 4 --> N4["🖥️ Node 33"]:::active
      Chunk1 -- Replica 5 --> N5["🖥️ Node 48"]:::active
   end

   Client["👤 Client Downloader"]

   Client -->|"1. DHT Lookup"| N1
   Client -.->|"2. UDP Broadcast (Fallback)"| N2
   Client -.->|Timeout| N3
   Client -->|"3. Download (HTTP)"| N4

   Note["Hybrid Discovery: DHT + UDP"]
```

## 4. Communication Protocol Specification

### Protocol Layers

The system relies on a dual-protocol stack: UDP for low-latency control and discovery, and HTTP for reliable data transfer, augmented by a Kademlia-based DHT overlay.

#### A. Discovery & Control Layer (UDP + DHT)

- **UDP Port**: `9999` (Broadcast)
- **Role**: Peer Discovery, Liveness, Local Content Search.

**1. HELLO Message (Beacon)**
Broadcasted every 1s by every active node.

- **Payload**:
  ```json
  {
    "type": "HELLO",
    "port": 8000 // The HTTP port of sending node
  }
  ```

**2. DHT Messages (RPC Over HTTP)**
Nodes maintain a routing table for efficient lookups.

- **PING**: Check node liveness.
- **STORE**: Publish key-value pair (ChunkID -> NodeURL).
- **FIND_NODE**: Find closest nodes to ID.
- **FIND_VALUE**: Find providers for a ChunkID.

#### B. Data Transport Layer (HTTP/REST)

- **Ports**: Dynamic range (8000-80XX).
- **Role**: Heavy data transfer, Node Management, DHT RPC.

**Endpoints:**

| Method   | Endpoint          | Description                                            |
| :------- | :---------------- | :----------------------------------------------------- |
| **GET**  | `/chunks`         | **Inventory**. Returns list of all stored chunk IDs.   |
| **GET**  | `/chunk/{id}`     | **Download**. Stream the binary content of the chunk.  |
| **PUT**  | `/chunk/{id}`     | **Upload/Replicate**. Save a chunk to this node.       |
| **POST** | `/dht/store`      | **DHT**. Store key-value pair.                         |
| **POST** | `/dht/find_value` | **DHT**. Find providers for value.                     |
| **POST** | `/unjoin`         | **Leave**. Trigger graceful exit and data offload.     |
| **GET**  | `/status`         | **Health**. Node stats.                                |
| **GET**  | `/openapi`        | **Spec**. Returns full Swagger/OpenAPI 3.0 definition. |

### Protocol Interaction Diagram

The following diagram illustrates the detailed message flow between nodes for discovery and data exchange (Hybrid Mode).

```mermaid
sequenceDiagram
    autonumber
    participant NodeA as 🖥️ Node A (Requester)
    participant Broad as 📣 Broadcast / DHT
    participant NodeB as 🖥️ Node B (Holder)
    participant NodeC as 🖥️ Node C (Other Peer)

    Note over NodeA, NodeC: 1. Peer Discovery (Bootstrap)
    NodeA->>Broad: UDP HELLO / DHT PING
    NodeB->>NodeB: Update Routing Table
    NodeC->>NodeC: Update Routing Table

    Note over NodeA, NodeC: 2. Content Discovery (Hybrid)
    NodeA->>Broad: DHT FIND_VALUE {id: "hash123"}
    alt Found in DHT
        Broad-->>NodeA: Found Providers: [NodeB]
    else Not Found
        NodeA->>Broad: UDP QUERY_CHUNK {id: "hash123"} (Fallback)
        NodeB->>NodeA: I_HAVE {url: "http://NodeB:8001"}
    end

    Note over NodeA, NodeC: 3. Data Transfer (Reliable HTTP)
    NodeA->>NodeB: GET /chunk/hash123
    activate NodeB
    NodeB-->>NodeA: 200 OK (Binary Stream)
    deactivate NodeB
```

---

## 5. Network Topology & Discovery

The network is fully decentralized and coordinator-free (Master-less). Discovery occurs via broadcast protocols, eliminating the need for manual configuration.

### Connectivity

The network implements a mesh topology where every node is potentially connected to every other node discovered via UDP.

```mermaid
graph TD
   classDef node fill:#198754,stroke:#333,stroke-width:2px,color:white;
   classDef client fill:#0d6efd,stroke:#333,stroke-width:2px,color:white;
   classDef link stroke:#666,stroke-width:1px,stroke-dasharray: 5 5;

   Client["👤 Client App"]:::client

   subgraph "Local Area Network (LAN)"
      direction BT
      N1["🖥️ Node 01"]:::node
      N2["🖥️ Node 02"]:::node
      N3["🖥️ Node 50"]:::node
   end

   %% Broadcast Discovery
   Client -.-o|"UDP: Broadcast / DHT RPC"| N1
   Client -.-o|"UDP: Broadcast / DHT RPC"| N2
   Client -.-o|"UDP: Broadcast / DHT RPC"| N3

   %% Mesh interactions (logical)
   N1 <-->|"HTTP: Transfer / DHT Gossip"| N2
   N2 <-->|"HTTP: Transfer / DHT Gossip"| N3

   Note["📡 Hybrid Discovery:<br/>Port 9999/UDP (Beacon)<br/>DHT over HTTP (Find)"]
```

## 6. Node Lifecycle Management

The lifecycle of a node consists of three main phases: Join, Active Participation, and Departure.

```mermaid
stateDiagram-v2
    [*] --> Initialization
    Initialization --> Active: UDP Beacon Start (Port 9999)

    state Active {
        [*] --> Idle
        Idle --> Processing : HTTP/UDP Request
        Processing --> Idle
    }

    Active --> Unjoining : POST /unjoin (Graceful)
    Active --> Offline : Crash / Net Split (Ungraceful)

    state Unjoining {
        [*] --> InventoryScan
        InventoryScan --> Offloading : Push to Peers
        Offloading --> Cleaning : Delete Storage
        Cleaning --> Shutdown
    }

    Unjoining --> [*]
    Offline --> [*]
```

### A. Initialization & Join (Bootstrap)

When a node starts (`P2PServer` initialization):

1.  **Storage Check**: Ensures the `storage_dir` exists.
2.  **Service Binding**: Binds HTTP port for data transfer and UDP port (random or fixed) for discovery.
3.  **Presence Announcement**: Immediately starts broadcasting `HELLO` beacons via UDP to port 9999.
4.  **Integration**: Other nodes receive the beacon and add the new node to their in-memory peer lists. No central registration is required.

### B. Active State (Liveness)

- **Heartbeat**: The node continues to broadcast `HELLO` packets every second.
- **Responsive**: It answers `QUERY_CHUNK` broadcasts if it holds requested data.
- **Passive Maintenance**: It accepts `PUT` requests from other peers (re-balancing or upload) and `GET` requests for downloads.

### C. Graceful Unjoin Protocol (Planned Departure)

Nodes can leave the network without causing data loss through the "Unjoin" process (`POST /unjoin`):

1. **Inventory Analysis**: The node scans its local storage for all hosted chunks.
2. **Cluster Replica Discovery**: The node scans for available peers in the cluster via Discovery Service.
3. **Data Offloading**:
   - The node actively pushes (PUT) each chunk to a random active peer.
   - This prevents the replication factor from dropping.
4. **Self-Destruct**:
   - Once all data is transferred, the node recursively deletes its local storage directory.
   - The process terminates gracefully.

```mermaid
sequenceDiagram
   participant Admin as 🔑 Admin
   participant Node as 🖥️ Node (Exiting)
   participant Cluster as 🌐 P2P Cluster

   Admin->>Node: POST /unjoin
   Node->>Node: Scan Local Storage (Inventory)
   Node->>Cluster: Discovery (Find Active Peers)

   loop For each Chunk
      Node->>Cluster: PUT /chunk/{id} (Offload Data)
      Cluster-->>Node: 200 OK (Confirmed)
   end

   Node->>Node: Delete Storage Directory
   Node->>Node: Shutdown Process
```

### D. Ungraceful Failure (Crash Recovery)

If a node crashes or is disconnected abruptly (Power loss, Network partition):

1. **Detection**:
   - The node stops sending UDP Beacons.
   - HTTP requests to the node fail (Timeout/Connection Refused).
2. **Impact**: The chunks hosted on that node become temporarily unavailable.
3. **Resilience**:
   - Due to $N=5$ redundancy, the file remains available on 4 other nodes.
   - The `RepairManager` (triggered manually or periodically) detects the missing replica during an audit and replicates the data to a new node to restore $N=5$.

```mermaid
flowchart TD
    Start[Repair Scan Started] --> Discover[Discover Active Nodes]
    Discover --> Check[Load File Manifest]
    Check --> Verify[Broadcast QUERY_CHUNK]

    Verify -- "5 Nodes Respond" --> OK[✅ Healthy]
    Verify -- "< 5 Nodes Respond" --> Loss[⚠️ Replica Loss Detected]

    Loss --> Select[Select New Candidate Node]
    Select --> Retrieve[Retrieve Chunk from a Survivor]
    Retrieve --> Replicate[PUT to New Candidate]

    Replicate --> Restore["✅ Redundancy Restored (N=5)"]
```
