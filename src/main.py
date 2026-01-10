from core.merkle import MerkleTree
from network.discovery import scan_network
import argparse
import os
import sys
import json
import asyncio
import hashlib
import random
import time
import concurrent.futures
from typing import List
from core.crypto import CryptoManager
from core.sharding import ShardManager
from core.metadata import MetadataManager
from core.distribution import DistributionStrategy
from network.node import LocalDirNode, StorageNode
from network.p2p_server import P2PServer
from network.remote_node import RemoteHttpNode


def setup_local_network(base_dir="network_data") -> List[StorageNode]:
    """Initializes a simulated network of local nodes (Fallback)."""
    nodes = []
    for i in range(1, 6):
        node = LocalDirNode(f"node_{i}", base_path=base_dir)
        nodes.append(node)
    return nodes


def setup_remote_network(entry_node: str) -> List[StorageNode]:
    """
    Discovers the network starting from an entry node.
    Returns a list of RemoteHttpNode adapters.
    """
    if not entry_node:
        raise ValueError("Entry node URL is required")

    print(f"Discovering peers via {entry_node}...")
    try:
        # Use the client to ask for peers from the entry node
        temp_node = RemoteHttpNode(entry_node)

        # Ask for the peers list directly via HTTP using the get_peers method
        peer_urls = temp_node.get_peers()

        # Add entry node too if missing
        entry_normalized = entry_node.rstrip('/')
        if entry_normalized not in peer_urls:
            peer_urls.append(entry_normalized)

        print(f"Network discovered: {len(peer_urls)} nodes -> {peer_urls}")

        nodes = []
        for url in peer_urls:
            nodes.append(RemoteHttpNode(url))
        return nodes

    except Exception as e:
        print(f"Discovery failed: {e}")
        return [RemoteHttpNode(entry_node)]


def visualize_network_cmd(args):
    """Explores the network and generates a graph diagram."""
    import urllib.request
    import json
    import time

    start_nodes = []
    if args.entry_node:
        start_nodes.append(args.entry_node.rstrip('/'))
    elif args.scan:
        print("Scanning network...")
        found = asyncio.run(scan_network())
        start_nodes.extend([u.rstrip('/') for u in found])

    if not start_nodes:
        print("No starting node. Use --scan or --entry-node.")
        return

    print(f"Starting crawling from: {start_nodes}")
    visited = set()
    edges = set()
    queue = list(start_nodes)

    while queue:
        current_node = queue.pop(0)
        if current_node in visited:
            continue
        visited.add(current_node)

        try:
            # req = urllib.request.Request(f"{current_node}/peers", method='GET')
            # req.add_header('Connection', 'close')
            # with urllib.request.urlopen(req, timeout=2) as resp:
            #     data = json.loads(resp.read())
            #     peers = [p.rstrip('/') for p in data.get('peers', [])]

            # Use a simple call for now
            with urllib.request.urlopen(f"{current_node}/peers", timeout=2) as resp:
                data = json.loads(resp.read())
                peers = [p.rstrip('/') for p in data.get('peers', [])]

            for p in peers:
                if p != current_node:
                    # Adds directed edge
                    edges.add((current_node, p))
                    if p not in visited and p not in queue:
                        queue.append(p)

        except Exception as e:
            print(f"Error contacting {current_node}: {e}")

    print("\n--- Network Topology (Mermaid) ---")
    print("graph TD")
    for src, dst in edges:
        s_id = src.replace("http://", "").replace(".", "_").replace(":", "_")
        d_id = dst.replace("http://", "").replace(".", "_").replace(":", "_")
        print(f"    {s_id}[{src}] --> {d_id}[{dst}]")
    print("----------------------------------\n")


def prune_orphans(args=None):
    """Deletes chunks that are not referenced by any manifest."""
    manifests_dir = 'manifests'
    network_data_dir = 'network_data'

    if not os.path.exists(manifests_dir):
        print("No manifests directory found.")
        return

    # 1. Collect Valid Chunk IDs
    valid_chunks = set()
    if os.path.exists(manifests_dir):
        manifest_files = [f for f in os.listdir(
            manifests_dir) if f.endswith('.manifest')]
        print(f"Scanning {len(manifest_files)} manifests...")

        for mf in manifest_files:
            path = os.path.join(manifests_dir, mf)
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    for chunk in data.get('chunks', []):
                        valid_chunks.add(chunk['id'])
            except Exception as e:
                print(f"Error reading manifest {mf}: {e}")

    # 1B. Fetch Public Catalog (New)
    try:
        # Define async helper to fetch catalog and parse remote manifests
        async def _sync_catalog_refs():
            print("Scanning network for Public Catalog...")
            peers = await scan_network(timeout=2)
            if not peers:
                # Fallback
                peers = ['http://127.0.0.1:8000']

            nodes = [RemoteHttpNode(p) for p in peers]

            # Fetch Catalog List
            cat = CatalogClient()
            items = await cat.fetch(nodes)
            print(f"Catalog contains {len(items)} items.")

            new_refs = set()

            # 1. Protect Manifest IDs themselves
            for item in items:
                new_refs.add(item['id'])

            # 2. Protect Chunks referenced by these Manifests
            # Optimization: We scan local disks to find the Manifest Body
            # instead of downloading from network (since we are pruning local data).

            for item in items:
                m_id = item['id']
                # Look for this file in any node_data dir
                found_loc = False
                for node_dir in os.listdir(network_data_dir):
                    maybe_path = os.path.join(network_data_dir, node_dir, m_id)
                    if os.path.exists(maybe_path):
                        try:
                            with open(maybe_path, 'rb') as f:
                                m_data = json.load(f)
                                for c in m_data.get('chunks', []):
                                    new_refs.add(c['id'])
                            found_loc = True
                            break  # Found valid copy, no need to check other nodes
                        except:
                            pass

                if not found_loc:
                    print(
                        f"Warning: Manifest {m_id} not found locally during prune scan. Referencing chunks might be lost if not shared.")

            return new_refs

        # Run async logic
        catalog_refs = asyncio.run(_sync_catalog_refs())
        valid_chunks.update(catalog_refs)
        print(
            f"Total valid chunks (including Public Catalog): {len(valid_chunks)}")

    except Exception as e:
        print(f"Warning: Could not sync with public catalog: {e}")

    print(f"Found {len(valid_chunks)} unique valid chunks referenced.")

    # 2. Scan Node Data Directories
    deleted_count = 0
    reclaimed_space = 0

    if not os.path.exists(network_data_dir):
        print(f"No {network_data_dir} found. Nothing to prune.")
        return

    for node_dir in os.listdir(network_data_dir):
        node_path = os.path.join(network_data_dir, node_dir)
        if not os.path.isdir(node_path):
            continue

        # print(f"Scanning {node_dir}...")
        for chunk_file in os.listdir(node_path):
            chunk_path = os.path.join(node_path, chunk_file)
            if not os.path.isfile(chunk_path):
                continue

            # Skip hidden files or non-chunk files if any (usually just hex IDs)
            if chunk_file.startswith('.'):
                continue

            if chunk_file not in valid_chunks:
                try:
                    size = os.path.getsize(chunk_path)
                    os.remove(chunk_path)
                    deleted_count += 1
                    reclaimed_space += size
                    # print(f"Deleted orphan: {chunk_file} from {node_dir}")
                except Exception as e:
                    print(f"Failed to delete {chunk_path}: {e}")

    print(f"Pruning complete.")
    print(f"Deleted {deleted_count} orphan chunks.")
    print(f"Reclaimed {reclaimed_space / (1024*1024):.2f} MB.")


def start_server(args):
    """Starts a P2P server node."""
    full_path = os.path.abspath(args.storage_dir)
    server = P2PServer(
        host=args.host,
        port=args.port,
        storage_dir=full_path,
        known_peer=args.join if args.join else None
    )
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server stopped.")


class CatalogClient:
    """Helper to interact with the Distributed Catalog via DHT."""

    def __init__(self, dht=None):
        # Needs access to a DHT instance (usually from P2PServer or Standalone)
        self.dht = dht

    def get_catalog_key(self):
        # Simplification: Single global bucket for prototype
        # In prod: bucket by date or first char of filename
        return hashlib.sha256(b"catalog_global_v1").hexdigest()

    async def publish(self, manifest_id, filename, size, distributor_nodes):
        """
        Publishes a manifest to the public catalog.

        Since we don't have a running P2PServer instance in CLI mode usually, 
        we use the Distributor's nodes to execute DHT STORE commands remotely.
        """
        key = self.get_catalog_key()
        # Prefix key for special handling in dht.py
        dht_key = "catalog_" + key[:56]

        entry = json.dumps({
            "id": manifest_id,
            "name": filename,
            "size": size,
            "ts": int(time.time())
        })

        print(f"Publishing to Catalog Key: {dht_key}")

        # We need to find nodes responsible for this key, or just broadcast to 5 random nodes
        # For simplicity in this hybrid model, we send STORE to random nodes we know,
        # hoping they participate in the DHT.

        success_count = 0
        import aiohttp

        client_id_hex = hashlib.sha256(b"client_cli").hexdigest()
        client_info = {"sender_id": client_id_hex,
                       "host": "127.0.0.1", "port": 0}

        async with aiohttp.ClientSession() as session:
            # Try to publish to up to 20 nodes to ensure propagation
            targets = distributor_nodes[:20]
            for node in targets:
                try:
                    url = f"{node.url}/dht/store"
                    # We spoof sender for now as we are a CLI client without a permanent port usually
                    payload = {
                        "key": dht_key,
                        "value": entry,
                        "sender": client_info
                    }
                    async with session.post(url, json=payload, timeout=2) as resp:
                        if resp.status == 200:
                            success_count += 1
                except Exception as e:
                    pass

        if success_count > 0:
            print(f"✅ Published to Catalog on {success_count} nodes.")
        else:
            print(f"⚠️ Failed to publish to catalog.")

    async def delete(self, manifest_id, distributor_nodes):
        """
        Removes a manifest from the public catalog.
        """
        key = self.get_catalog_key()
        dht_key = "catalog_" + key[:56]

        print(
            f"Removing from Catalog {dht_key} and Purging Key {manifest_id[:8]}...")

        client_id_hex = hashlib.sha256(b"client_cli").hexdigest()
        client_info = {"sender_id": client_id_hex,
                       "host": "127.0.0.1", "port": 0}

        # Payload 1: Remove from Catalog List
        payload_list = {
            "key": dht_key,
            "value": manifest_id,
            "sender": client_info
        }

        # Payload 2: Remove ID key (if stored as KV)
        payload_kv = {
            "key": manifest_id,
            "value": "",
            "sender": client_info
        }

        success_count = 0
        import aiohttp
        async with aiohttp.ClientSession() as session:
            targets = distributor_nodes
            print(f"Catalog Delete: Broadcasting to {len(targets)} nodes...")

            tasks = []
            for node in targets:
                url = f"{node.url}/dht/delete"
                # Send both requests
                tasks.append(session.post(url, json=payload_list, timeout=2))
                tasks.append(session.post(url, json=payload_kv, timeout=2))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if not isinstance(res, Exception) and res.status == 200:
                        success_count += 0.5

        if success_count > 0:
            print(f"✅ Removed from Catalog/DHT on {int(success_count)} nodes.")
        else:
            print(f"⚠️ Failed to remove from catalog (or node unreachable).")

    async def fetch(self, distributor_nodes):
        """
        Retrieves the catalog from the network.
        """
        key = self.get_catalog_key()
        dht_key = "catalog_" + key[:56]

        print(f"Fetching Catalog from {dht_key}...")

        client_id_hex = hashlib.sha256(b"client_cli").hexdigest()
        client_info = {"sender_id": client_id_hex,
                       "host": "127.0.0.1", "port": 0}

        catalogs = []
        import aiohttp
        async with aiohttp.ClientSession() as session:
            # Query ALL known nodes to ensure we don't miss any data due to partition
            # random.shuffle(distributor_nodes)
            targets = distributor_nodes  # [:20] Removed limit

            # Helper for parallel fetch
            async def fetch_one(node_url):
                try:
                    url = f"{node_url}/dht/find_value"
                    payload = {
                        "key": dht_key,
                        "sender": client_info
                    }
                    async with session.post(url, json=payload, timeout=2) as resp:
                        res = await resp.json()
                        if "value" in res:
                            val = res["value"]
                            if isinstance(val, list):
                                return val
                            else:
                                return [val]
                except:
                    pass
                return []

            tasks = [fetch_one(node.url) for node in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, list):
                    catalogs.extend(res)

        # Deduplicate
        unique = {}
        for c in catalogs:
            try:
                obj = json.loads(c) if isinstance(c, str) else c
                unique[obj['id']] = obj
            except:
                pass

        return list(unique.values())


def distribute(args, progress_callback=None):
    """Upload/distribution logic with redundancy."""
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    if progress_callback:
        progress_callback(0, "Scanning network...")

    # Choose network type
    if args.entry_node:
        nodes = setup_remote_network(args.entry_node)
        redundancy = min(len(nodes), 2)
    elif args.scan:
        print("Auto-scanning network...")
        # Use async utility function to find peers
        found_peers = asyncio.run(scan_network())
        if not found_peers:
            msg = "No peers found automatically. Make sure nodes are started."
            print(msg)
            raise RuntimeError(msg)

        print(f"Peers found: {found_peers}")
        # Use the first found peer as entry point to discover the rest of the network
        # or use them all directly
        nodes = []
        for url in found_peers:
            nodes.append(RemoteHttpNode(url))
        # Increase redundancy to 5 as requested, or user override
        redundancy = getattr(args, 'redundancy', 5)
        # Cap redundancy to node count (can't have more copies than nodes)
        if redundancy > len(nodes):
            redundancy = len(nodes)
        if redundancy < 1:
            redundancy = 1
    else:
        print("No --entry-node or --scan specified, using local simulation.")
        nodes = setup_local_network()
        # Increase redundancy for local fallback too if possible (but local nodes are only 5 usually)
        redundancy = getattr(args, 'redundancy', 5)

    distributor = DistributionStrategy(nodes, redundancy_factor=redundancy)

    print(
        f"Mode: {'REMOTE P2P' if (args.entry_node or args.scan) else 'LOCAL SIMULATION'}")
    print(f"Active nodes: {len(nodes)}")
    print(f"Redundancy: {redundancy}x")

    print("Generating encryption key...")
    key = CryptoManager.generate_key()

    shard_mgr = ShardManager(key)
    print("Processing file and streaming uploads...")

    # Process is now a generator
    use_compression = getattr(args, 'compression', True)
    chunk_generator = shard_mgr.process_file(
        file_path, compression=use_compression)

    chunks_info_for_manifest = []

    def upload_chunk_task(chunk):
        print(f"Distributing chunk {chunk['index']} ({chunk['id'][:8]}...)...")
        try:
            locations = distributor.distribute_chunk(
                chunk['id'], chunk['data'])
            print(f"  -> Saved on: {', '.join(locations)}")
            chunk['locations'] = locations
            return chunk, None
        except RuntimeError as e:
            return None, f"Critical error distributing chunk {chunk['index']}: {e}"

    print(f"Starting pipeline (Processing -> Upload)...")
    if progress_callback:
        progress_callback(5, "Starting processing pipeline...")

    processed_bytes = 0
    sharded_bytes = 0
    total_file_size = os.path.getsize(file_path)

    pending_futures = set()

    def update_combined_progress():
        if progress_callback and total_file_size > 0:
            # 30% weight for Sharding, 70% for Distribution
            p_shard = (sharded_bytes / total_file_size) * 30
            p_dist = (processed_bytes / total_file_size) * 69
            pct = int(p_shard + p_dist)
            if pct >= 100:
                pct = 99

            # Message priorities
            msg = f"Processing: {int(sharded_bytes/1024)}KB | Distributed: {int(processed_bytes/1024)}KB"
            progress_callback(pct, msg)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        # 1. Pipeline Loop: Shard -> Submit -> Check Done (Interleaved)
        for chunk in chunk_generator:
            sharded_bytes += chunk['original_size']

            # Submit upload task
            future = executor.submit(upload_chunk_task, chunk)
            pending_futures.add(future)

            # Check for any completed tasks (non-blocking)
            done, _ = concurrent.futures.wait(pending_futures, timeout=0)
            for f in done:
                pending_futures.remove(f)
                chunk_res, error = f.result()
                if error:
                    print(error)
                    if progress_callback:
                        progress_callback(-1, f"Error: {error}")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                chunks_info_for_manifest.append(chunk_res)
                processed_bytes += chunk_res.get('original_size', 0)

            update_combined_progress()

        # 2. Drain remaining tasks
        for f in concurrent.futures.as_completed(pending_futures):
            chunk_res, error = f.result()
            if error:
                print(error)
                if progress_callback:
                    progress_callback(-1, f"Error: {error}")
                return

            chunks_info_for_manifest.append(chunk_res)
            processed_bytes += chunk_res.get('original_size', 0)
            update_combined_progress()

    # Sort by index to keep manifest clean
    chunks_info_for_manifest.sort(key=lambda x: x['index'])

    meta_mgr = MetadataManager(manifest_dir=None)  # Start serverless
    manifest_dict = meta_mgr.create_manifest(
        os.path.basename(file_path), key, chunks_info_for_manifest, compression=use_compression)

    # --- Distribute Manifest (Cloud) ---
    print("Uploading Manifest to Network...")

    # Serialize to memory only - No local file in 'manifests/'
    manifest_bytes = json.dumps(manifest_dict).encode('utf-8')

    manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
    try:
        distributor.distribute_chunk(manifest_id, manifest_bytes)
        print(f"✅ Manifest Distributed: {manifest_id}")
        print(f"   (Share this ID to download the file directly)")
    except Exception as e:
        print(f"⚠️  Failed to distribute manifest: {e}")
        manifest_id = "UPLOAD_FAILED"

    # --- Publish to Public Catalog (Optional) ---
    if manifest_id != "UPLOAD_FAILED":
        try:
            # Pass the raw RemoteHttpNode list used by distributor
            active_nodes = distributor.nodes

            # Filter only Remote nodes (LocalDirNode doesn't support HTTP catalog)
            remote_nodes = [n for n in active_nodes if hasattr(n, 'url')]

            if remote_nodes:
                print("Publishing to Public Catalog...")
                cat = CatalogClient()
                asyncio.run(cat.publish(
                    manifest_id,
                    os.path.basename(file_path),
                    # approx size
                    chunks_info_for_manifest[-1].get(
                        'original_size', 0) * len(chunks_info_for_manifest),
                    remote_nodes
                ))
        except Exception as e:
            print(f"Catalog publish error: {e}")
    # --------------------------------------------

    msg = f"Success! Manifest ID: {manifest_id} | File: {os.path.basename(file_path)}"
    print(msg)
    if progress_callback:
        progress_callback(100, msg)
    # print(f"Manifest saved in: {manifest_path}")


def prepare_network_for_reconstruct(args, progress_callback=None):
    """Refactored network discovery for reuse."""
    if progress_callback:
        progress_callback(10, "Discovering network...")

    if args.entry_node:
        print(f"Using manual entry node: {args.entry_node}")
        return setup_remote_network(args.entry_node)
    elif args.scan:
        print("Auto-scanning network for recovery...")
        found_peers = asyncio.run(scan_network())
        if not found_peers:
            msg = "No nodes found. Cannot start recovery."
            print(msg)
            if progress_callback:
                progress_callback(-1, msg)
            raise RuntimeError(msg)
        print(f"Peers found: {found_peers}")
        return [RemoteHttpNode(url) for url in found_peers]
    elif args.local:
        return setup_local_network()
    else:
        print("No network method specified. Trying local simulation.")
        return setup_local_network()


def collect_chunks_data(manifest, distributor, progress_callback=None):
    chunks_data = []
    print("Recovering chunks via network query (Query Flooding)...")

    if progress_callback:
        progress_callback(15, "Starting parallel download...")

    def download_chunk_task(chunk_info):
        chunk_id = chunk_info['id']
        print(f"Searching Chunk {chunk_info['index']} ({chunk_id[:8]})...")
        try:
            encrypted_data = distributor.retrieve_chunk(chunk_id)

            # INTEGRITY CHECK
            computed_hash = hashlib.sha256(encrypted_data).hexdigest()
            if computed_hash != chunk_id:
                return None, f"Corruption in Chunk {chunk_info['index']}"

            return {
                "index": chunk_info['index'],
                "data": encrypted_data
            }, None
        except Exception as e:
            return None, f"Chunk {chunk_info['index']} Fetch Error: {e}"

    print(f"Starting parallel upload (max 10 workers)...")

    total_chunks = len(manifest['chunks'])
    completed_chunks = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_chunk = {executor.submit(
            download_chunk_task, c): c for c in manifest['chunks']}

        for future in concurrent.futures.as_completed(future_to_chunk):
            result, error = future.result()
            if error:
                print(error)
                if progress_callback:
                    progress_callback(-1, f"Error: {error}")
                raise RuntimeError(error)

            chunks_data.append(result)
            completed_chunks += 1
            if progress_callback:
                pct = 15 + int((completed_chunks / total_chunks) * 75)
                progress_callback(
                    pct, f"Downloaded chunk {completed_chunks}/{total_chunks}")

    chunks_data.sort(key=lambda x: x['index'])
    return chunks_data


def reconstruct(args, progress_callback=None, stream=False):
    """Recovery/reconstruction logic."""
    manifest_source = args.manifest
    output_path = args.output

    # 1. Prepare Network (Moved up to support Manifest Download)
    try:
        nodes = prepare_network_for_reconstruct(args, progress_callback)
    except RuntimeError:
        return None

    distributor = DistributionStrategy(nodes)
    meta_mgr = MetadataManager()
    manifest_path = manifest_source
    manifest_obj = None

    # 2. Manifest Resolution (File or Network ID)
    if not os.path.exists(manifest_source):
        # Check if it looks like a valid SHA256 ID
        if len(manifest_source) == 64 and all(c in '0123456789abcdefABCDEF' for c in manifest_source):
            print(f"Detected Manifest ID. Downloading {manifest_source}...")
            if progress_callback:
                progress_callback(2, "Downloading Manifest...")
            try:
                manifest_data = distributor.retrieve_chunk(manifest_source)
                manifest_obj = json.loads(manifest_data)
                print(f"✅ Manifest downloaded (Memory)")
            except Exception as e:
                raise RuntimeError(
                    f"Failed to download manifest {manifest_source}: {e}")
        else:
            raise FileNotFoundError(
                f"Error: Manifest {manifest_source} not found.")

    if progress_callback:
        progress_callback(5, "Loading manifest...")

    if not manifest_obj:
        manifest_obj = meta_mgr.load_manifest(manifest_path)

    manifest = manifest_obj

    key = manifest['key'].encode('utf-8')
    shard_mgr = ShardManager(key)
    compression_mode = manifest.get('compression', None)

    try:
        chunks_data = collect_chunks_data(
            manifest, distributor, progress_callback)
    except RuntimeError:
        return None

    if stream:
        # Return necessary objects for streaming instead of writing to disk
        return shard_mgr, chunks_data, compression_mode, manifest

    if progress_callback:
        progress_callback(90, "Reassembling file...")

    def reassembly_monitor(done_chunks, total):
        if progress_callback:
            pct = 90 + int((done_chunks/total) * 10)
            progress_callback(
                pct, f"Reassembling: {int((done_chunks/total)*100)}%")

    shard_mgr.reconstruct_file(
        chunks_data, output_path, progress_cb=reassembly_monitor, compression_mode=compression_mode)

    print(f"File reconstructed: {output_path}")

    if progress_callback:
        progress_callback(100, "Download and reconstruction complete!")

    # Verify Merkle
    # ... (Keep existing verify logic from original code, omitted here for brevity if it was outside this function block in tool usage)
    # The previous editing tool snapshot suggests we are overwriting reconstruct completely.
    # I need to ensure I don't delete the Merkle check at the end.

    # Restoring Merkle Check logic manually since I'm overwriting the function
    expected_root = manifest.get('merkle_root')
    if expected_root:
        chunk_ids = [c['id'] for c in manifest['chunks']]
        verifier = MerkleTree(chunk_ids)
        computed_root = verifier.get_root()
        if computed_root == expected_root:
            print("✅ INTEGRITY CHECK PASSED")
        else:
            print("❌ INTEGRITY CHECK FAILED")

    key = manifest['key'].encode('utf-8')
    shard_mgr = ShardManager(key)

    chunks_data = []
    print("Recovering chunks via network query (Query Flooding)...")

    if progress_callback:
        progress_callback(15, "Starting parallel download...")

    def download_chunk_task(chunk_info):
        chunk_id = chunk_info['id']
        # locations = chunk_info.get('locations', []) # NOW IGNORED / ABSENT
        print(f"Searching Chunk {chunk_info['index']} ({chunk_id[:8]})...")
        try:
            # Call without locations -> Triggers network-wide search
            encrypted_data = distributor.retrieve_chunk(chunk_id)

            # INTEGRITY CHECK: Verify hash matches ID (Merkle Proof for Leaf)
            # The chunk ID is derived from the hash of the encrypted binary data.
            computed_hash = hashlib.sha256(encrypted_data).hexdigest()
            if computed_hash != chunk_id:
                return None, f"Data Corruption detected in Chunk {chunk_info['index']}! Hash mismatch.\nExpected: {chunk_id}\nGot: {computed_hash}"

            return {
                "index": chunk_info['index'],
                "data": encrypted_data
            }, None
        except Exception as e:
            return None, f"  -> FATAL ERROR Chunk {chunk_info['index']}: {e}"

    print(f"Starting parallel upload (max 20 workers)...")

    total_chunks = len(manifest['chunks'])
    completed_chunks = 0

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_chunk = {executor.submit(
                download_chunk_task, c): c for c in manifest['chunks']}

            for future in concurrent.futures.as_completed(future_to_chunk):
                result, error = future.result()
                if error:
                    print(error)
                    print("Stopping recovery due to fatal error.")
                    if progress_callback:
                        progress_callback(-1, f"Error: {error}")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                chunks_data.append(result)

                completed_chunks += 1
                if progress_callback:
                    # Map progress from 15% to 90%
                    pct = 15 + int((completed_chunks / total_chunks) * 75)
                    progress_callback(
                        pct, f"Downloaded chunk {completed_chunks}/{total_chunks}")

        # Important: reorder chunks before rebuilding!
        chunks_data.sort(key=lambda x: x['index'])

        if progress_callback:
            progress_callback(90, "Reassembling file...")

        def reassembly_monitor(done_chunks, total):
            if progress_callback:
                # Map progress from 90% to 100%
                pct = 90 + int((done_chunks/total) * 10)
                progress_callback(
                    pct, f"Reassembling: {int((done_chunks/total)*100)}%")

        # Reassemble the file
        shard_mgr.reconstruct_file(
            chunks_data, output_path, progress_cb=reassembly_monitor)

        print(f"File reconstructed: {output_path}")

        if progress_callback:
            progress_callback(100, "Download and reconstruction complete!")

        # --- Merkle Verification ---

        # --- Merkle Verification ---
        expected_root = manifest.get('merkle_root')
        if expected_root:
            print("\nVerifying data integrity with Merkle Tree...")

            # Re-read the downloaded chunks from manifest (which has the IDs)
            # We trust that ShardManager has written the correct bytes.
            # To be strictly correct we should verify that the downloaded chunks ID match
            # But here we verify the full set of chunk IDs from manifest against the root

            # A more robust check would happen inside ShardManager, but let's do it here
            # These are the IDs we requested
            chunk_ids = [c['id'] for c in manifest['chunks']]

            # Check if the set of IDs we used matches the Merkle Root signed in manifest
            verifier = MerkleTree(chunk_ids)
            computed_root = verifier.get_root()

            if computed_root == expected_root:
                print("✅ INTEGRITY CHECK PASSED: Merkle Root matches.")
            else:
                print("❌ INTEGRITY CHECK FAILED: Merkle Root mismatch!")
                print(f"Expected: {expected_root}")
                print(f"Computed: {computed_root}")
        else:
            print("\n⚠️  No Merkle Root in manifest. Skipping integrity check.")

    except Exception as e:
        print(f"Global error: {e}")


def catalog_cmd(args):
    """
    Lists files available in the public network catalog.
    """
    print("Fetching Global Catalog...")

    # 1. Bootstrat Network connection
    if args.entry_node:
        nodes = setup_remote_network(args.entry_node)
    elif args.scan:
        found = asyncio.run(scan_network())
        nodes = [RemoteHttpNode(u) for u in found]
    else:
        print("Please provide --entry-node or --scan to find the network.")
        return

    if not nodes:
        print("No nodes found.")
        return

    cat = CatalogClient()
    items = asyncio.run(cat.fetch(nodes))

    print(f"\n--- 🌍 Public Network Catalog ({len(items)} files) ---")
    print(f"{'MANIFEST ID':<66} | {'SIZE (B)':<10} | {'FILENAME':<30}")
    print("-" * 115)

    for item in items:
        # Check integrity
        if 'id' not in item or 'name' not in item:
            continue

        print(
            f"{item['id']:<66} | {item.get('size', 0):<10} | {item['name']:<30}")
    print("-" * 115)
    print("Use 'reconstruct <MANIFEST_ID> output_file' to download.\n")


def main():
    parser = argparse.ArgumentParser(description="Secure P2P Storage Tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Command: START NODE
    server_parser = subparsers.add_parser(
        "start-node", help="Starts a storage server node")
    server_parser.add_argument(
        "--port", type=int, default=8000, help="Port to listen on")
    server_parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Host")
    server_parser.add_argument(
        "--storage-dir", type=str, default="network_data/node_data", help="Data folder")
    server_parser.add_argument(
        "--join", type=str, help="URL of another node to join (e.g. http://localhost:8000)")

    # Command: DISTRIBUTE
    dist_parser = subparsers.add_parser("distribute", help="Distribute file")
    dist_parser.add_argument("file", help="Input file")
    dist_parser.add_argument(
        "--entry-node", help="URL of a node to connect to (e.g. http://localhost:8000)")
    dist_parser.add_argument("--scan", action="store_true",
                             help="Automatically search for nodes in local network")

    # Command: RECONSTRUCT
    rec_parser = subparsers.add_parser("reconstruct", help="Reconstruct file")
    rec_parser.add_argument("manifest", help="Manifest file")
    rec_parser.add_argument("output", help="Output file")
    rec_parser.add_argument(
        "--entry-node", help="Optional: Node URL for discovery")
    rec_parser.add_argument("--scan", action="store_true",
                            help="Use Auto-Discovery to find network")
    rec_parser.add_argument("--local", action="store_true",
                            help="Use local simulation (default if unspecified)")
    rec_parser.add_argument(
        "--kill-node", help="Local crash simulation", default=None)

    # Command: CATALOG
    cat_parser = subparsers.add_parser(
        "catalog", help="List public files in network")
    cat_parser.add_argument(
        "--entry-node", help="Optional: Node URL for discovery")
    cat_parser.add_argument("--scan", action="store_true",
                            help="Use Auto-Discovery to find network")

    # Command: VISUALIZE
    vis_parser = subparsers.add_parser(
        "visualize", help="Visualize P2P network topology")
    vis_parser.add_argument(
        "--entry-node", help="Optional: Node URL for discovery")
    vis_parser.add_argument("--scan", action="store_true",
                            help="Use Auto-Discovery to find network")

    # Command: PRUNE (Garbage Collection)
    prune_parser = subparsers.add_parser("prune", help="Delete orphan chunks")

    # Command: WEB UI
    web_parser = subparsers.add_parser("web-ui", help="Start Web UI")
    web_parser.add_argument("--port", type=int, default=8888,
                            help="Port Web UI (default: 8888)")

    args = parser.parse_args()

    if args.command == "start-node":
        start_server(args)
    elif args.command == "distribute":
        distribute(args)
    elif args.command == "reconstruct":
        reconstruct(args)
    elif args.command == "visualize":
        visualize_network_cmd(args)
    elif args.command == "prune":
        prune_orphans(args)
    elif args.command == "catalog":
        catalog_cmd(args)
    elif args.command == "web-ui":
        from web_ui import start_web_server
        start_web_server(port=args.port)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
