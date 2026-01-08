from core.merkle import MerkleTree
from network.discovery import scan_network
import argparse
import os
import sys
import json
import asyncio
import hashlib
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

    meta_mgr = MetadataManager()
    manifest_path = meta_mgr.save_manifest(
        os.path.basename(file_path), key, chunks_info_for_manifest)

    msg = f"Distribution completed successfully! Manifest: {os.path.basename(manifest_path)}"
    print(msg)
    if progress_callback:
        progress_callback(100, msg)
    print(f"Manifest saved in: {manifest_path}")


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
    manifest_path = args.manifest
    output_path = args.output

    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Error: Manifest {manifest_path} not found.")

    if progress_callback:
        progress_callback(5, "Loading manifest...")

    meta_mgr = MetadataManager()
    manifest = meta_mgr.load_manifest(manifest_path)

    try:
        nodes = prepare_network_for_reconstruct(args, progress_callback)
    except RuntimeError:
        return None

    distributor = DistributionStrategy(nodes)
    key = manifest['key'].encode('utf-8')
    shard_mgr = ShardManager(key)

    try:
        chunks_data = collect_chunks_data(
            manifest, distributor, progress_callback)
    except RuntimeError:
        return None

    if stream:
        # Return necessary objects for streaming instead of writing to disk
        return shard_mgr, chunks_data

    if progress_callback:
        progress_callback(90, "Reassembling file...")

    def reassembly_monitor(done_chunks, total):
        if progress_callback:
            pct = 90 + int((done_chunks/total) * 10)
            progress_callback(
                pct, f"Reassembling: {int((done_chunks/total)*100)}%")

    shard_mgr.reconstruct_file(
        chunks_data, output_path, progress_cb=reassembly_monitor)

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
    elif args.command == "web-ui":
        from web_ui import start_web_server
        start_web_server(port=args.port)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
