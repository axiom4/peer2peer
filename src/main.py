from network.discovery import scan_network
import argparse
import os
import sys
import asyncio
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


def distribute(args):
    """Upload/distribution logic with redundancy."""
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

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
        # Increase redundancy to 5 as requested
        redundancy = min(len(nodes), 5)
        if redundancy < 1:
            redundancy = 1
    else:
        print("No --entry-node or --scan specified, using local simulation.")
        nodes = setup_local_network()
        # Increase redundancy for local fallback too if possible (but local nodes are only 5 usually)
        redundancy = 5

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
    chunk_generator = shard_mgr.process_file(file_path)

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

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Create futures as we iterate the generator
        futures_params = {executor.submit(upload_chunk_task, chunk): chunk 
                          for chunk in chunk_generator}

        for future in concurrent.futures.as_completed(futures_params):
            chunk_res, error = future.result()
            if error:
                print(error)
                print("Stopping distribution due to critical error.")
                executor.shutdown(wait=False, cancel_futures=True)
                return

            chunks_info_for_manifest.append(chunk_res)

    # Sort by index to keep manifest clean
    chunks_info_for_manifest.sort(key=lambda x: x['index'])

    meta_mgr = MetadataManager()
    manifest_path = meta_mgr.save_manifest(
        os.path.basename(file_path), key, chunks_info_for_manifest)
    print(f"\nDistribution completed successfully!")
    print(f"Manifest saved in: {manifest_path}")


def reconstruct(args):
    """Recovery/reconstruction logic."""
    manifest_path = args.manifest
    output_path = args.output

    if not os.path.exists(manifest_path):
        print(f"Error: Manifest {manifest_path} not found.")
        return

    # In real reconstruction, ideally knowing the entry node is not needed if we save full URLs in the manifest.
    # But in our current design the manifest only has ID/URLs of the nodes.
    # If we use LocalDirNode, IDs are "node_1". If RemoteHttpNode, IDs are "http://localhost:8080".
    # So DistributionStrategy will work if we recreate the right objects.

    # If the manifest contains full URLs, we can instantiate RemoteHttpNode on the fly.
    # For simplicity, we rebuild the network as in distribute.

    meta_mgr = MetadataManager()
    manifest = meta_mgr.load_manifest(manifest_path)

    # Network discovery for recovery
    if args.entry_node:
        print(f"Using manual entry node: {args.entry_node}")
        nodes = setup_remote_network(args.entry_node)
    elif args.scan:
        print("Auto-scanning network for recovery...")
        found_peers = asyncio.run(scan_network())
        if not found_peers:
            print("No nodes found. Cannot start recovery.")
            return
        print(f"Peers found: {found_peers}")
        nodes = [RemoteHttpNode(url) for url in found_peers]
    elif args.local:
        # Force local mode
        nodes = setup_local_network()
    else:
        # Smart fallback: infer if local or remote
        # But WITHOUT reading locations from manifest (now empty/absent)
        # Assume local mode if unspecified, for backward compatibility
        print(
            "No network method specified (--entry-node, --scan). Trying local simulation.")
        nodes = setup_local_network()

    distributor = DistributionStrategy(nodes)

    key = manifest['key'].encode('utf-8')
    shard_mgr = ShardManager(key)

    chunks_data = []
    print("Recovering chunks via network query (Query Flooding)...")

    def download_chunk_task(chunk_info):
        chunk_id = chunk_info['id']
        # locations = chunk_info.get('locations', []) # NOW IGNORED / ABSENT
        print(f"Searching Chunk {chunk_info['index']} ({chunk_id[:8]})...")
        try:
            # Call without locations -> Triggers network-wide search
            encrypted_data = distributor.retrieve_chunk(chunk_id)
            return {
                "index": chunk_info['index'],
                "data": encrypted_data
            }, None
        except Exception as e:
            return None, f"  -> FATAL ERROR Chunk {chunk_info['index']}: {e}"

    print(f"Starting parallel upload (max 5 workers)...")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_chunk = {executor.submit(
                download_chunk_task, c): c for c in manifest['chunks']}

            for future in concurrent.futures.as_completed(future_to_chunk):
                result, error = future.result()
                if error:
                    print(error)
                    print("Stopping recovery due to fatal error.")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                chunks_data.append(result)

        # Important: reorder chunks before rebuilding!
        chunks_data.sort(key=lambda x: x['index'])

        shard_mgr.reconstruct_file(chunks_data, output_path)
        print(f"File reconstructed: {output_path}")

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
    elif args.command == "web-ui":
        from web_ui import start_web_server
        start_web_server(port=args.port)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
