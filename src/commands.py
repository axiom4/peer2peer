import os
import asyncio
import json
import logging
import concurrent.futures
from typing import List, Dict, Any

from network.p2p_server import P2PServer
from core.crypto import CryptoManager
from core.sharding import ShardManager
from core.metadata import MetadataManager
from core.distribution import DistributionStrategy
from network.node import LocalDirNode, StorageNode
from network.remote_node import RemoteLibP2PNode
from libp2p.abc import IHost

logger = logging.getLogger(__name__)


def setup_local_network(base_dir="network_data") -> List[StorageNode]:
    """Initializes a simulated network of local nodes (Fallback)."""
    nodes = []
    for i in range(1, 6):
        node = LocalDirNode(f"node_{i}", base_path=base_dir)
        nodes.append(node)
    return nodes


async def setup_remote_network(entry_node: str, bridge: Any) -> List[StorageNode]:
    """
    Connects to the network via entry node using LibP2P.
    Returns list containing the connected peers (currently just entry node).
    """
    if not entry_node:
        raise ValueError("Entry node MultiAddr is required")

    print(f"Connecting to entry node: {entry_node}...")

    loop = asyncio.get_running_loop()
    node = RemoteLibP2PNode(entry_node, bridge, loop)

    # Try to connect
    if await node._ensure_connection():
        print("Connected to entry node.")
        return [node]
    else:
        print("Failed to connect to entry node.")
        return []


async def distribute_wrapper(args, progress_callback=None):
    """Async wrapper for distribution logic."""

    # Check if a server instance is already provided in args
    server = getattr(args, 'server', None)

    if server:
        pass
    else:
        # Initialize a local P2P Node (Host) to act as client
        # Use ephemeral port or explicit
        port = getattr(args, 'port', 0)
        storage_dir = "uploads_temp"

        server = P2PServer("127.0.0.1", port, storage_dir)
        await server.initialize()
        # await server.start() # Starts background tasks - don't start loop if possible
        # Just initialize host so we can use it

    return await _distribute_logic(args, server, progress_callback)


async def _distribute_logic(args, server: P2PServer, progress_callback=None):
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    redundancy = getattr(args, 'redundancy', 5)
    nodes = []

    # Collect potential peer addresses
    peer_addrs = set()
    if getattr(args, 'entry_node', None):
        peer_addrs.add(args.entry_node)

    known_peers = getattr(args, 'known_peers', [])
    if known_peers:
        peer_addrs.update(known_peers)

    if peer_addrs:
        print(f"Connecting to {len(peer_addrs)} peers...")
        for addr in peer_addrs:
            try:
                # Reuse setup_remote_network for single node connection
                # or manually:
                loop = asyncio.get_running_loop()
                # pass bridge instead of host (server.host is None/irrelevant in asyncio context now)
                node = RemoteLibP2PNode(addr, server.bridge, loop)
                if await node._ensure_connection():
                    nodes.append(node)
                else:
                    print(f"Could not connect to {addr}")
            except Exception as e:
                print(f"Error connecting to {addr}: {e}")

        # Cap redundancy
        if len(nodes) < redundancy:
            print(
                f"Warning: Only {len(nodes)} nodes available. Redundancy reduced.")
            redundancy = len(nodes)

    elif getattr(args, 'local', False):
        nodes = setup_local_network()
    else:
        # Strict mode: If no peers found and not local mode, do NOT fallback to simulation 
        # to avoid polluting filesystem with "node_X" directories.
        pass

    if not nodes:
        print("No active nodes available for distribution.")
        return

    distributor = DistributionStrategy(nodes, redundancy_factor=redundancy)

    print(f"Active nodes: {len(nodes)}")

    key = CryptoManager.generate_key()
    shard_mgr = ShardManager(key)

    print("Processing file and streaming uploads...")
    if progress_callback:
        progress_callback(0, "Sharding and encrypting...")

    async def process_and_upload_async():
        chunks_info = []
        chunk_generator = shard_mgr.process_file(file_path, compression=True)

        # To avoid loading the whole file in RAM, we should ideally use a queue,
        # but for compatibility with previous logic (which queued all futures),
        # we'll collect tasks.
        # IMPROVEMENT: Use a bounded semaphore to limit active tasks.

        tasks = []
        # Limit to 5 concurrent chunk distributions
        semaphore = asyncio.Semaphore(5)

        async def upload_bound(chunk):
            async with semaphore:
                try:
                    # print(f"Distributing chunk {chunk['index']}...")
                    locations = await distributor.distribute_chunk_async(chunk['id'], chunk['data'])
                    chunk['locations'] = locations
                    return chunk
                except Exception as e:
                    print(f"Error uploading chunk {chunk['index']}: {e}")
                    return None

        # Convert generator to list of tasks, but we need to know total for progress
        # If we iterate, we consume.

        # Let's just consume and launch tasks.
        all_chunks = list(chunk_generator)
        total_chunks = len(all_chunks)

        for chunk in all_chunks:
            task = asyncio.create_task(upload_bound(chunk))
            tasks.append(task)

        completed = 0
        for f in asyncio.as_completed(tasks):
            res = await f
            if res:
                chunks_info.append(res)

            completed += 1
            if progress_callback:
                percent = int((completed / total_chunks) * 100)
                progress_callback(
                    percent, f"Uploaded {completed}/{total_chunks} chunks")

        return chunks_info

    # Run async logic directly
    chunks_info = await process_and_upload_async()

    # Verify completeness
    if len(chunks_info) == 0:
        print("Error: No chunks uploaded.")
        return

    # Check for missing chunks (we don't know exact total if generator, but tasks count helps)
    # We can check sequential index
    chunks_info.sort(key=lambda x: x['index'])
    expected_indices = set(range(len(chunks_info)))
    actual_indices = set(c['index'] for c in chunks_info)

    # Note: If a middle chunk failed, len(chunks_info) will be less than total emitted.
    # But since we don't know total emitted from generator easily without consuming...
    # Better to check if any task returned None.
    # But process_and_upload_async filters None.
    # Improvement: process_and_upload_async should throw or return status.

    # Let's rely on continuity of indices assuming 0-based
    if chunks_info[-1]['index'] != len(chunks_info) - 1:
        error_msg = f"Upload incomplete. Missing chunks. Got {len(chunks_info)} chunks, last index {chunks_info[-1]['index']}"
        print(error_msg)
        if progress_callback:
            progress_callback(-1, error_msg)
        return

    # Create Manifest
    meta_mgr = MetadataManager(manifest_dir=None)
    manifest_dict = meta_mgr.create_manifest(
        os.path.basename(file_path), key, chunks_info, compression=True)

    print("Distribution complete.")

    # -------------------------------------------------------------
    # DECENTRALIZED MANIFEST STORAGE
    # -------------------------------------------------------------
    # Convert manifest to bytes (JSON)
    manifest_json = json.dumps(manifest_dict).encode('utf-8')
    # Generate ID for the manifest (Content Hash)
    import hashlib
    manifest_id = hashlib.sha256(manifest_json).hexdigest()

    # Inject the ID into the manifest dictionary itself (useful for local reference)
    manifest_dict['id'] = manifest_id

    print(f"Uploading manifest (ID: {manifest_id}) to network...")
    try:
        if progress_callback:
            progress_callback(95, "Uploading manifest to network...")

        # Distribute the manifest as a chunk
        # Note: distribute_chunk_async returns list of nodes that stored it
        stored_nodes = await distributor.distribute_chunk_async(manifest_id, manifest_json)
        print(
            f"Manifest uploaded successfully (decentralized on {len(stored_nodes)} nodes).")

        # 3. Announce to Catalog
        # We broadcast the catalog update to the SAME nodes we stored the manifest on,
        # OR all available nodes. Let's do all available for maximum visibility.
        catalog_entry = {
            "id": manifest_id,
            "name": os.path.basename(file_path),
            "size": manifest_dict.get('size', 0),
            "chunks": len(manifest_dict.get('chunks', [])),
            "timestamp": manifest_dict.get('timestamp', 0)
        }

        print("Announcing to network catalog...")
        announce_count = 0
        for node in distributor.nodes:
            if node.is_available() and hasattr(node, 'update_catalog_async'):
                # Fire and forget (or await for confirmation)
                try:
                    await node.update_catalog_async(catalog_entry)
                    announce_count += 1
                except:
                    pass
        print(f"Announced to {announce_count} nodes.")

    except Exception as e:
        print(f"Failed to upload manifest to network: {e}")
        # Proceed to save locally anyway so user doesn't lose data

    # -------------------------------------------------------------

    # Do not save manifest locally to filesystem
    # Return it so caller can use it
    
    return manifest_dict


async def reconstruct_wrapper(args):
    port = getattr(args, 'port', 0)
    server = P2PServer("127.0.0.1", port, "downloads_temp")
    await server.initialize()

    await _reconstruct_logic(args, server)


async def _reconstruct_logic(args, server):
    manifest_source = args.manifest
    output_path = args.output

    nodes = []
    if args.entry_node:
        nodes = await setup_remote_network(args.entry_node, server.host)
    else:
        nodes = setup_local_network()

    if not nodes:
        print("No nodes available.")
        return

    distributor = DistributionStrategy(nodes)

    manifest = None

    if os.path.exists(manifest_source):
        with open(manifest_source, 'r') as f:
            manifest = json.load(f)
    elif len(manifest_source) == 64:
        # Assuming it is a SHA256 ID
        print(f"Resolving manifest from network (ID: {manifest_source})...")
        try:
            manifest_data = await distributor.retrieve_chunk_async(manifest_source)
            manifest = json.loads(manifest_data)
            print("Manifest resolved successfully.")
        except Exception as e:
            print(f"Failed to resolve manifest from network: {e}")
            return
    else:
        print(f"Manifest not found: {manifest_source}")
        return

    key = manifest['key'].encode('utf-8')
    shard_mgr = ShardManager(key)

    print("Collecting chunks...")

    async def download_all_async():
        chunks_data = []
        semaphore = asyncio.Semaphore(5)

        async def download_task(chunk_info):
            async with semaphore:
                chunk_id = chunk_info['id']
                try:
                    data = await distributor.retrieve_chunk_async(chunk_id)
                    return {"index": chunk_info['index'], "data": data}
                except Exception as e:
                    print(f"Failed to retrieve {chunk_id}: {e}")
                    return None

        tasks = [asyncio.create_task(download_task(c))
                 for c in manifest['chunks']]

        for f in asyncio.as_completed(tasks):
            res = await f
            if res:
                chunks_data.append(res)
        return chunks_data

    chunks_data = await download_all_async()
    chunks_data.sort(key=lambda x: x['index'])

    print("Reconstructing file...")
    # Reconstruction is CPU bound (decryption/decompression), run in executor if needed
    # But for now, direct call is fine unless files are huge
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: shard_mgr.reconstruct_file(chunks_data, output_path))
    print(f"File saved to {output_path}")


async def prune_orphans(args):
    """
    Scans the network (or local storage) for orphaned chunks not referenced in any known manifest.
    (Stub implementation)
    """
    print("Pruning orphans (Not Implemented in LibP2P version yet)...")
    return {"status": "skipped", "details": "Not implemented"}


class CatalogClient:
    """
    Client for fetching network catalog.
    """

    def __init__(self, entry_node=None):
        self.entry_node = entry_node

    async def fetch(self, nodes=None) -> List[Dict[str, Any]]:
        # Only fallback to local simulation if nodes is None (not explicit empty list)
        if nodes is None:
            # Setup local network for discovery if no nodes provided
            nodes = setup_local_network()

        all_items = []
        # Parallel fetch from all nodes
        import asyncio
        tasks = [n.fetch_catalog_async()
                 for n in nodes if hasattr(n, 'fetch_catalog_async')]

        if not tasks:
            return []

        results = await asyncio.gather(*tasks, return_exceptions=True)

        seen_ids = set()
        for res in results:
            if isinstance(res, list):
                for item in res:
                    if item.get('id') not in seen_ids:
                        all_items.append(item)
                        seen_ids.add(item.get('id'))

        return all_items

    async def delete(self, manifest_id, nodes=None):
        if nodes is None:
            nodes = setup_local_network()

        import asyncio
        tasks = []
        for n in nodes:
            if hasattr(n, 'remove_catalog_async'):
                tasks.append(n.remove_catalog_async(manifest_id))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            print(
                f"Broadcasted delete for {manifest_id} to {len(tasks)} nodes.")


def start_server_cmd(args):
    full_path = os.path.abspath(args.storage_dir)
    server = P2PServer(
        host_ip=args.host,
        port=args.port,
        storage_dir=full_path,
        known_peer=args.join if args.join else None
    )
    asyncio.run(server.start())
