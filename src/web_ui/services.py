import asyncio
import time
import json
import hashlib
import random
import aiohttp
from typing import Optional, List

from .deps import (
    scan_network,
    # RemoteHttpNode,
    RemoteLibP2PNode,
    CatalogClient,
    DirectoryNode,
    FilesystemManager
)
from .state import (
    NODE_CACHE,
    PEER_CACHE,
    LAST_PEER_SCAN,
    PEER_SCAN_LOCK,
    CACHE_TTL,
    WEB_UI_NODE_ID,
    CACHED_ROOT_ID,
    FS_ROOT_KEY,
    FS_LOCK,
    FS_MANAGER
)


async def get_active_peers():
    """Returns a list of cached peers or performs a scan if cache is stale."""
    from . import state

    # If we have a P2P Server, prefer its active connections
    if state.GLOBAL_P2P_SERVER:
        try:
            return await state.GLOBAL_P2P_SERVER.get_active_peers()
        except:
            pass

    async with state.PEER_SCAN_LOCK:
        now = time.time()

        # Determine local identity to filter out self
        local_ids = []
        if state.GLOBAL_P2P_SERVER and hasattr(state.GLOBAL_P2P_SERVER, 'host'):
            try:
                local_ids.append(
                    state.GLOBAL_P2P_SERVER.host.get_id().to_string())
            except:
                pass

        if state.PEER_CACHE and (now - state.LAST_PEER_SCAN < CACHE_TTL):
            peers = list(state.PEER_CACHE)
        else:
            # Cache expired or empty, perform scan
            try:
                # 1. Try UDP Scan
                found_list = await scan_network(timeout=3)

                # 2. If Scan failed (likely port bind issue), check internal DiscoveryService
                if not found_list and state.GLOBAL_P2P_SERVER and state.GLOBAL_P2P_SERVER.discovery:
                    found_list = list(state.GLOBAL_P2P_SERVER.discovery.peers)
                    # print(f"Using internal discovery peers: {len(found_list)}")

                if found_list:
                    state.PEER_CACHE.update(found_list)
                    state.LAST_PEER_SCAN = now
                    # print(f"Updated Peer Cache: {len(state.PEER_CACHE)} peers")
                peers = list(state.PEER_CACHE)
            except Exception as e:
                print(f"Peer scan failed: {e}")
                peers = list(state.PEER_CACHE)

        # Filter out self
        filtered_peers = []
        for p in peers:
            # p is multiaddr "/ip4/.../p2p/Qm..."
            is_self = False
            for lid in local_ids:
                if lid in p:
                    is_self = True
                    break
            if not is_self:
                filtered_peers.append(p)

        return filtered_peers


async def get_dht_nodes(count=5):
    """Returns a deterministic list of Nodes for metadata consistency."""
    # stable_peers = [f"http://127.0.0.1:{8000+i}" for i in range(20)]
    # return [RemoteHttpNode(u) for u in stable_peers[:count]]
    return []  # HTTP DHT not supported in LibP2P mode yet


async def fs_fetch_node_fn(node_id: str) -> Optional[str]:
    from . import state
    # Check local cache first (Read-Your-Writes)
    if node_id in NODE_CACHE:
        return NODE_CACHE[node_id]

    server = state.GLOBAL_P2P_SERVER
    if not server:
        return None

    peers = await get_active_peers()
    loop = asyncio.get_running_loop()

    async def _try_fetch(peer_addr):
        try:
            node = RemoteLibP2PNode(peer_addr, server.bridge, loop)
            data = await node.retrieve_async(node_id)
            return data
        except:
            return None

    tasks = [_try_fetch(p) for p in peers]
    if not tasks:
        return None

    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            try:
                json_str = result.decode('utf-8')
                NODE_CACHE[node_id] = json_str
                return json_str
            except:
                pass
    return None


async def fs_store_node_fn(node: DirectoryNode) -> str:
    from . import state
    node_hash = node.get_hash()
    json_val = node.to_json()
    NODE_CACHE[node_hash] = json_val

    # Store on LibP2P Network
    server = state.GLOBAL_P2P_SERVER

    if not server:
        # If no global server (e.g. testing), we can't persist to network efficiently here
        # without spinning up a new host which is slow.
        # But for now, we just warn and allow local cache to work (it persists in memory)
        print("Warning: No Global P2P Server, FS node stored in memory only.")
        return node_hash

    peers = await get_active_peers()
    if not peers:
        print("Warning: No peers found for FS storage. Stored locally.")
        return node_hash

    data = json_val.encode('utf-8')
    loop = asyncio.get_running_loop()

    success_count = 0

    async def _pusher(peer_addr):
        nonlocal success_count
        try:
            rnode = RemoteLibP2PNode(peer_addr, server.bridge, loop)
            if await rnode.store_async(node_hash, data):
                success_count += 1
        except:
            pass

    # Fire in parallel
    await asyncio.gather(*[_pusher(p) for p in peers])

    if success_count == 0:
        # Warn but don't crash
        print(
            f"Warning: Failed to replicate directory node {node_hash} to network.")

    return node_hash


async def fs_get_root_id() -> Optional[str]:
    from . import state
    from .state import FS_ROOT_KEY
    if state.CACHED_ROOT_ID:
        return state.CACHED_ROOT_ID

    server = state.GLOBAL_P2P_SERVER
    if not server:
        return None

    peers = await get_active_peers()
    loop = asyncio.get_running_loop()

    # Try to retrieve FS_ROOT_KEY as a chunk/file
    async def _try_fetch(peer_addr):
        try:
            node = RemoteLibP2PNode(peer_addr, server.bridge, loop)
            # "Retrieving" the root key file
            data = await node.retrieve_async(FS_ROOT_KEY)
            return data
        except:
            return None

    tasks = [_try_fetch(p) for p in peers]
    if not tasks:
        return None

    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            try:
                root_id = result.decode('utf-8')
                state.CACHED_ROOT_ID = root_id
                return root_id
            except:
                pass
    return None


async def fs_set_root_id(new_root_id: str):
    from . import state
    from .state import FS_ROOT_KEY
    state.CACHED_ROOT_ID = new_root_id

    server = state.GLOBAL_P2P_SERVER
    if not server:
        # Warn if no server
        print(
            f"Warning: No Global P2P Server, FS Root ID {FS_ROOT_KEY} updated in memory only.")
        return

    peers = await get_active_peers()
    if not peers:
        print(
            f"Warning: No peers found. FS Root ID {FS_ROOT_KEY} updated locally/memory.")
        return

    data = new_root_id.encode('utf-8')
    loop = asyncio.get_running_loop()

    success_count = 0

    async def _pusher(peer_addr):
        nonlocal success_count
        try:
            rnode = RemoteLibP2PNode(peer_addr, server.bridge, loop)
            # Store FS_ROOT_KEY as a mutable file (Last Write Wins)
            if await rnode.store_async(FS_ROOT_KEY, data):
                success_count += 1
        except:
            pass

    # Fire in parallel
    await asyncio.gather(*[_pusher(p) for p in peers])

    if success_count == 0:
        print(
            f"Warning: Failed to update FS Root ID {FS_ROOT_KEY} on any node.")


async def check_chunk_task(chunk_id, peers):
    """
    Checks which nodes have a specific chunk using LibP2P.
    """
    from . import state
    if not state.GLOBAL_P2P_SERVER:
        return chunk_id, []

    # peers should be list of multiaddr strings
    locations = []

    # We need to run checks in parallel
    # We can use asyncio.gather for this, wrapping RemoteLibP2PNode calls

    server = state.GLOBAL_P2P_SERVER
    loop = asyncio.get_running_loop()

    async def sub_check(addr):
        try:
            node = RemoteLibP2PNode(addr, server.bridge, loop)
            # Use the new check_availability_async method
            if await node.check_availability_async(chunk_id):
                return addr
        except Exception:
            pass
        return None

    tasks = [sub_check(p) for p in peers]
    if not tasks:
        return chunk_id, []

    results = await asyncio.gather(*tasks)
    locations = [r for r in results if r]

    return chunk_id, locations
