import asyncio
import time
import json
import hashlib
import random
import aiohttp
from typing import Optional, List

from .deps import (
    scan_network,
    RemoteHttpNode,
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
    # Note: We need to modify global variables here, so we access them from the state module
    # or assume state module objects are mutable.
    # Since simple types (int, float) are immutable, we might need a state manager class or `state.LAST_PEER_SCAN` syntax if imported as module.
    # However, importing variables directly means we have local copies if they are immutable.
    # To fix this, we should access them via the module namespace.
    from . import state

    async with state.PEER_SCAN_LOCK:
        now = time.time()
        if state.PEER_CACHE and (now - state.LAST_PEER_SCAN < CACHE_TTL):
            return list(state.PEER_CACHE)

        # Cache expired or empty, perform scan
        try:
            found = await scan_network(timeout=3)
            if found:
                state.PEER_CACHE.update(found)
                state.LAST_PEER_SCAN = now
                print(f"Updated Peer Cache: {len(state.PEER_CACHE)} peers")
            return list(state.PEER_CACHE)
        except Exception as e:
            print(f"Peer scan failed: {e}")
            return list(state.PEER_CACHE)


async def get_dht_nodes(count=5):
    """Returns a deterministic list of RemoteHttpNodes for metadata consistency."""
    stable_peers = [f"http://127.0.0.1:{8000+i}" for i in range(20)]
    return [RemoteHttpNode(u) for u in stable_peers[:count]]


async def fs_fetch_node_fn(node_id: str) -> Optional[str]:
    # Check local cache first (Read-Your-Writes)
    if node_id in NODE_CACHE:
        return NODE_CACHE[node_id]

    # print(f"FS DEBUG: Cache MISS for {node_id[:8]}, querying network...")
    gateways = await get_dht_nodes(10)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for gateway in gateways:
            url = f"{gateway.url}/dht/find_value"
            payload = {"key": node_id, "sender": {
                "sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}}

            task = asyncio.create_task(
                session.post(url, json=payload, timeout=2))
            tasks.append(task)

        pending = tasks
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    resp = await task
                    if resp.status == 200:
                        data = await resp.json()
                        if "value" in data:
                            val = data["value"]
                            for p in pending:
                                p.cancel()

                            if isinstance(val, str):
                                NODE_CACHE[node_id] = val
                                return val
                            json_val = json.dumps(val)
                            NODE_CACHE[node_id] = json_val
                            return json_val
                except Exception:
                    pass
    return None


async def fs_store_node_fn(node: DirectoryNode) -> str:
    gateways = await get_dht_nodes(5)
    node_hash = node.get_hash()

    NODE_CACHE[node_hash] = node.to_json()

    success_count = 0
    async with aiohttp.ClientSession() as session:
        async def _pusher(gw):
            nonlocal success_count
            try:
                url = f"{gw.url}/dht/store"
                payload = {
                    "key": node_hash,
                    "value": node.to_json(),
                    "sender": {"sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}
                }
                async with session.post(url, json=payload, timeout=2) as resp:
                    if resp.status == 200:
                        success_count += 1
            except:
                pass

        await asyncio.gather(*[_pusher(g) for g in gateways])

    if success_count == 0:
        raise RuntimeError(
            f"Failed to store directory node {node_hash} on any gateway")

    return node_hash


async def fs_get_root_id() -> Optional[str]:
    from . import state
    if state.CACHED_ROOT_ID:
        return state.CACHED_ROOT_ID

    gateways = await get_dht_nodes(10)
    async with aiohttp.ClientSession() as session:
        for gateway in gateways:
            try:
                url = f"{gateway.url}/dht/find_value"
                payload = {"key": FS_ROOT_KEY, "sender": {
                    "sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}}
                async with session.post(url, json=payload, timeout=2) as resp:
                    data = await resp.json()
                    if "value" in data:
                        state.CACHED_ROOT_ID = data["value"]
                        return data["value"]
            except Exception:
                pass
    return None


async def fs_set_root_id(new_root_id: str):
    from . import state
    state.CACHED_ROOT_ID = new_root_id

    gateways = await get_dht_nodes(10)
    success_count = 0
    async with aiohttp.ClientSession() as session:
        async def _pusher(gw):
            nonlocal success_count
            try:
                url = f"{gw.url}/dht/store"
                payload = {
                    "key": FS_ROOT_KEY,
                    "value": new_root_id,
                    "sender": {"sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}
                }
                async with session.post(url, json=payload, timeout=2) as resp:
                    if resp.status == 200:
                        success_count += 1
            except Exception:
                pass

        await asyncio.gather(*[_pusher(g) for g in gateways])

    if success_count == 0:
        print(
            f"Warning: Failed to update FS Root ID {FS_ROOT_KEY} on any node.")


async def check_chunk_task(session, chunk_id, peers):
    """
    Checks which nodes have a specific chunk.
    Executes HEAD requests in parallel for each known peer.
    """
    locations = []
    peer_list = list(peers)

    async def sub_check(p):
        try:
            async with session.head(f"{p}/chunk/{chunk_id}", timeout=3.0) as resp:
                if resp.status == 200:
                    return p
        except:
            pass
        return None

    tasks = [sub_check(p) for p in peer_list]
    results = await asyncio.gather(*tasks)

    locations = [r for r in results if r]
    return chunk_id, locations
