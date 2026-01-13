import aiohttp
from aiohttp import web
from ..deps import scan_network, CatalogClient, RemoteLibP2PNode
from ..services import get_active_peers
from .. import state
import asyncio


async def fetch_catalog(request):
    """
    Fetches the global public catalog from the network.
    """
    try:
        # Use active peers from service (handles caching and filtering self)
        peers = await get_active_peers()
        
        nodes = []
        server = state.GLOBAL_P2P_SERVER
        
        if server and peers:
            loop = asyncio.get_running_loop()
            for p in peers:
                # p is a multiaddr string
                node = RemoteLibP2PNode(p, server.bridge, loop)
                nodes.append(node)

        cat = CatalogClient()
        items = await cat.fetch(nodes)

        # Returns list of {id, name, size, ts}
        return web.json_response(items)

    except Exception as e:
        print(f"Catalog fetch error: {e}")
        return web.json_response({"error": str(e)}, status=500)


async def get_network_graph(request):
    try:
        # Use get_active_peers which handles filtering and caching
        peers = await get_active_peers()
        
        nodes = []
        edges = []
        
        # Self node
        local_id = "WebUI"
        server = state.GLOBAL_P2P_SERVER
        if server:
            local_id = server.host.get_id().to_string()
            
        nodes.append({"id": local_id, "label": "This Node (WebUI)", "group": "local"})
        
        for p in peers:
            # p is a multiaddr like /ip4/.../p2p/QmID
            parts = p.split('/')
            peer_id = parts[-1] if 'p2p' in parts else p
            short_label = peer_id[:8] + "..."
            
            nodes.append({"id": peer_id, "label": short_label})
            # We are connected to them
            edges.append({"from": local_id, "to": peer_id})

        return web.json_response({
            "nodes": nodes,
            "edges": edges
        })
    except Exception as e:
        print(f"Graph error: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)
