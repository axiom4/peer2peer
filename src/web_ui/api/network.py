import aiohttp
from aiohttp import web
from ..deps import scan_network, CatalogClient, RemoteHttpNode
from ..services import get_active_peers
import asyncio


async def fetch_catalog(request):
    """
    Fetches the global public catalog from the network.
    """
    try:
        # 1. Quick discovery of some entry nodes
        peers = await scan_network()
        if not peers:
            # Fallback if scan fails
            peers = []

        nodes = [RemoteHttpNode(u) for u in peers]

        # If no nodes, try localhost:8000 as default entry
        if not nodes:
            nodes.append(RemoteHttpNode("http://127.0.0.1:8000"))

        cat = CatalogClient()
        items = await cat.fetch(nodes)

        # Returns list of {id, name, size, ts}
        return web.json_response(items)

    except Exception as e:
        print(f"Catalog fetch error: {e}")
        return web.json_response({"error": str(e)}, status=500)


async def get_network_graph(request):
    try:
        # 1. Initial Discovery
        found = await scan_network(timeout=1.5)
        edges = []
        # Normalize URLs
        nodes = set(u.rstrip('/') for u in found)

        if not nodes:
            return web.json_response({"nodes": [], "edges": []})

        # 2. Parallel crawl of ALL found nodes to discover connections
        async with aiohttp.ClientSession() as session:
            tasks = []
            peers_list = list(nodes)

            async def fetch_peers(node_url):
                try:
                    async with session.get(f"{node_url}/peers", timeout=1.0) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return node_url, data.get('peers', [])
                except:
                    pass
                return node_url, []

            # Launch all requests in parallel
            tasks = [fetch_peers(p) for p in peers_list]
            results = await asyncio.gather(*tasks)

            # 3. Build the graph
            for node, neighbors in results:
                for neighbor in neighbors:
                    neighbor = neighbor.rstrip('/')
                    if neighbor != node:
                        # If neighbor was not in list, add it (optional, but useful)
                        nodes.add(neighbor)
                        edges.append({"from": node, "to": neighbor})

        return web.json_response({
            "nodes": [{"id": n, "label": n.replace("http://", "").rstrip('/')} for n in nodes],
            "edges": edges
        })
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)
