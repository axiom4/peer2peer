import os
import json
import logging
import aiohttp
from aiohttp import web
import collections
import time
import random
import asyncio
from network.discovery import DiscoveryService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class P2PServer:
    def __init__(self, host: str, port: int, storage_dir: str, known_peer: str = None):
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.peers = set()
        self.max_peers = 5  # Connection limit to form a sparse graph
        self.seen_requests = collections.deque(maxlen=1000)  # Loop prevention

        # Automatic discovery
        self.discovery = DiscoveryService(self.port)

        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

        # Increase body size limit to 10MB to allow upload of chunks > 1MB
        self.app = web.Application(client_max_size=10 * 1024 * 1024)
        self.app.add_routes([
            web.post('/join', self.handle_join),
            web.get('/peers', self.handle_get_peers),
            web.put('/chunk/{id}', self.handle_upload_chunk),
            web.delete('/chunk/{id}', self.handle_delete_chunk),
            web.get('/chunk/{id}', self.handle_download_chunk),
            # HEAD is handled implicitly by GET
            web.get('/chunks', self.handle_list_chunks),
            web.get('/status', self.handle_status)
        ])

        self.known_peer = known_peer  # Url of a peer to join at startup

    async def start(self):
        # Start UDP Discovery
        self.discovery.start()

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        logger.info(f"Starting P2P Node on {self.host}:{self.port}")
        await site.start()

        if self.known_peer:
            await self.join_network(self.known_peer)

        # Periodic sync with discovery service
        asyncio.create_task(self._sync_discovery())

        # Keep alive
        while True:
            await asyncio.sleep(3600)

    async def _sync_discovery(self):
        """Periodically adds peers discovered via UDP to the active peers list."""
        while True:
            # If I already have enough peers, don't actively search for new ones
            if len(self.peers) >= self.max_peers:
                await asyncio.sleep(10)
                continue

            discovered = self.discovery.get_discovered_peers()
            my_url = f"http://{self.host}:{self.port}"

            # Shuffle to avoid everyone choosing the same ones (e.g. lowest IPs)
            candidates = list(discovered)
            random.shuffle(candidates)

            for peer in candidates:
                if len(self.peers) >= self.max_peers:
                    break

                if peer != my_url and peer not in self.peers:
                    self.peers.add(peer)
                    logger.info(
                        f"Added peer {peer} via Discovery. Total: {len(self.peers)}")

            await asyncio.sleep(5)

    async def handle_join(self, request):
        """Another node wants to join us."""
        data = await request.json()
        new_node_url = data.get("url")

        if new_node_url and new_node_url not in self.peers:
            # Accept incoming connection even if full?
            # To create a connected graph it's better to accept, maybe replacing an old one
            # Or simply allowing inbound > outbound
            self.peers.add(new_node_url)
            # Trim if too many (FIFO or random)
            if len(self.peers) > self.max_peers + 2:  # Tolerance for in-bound
                removed = self.peers.pop()
                logger.info(f"Peer list full, dropped {removed}")

            logger.info(f"New peer joined: {new_node_url}")

        # Respond with list of my known peers (Gossip exchange)
        # Return only a subset to avoid saturating the requester
        return web.json_response({"peers": list(list(self.peers)[:5])})

    async def handle_get_peers(self, request):
        return web.json_response({"peers": list(self.peers)})

    async def handle_upload_chunk(self, request):
        """Saves a chunk locally."""
        chunk_id = request.match_info['id']
        data = await request.read()

        path = os.path.join(self.storage_dir, chunk_id)
        with open(path, 'wb') as f:
            f.write(data)

        logger.info(f"Stored chunk {chunk_id}")
        return web.Response(text="Stored")

    async def handle_delete_chunk(self, request):
        chunk_id = request.match_info['id']
        path = os.path.join(self.storage_dir, chunk_id)
        if os.path.exists(path):
            try:
                os.remove(path)
                logger.info(f"Deleted chunk {chunk_id}")
                return web.Response(text="Deleted")
            except Exception as e:
                return web.Response(status=500, text=str(e))
        return web.Response(status=404, text="Chunk not found")

    async def handle_check_chunk(self, request):
        """Responds only with 200/404 if it has the chunk, without body."""
        chunk_id = request.match_info['id']
        path = os.path.join(self.storage_dir, chunk_id)
        if os.path.exists(path):
            return web.Response(status=200)
        return web.Response(status=404)

    async def handle_download_chunk(self, request):
        """Searches for chunk locally. If not present, forwards request to peers (with TTL)."""
        chunk_id = request.match_info['id']
        path = os.path.join(self.storage_dir, chunk_id)

        # 1. Check Locally
        if os.path.exists(path):
            logger.info(f"Chunk {chunk_id} found locally.")
            if request.method == 'HEAD':
                return web.Response(status=200)
            with open(path, 'rb') as f:
                return web.Response(body=f.read())

        # For HEAD requests (mapping), do not forward request
        if request.method == 'HEAD':
            return web.Response(status=404)

        # 2. Forwarding Management (Path finding)
        request_id = request.headers.get(
            'X-Request-Id', f"{self.port}-{time.time()}-{random.randint(0, 1000)}")
        hops = int(request.headers.get('X-Hops', 3))  # Default 3 hop

        if request_id in self.seen_requests:
            return web.Response(status=404, text="Loop detected")
        self.seen_requests.append(request_id)

        if hops > 0:
            logger.info(
                f"Chunk {chunk_id} not found locally. Forwarding request (hops={hops})...")
            # Forward to all peers in parallel
            tasks = []
            async with aiohttp.ClientSession() as session:
                for peer in self.peers:
                    tasks.append(self._forward_request(
                        session, peer, chunk_id, request_id, hops - 1))

                # Wait for first success
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if isinstance(res, bytes) and res:
                        return web.Response(body=res)

        return web.Response(status=404, text="Chunk not found in network path")

    async def _forward_request(self, session, peer, chunk_id, request_id, hops):
        try:
            url = f"{peer}/chunk/{chunk_id}"
            headers = {'X-Request-Id': request_id, 'X-Hops': str(hops)}
            # Short timeout to avoid blocking
            async with session.get(url, headers=headers, timeout=2) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception:
            pass
        return None

    async def handle_list_chunks(self, request):
        """Returns a list of all chunk IDs stored on this node."""
        try:
            chunks = os.listdir(self.storage_dir)
            # Filter out hidden files or temps
            chunks = [c for c in chunks if not c.startswith('.')]
            return web.json_response({"chunks": chunks})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_status(self, request):
        return web.json_response({
            "host": self.host,
            "port": self.port,
            "peers": list(self.peers),
            "chunks_count": len(os.listdir(self.storage_dir))
        })
