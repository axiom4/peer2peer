import os
import json
import logging
import aiohttp
from aiohttp import web
import collections
import time
import random
import asyncio
import shutil
import sys
from network.discovery import DiscoveryService, udp_search_chunk_owners
from network.dht import DHT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class P2PServer:
    def __init__(self, host: str, port: int, storage_dir: str, known_peer: str = None):
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.peers = set()
        self.max_peers = 25  # Increased for DHT connectivity
        self.seen_requests = collections.deque(
            maxlen=50000)  # Loop prevention for DHT
        # Loop prevention for Chunk Deletion
        self.seen_reclaims = collections.deque(maxlen=50000)

        # Initialize DHT
        self.dht = DHT(host, port, storage_dir)

        # Automatic discovery with storage check capability
        self.discovery = DiscoveryService(self.port)

        # Link storage check to discovery service
        self.discovery.set_storage_check(self._check_storage)

        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

        # Increase body size limit to 10MB to allow upload of chunks > 1MB
        self.app = web.Application(client_max_size=10 * 1024 * 1024)
        self.app.add_routes([
            web.post('/join', self.handle_join),
            web.get('/peers', self.handle_get_peers),
            web.put('/chunk/{id}', self.handle_upload_chunk),
            web.delete('/chunk/{id}', self.handle_delete_chunk),
            # NEW: Batch delete endpoint
            web.post('/chunks/delete_batch', self.handle_delete_chunks_batch),
            web.get('/chunk/{id}', self.handle_download_chunk),
            # HEAD is handled implicitly by GET
            web.get('/chunks', self.handle_list_chunks),
            web.get('/status', self.handle_status),
            web.get('/openapi', self.handle_openapi),
            web.post('/unjoin', self.handle_unjoin),

            # DHT Routes
            web.get('/dht/retrieve', self.handle_dht_retrieve),
            web.post('/dht/ping', self.handle_dht_ping),
            web.post('/dht/find_node', self.handle_dht_find_node),
            web.post('/dht/find_value', self.handle_dht_find_value),
            web.post('/dht/store', self.handle_dht_store),
            web.post('/dht/delete', self.handle_dht_delete),
        ])

        self.known_peer = known_peer  # Url of a peer to join at startup

    async def handle_dht_retrieve(self, request):
        """
        GET /dht/retrieve?key=<key_hex>
        Simple read-only access to local DHT storage (no routing/ping).
        Used by Web UI to fetch Manifests.
        """
        try:
            key = request.query.get('key')
            if not key:
                return web.Response(status=400, text="Missing key")

            # Direct lookup in local storage
            # Note: We bypass dht.handle_find_value to avoid
            # polluting routing table with WebUI client info.
            raw_val = self.dht.storage.get(key)
            val = raw_val
            if isinstance(raw_val, dict) and 'v' in raw_val:
                val = raw_val['v']

            return web.json_response({"value": val})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_dht_ping(self, request):
        try:
            data = await request.json()
            resp = self.dht.handle_ping(data)
            return web.json_response(resp)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_dht_find_node(self, request):
        try:
            data = await request.json()
            resp = self.dht.handle_find_node(data['target_id'], data['sender'])
            return web.json_response(resp)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_dht_find_value(self, request):
        try:
            data = await request.json()
            resp = self.dht.handle_find_value(data['key'], data['sender'])
            return web.json_response(resp)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_dht_store(self, request):
        try:
            data = await request.json()
            resp = self.dht.handle_store(
                data['key'], data['value'], data['sender'])
            return web.json_response(resp)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_dht_delete(self, request):
        try:
            data = await request.json()
            # Value is optional for generic delete, but required for Catalog list item removal
            val = data.get('value', '')
            key = data['key']

            print(
                f"DHT DELETE REQUEST received for key={key[:8]}... val={val[:20]}...")

            # GOSSIP PROPAGATION
            # Create a unique ID for this delete request to dedup, include nonce if present
            nonce = data.get('nonce', '')
            import hashlib
            msg_id = hashlib.sha256(f"{key}{val}{nonce}".encode()).hexdigest()

            if msg_id in self.seen_requests:
                # Already processed, stop propagation
                # print(f"  -> Skipping duplicate delete request {msg_id[:8]}")
                return web.json_response({"status": "ok", "msg": "already_seen"})

            self.seen_requests.append(msg_id)

            # 1. Execute Local Delete
            resp = self.dht.handle_delete(key, val, data['sender'])
            print(f"  -> Local delete result: {resp}")

            # 2. Propagate to Peers (Gossip)
            # Fire and forget propagation to random 5 peers (or all if small)
            # This ensures network-wide cleanup
            peers_list = list(self.peers)
            if peers_list:
                targets = random.sample(peers_list, min(len(peers_list), 5))

                async def propagate(target_url):
                    try:
                        async with aiohttp.ClientSession() as session:
                            await session.post(
                                f"{target_url}/dht/delete",
                                json=data,  # Forward exact payload
                                timeout=1
                            )
                    except:
                        pass

                # Launch background tasks
                for p in targets:
                    asyncio.create_task(propagate(p))

            return web.json_response(resp)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    def _check_storage(self, chunk_id):
        return os.path.exists(os.path.join(self.storage_dir, chunk_id))

    async def handle_start_dht_bootstrap(self):
        # Bootstrap DHT with current peers
        peers_list = list(self.peers)
        if peers_list:
            logger.info(f"Bootstrapping DHT with {len(peers_list)} peers...")
            await self.dht.bootstrap(peers_list)

    async def _announce_all_chunks(self):
        # Give some time for neighbours to be discovered
        await asyncio.sleep(5)
        my_url = f"http://{self.host}:{self.port}"
        try:
            chunks = [f for f in os.listdir(
                self.storage_dir) if not f.startswith('.')]
            if chunks:
                logger.info(f"DHT: Announcing {len(chunks)} stored chunks...")
                for chunk_id in chunks:
                    asyncio.create_task(self.dht.put(chunk_id, my_url))
        except Exception as e:
            logger.error(f"Error announcing chunks: {e}")

    async def start(self):
        # Start UDP Discovery
        self.discovery.start()

        # Start DHT maintenance
        await self.dht.start()

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        logger.info(f"Starting P2P Node on {self.host}:{self.port}")
        await site.start()

        if self.known_peer:
            await self.join_network(self.known_peer)
            await self.dht.bootstrap([self.known_peer])
            logger.info(f"Bootstrapped DHT from known peer {self.known_peer}")

        # Announce local chunks to DHT
        # Start Periodic Re-announcement Loop instead of one-shot
        asyncio.create_task(self._announce_loop())

        # Start Catalog Re-Publish Loop (Self-Consistency)
        asyncio.create_task(self._republish_catalog_loop())

        # Periodic sync with discovery service
        asyncio.create_task(self._sync_discovery())

        # Keep alive
        while True:
            await asyncio.sleep(3600)

    async def _republish_catalog_loop(self):
        """
        Periodically scans local chunks to identify hidden Manifests (Catalog Mining)
        and re-publishes them to the public Catalog so they don't expire.
        """
        import hashlib

        # Hardcoded Key Logic matching main.py CatalogClient
        CATALOG_BASE_KEY = hashlib.sha256(b"catalog_global_v1").hexdigest()
        DHT_CATALOG_KEY = "catalog_" + CATALOG_BASE_KEY[:56]

        loop = asyncio.get_event_loop()

        while True:
            # Wait initial start time + interval (e.g. 1 minute first run)
            await asyncio.sleep(60)

            try:
                if not os.path.exists(self.storage_dir):
                    continue

                chunk_ids = [f for f in os.listdir(
                    self.storage_dir) if not f.startswith('.')]

                if not chunk_ids:
                    continue

                republished_count = 0

                # Scan all chunks
                for chunk_id in chunk_ids:
                    file_path = os.path.join(self.storage_dir, chunk_id)

                    try:
                        # Synchronous read in thread pool to avoid blocking async loop
                        def _read_candidate():
                            try:
                                # Optimization: Manifests are small (<75KB)
                                if os.path.getsize(file_path) > 75 * 1024:
                                    return None
                                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                    return f.read()
                            except:
                                return None

                        content = await loop.run_in_executor(None, _read_candidate)

                        if not content:
                            continue

                        # Quick heuristic check
                        if not (content.strip().startswith('{') and 'chunks' in content):
                            continue

                        # Parse JSON
                        try:
                            manifest = json.loads(content)
                            if isinstance(manifest, dict) and 'id' in manifest and 'chunks' in manifest:
                                # Valid Manifest Found!
                                entry = json.dumps({
                                    "id": manifest['id'],
                                    "name": manifest.get('filename', 'Unknown'),
                                    "size": manifest.get('filesize', 0),
                                    "ts": int(time.time())
                                })

                                # Publish to DHT
                                await self.dht.put(DHT_CATALOG_KEY, entry)
                                republished_count += 1
                        except:
                            pass

                    except Exception:
                        pass

                    # Yield to other tasks
                    await asyncio.sleep(0)

                if republished_count > 0:
                    logger.info(
                        f"Catalog Mining: Republished {republished_count} manifests found in raw storage.")

            except Exception as e:
                logger.error(f"Catalog republish error: {e}")

            # Sleep 5 minutes
            await asyncio.sleep(300)

    async def _announce_loop(self):
        """Periodically re-announces local chunks to refresh DHT TTL."""
        while True:
            await self._announce_all_chunks()
            # Interval < TTL (TTL=600s, Interval=300s is safe)
            await asyncio.sleep(300)

    async def _announce_all_chunks(self):
        # Give some time for neighbours to be discovered
        while True:
            await asyncio.sleep(3600)

    async def _sync_discovery(self):
        """Periodically adds peers discovered via UDP to the active peers list."""
        while True:
            # Sync DHT occasionally
            if len(self.peers) > 0 and random.random() < 0.2:  # 20% chance every loop
                await self.dht.bootstrap(list(self.peers))

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

        # Announce to DHT (Async Fire-and-Forget)
        my_url = f"http://{self.host}:{self.port}"
        asyncio.create_task(self.dht.put(chunk_id, my_url))

        logger.info(f"Stored chunk {chunk_id}")
        return web.Response(text="Stored")

    async def handle_delete_chunk(self, request):
        chunk_id = request.match_info['id']

        # --- Chunk Deletion Gossip Propagation ---
        # Identify message uniqueness to prevent loops
        if chunk_id in self.seen_reclaims:
            # If we already processed this delete recently, stop propagation
            # But we still check local file just in case
            pass
        else:
            self.seen_reclaims.append(chunk_id)
            # Propagate "Delete Chunk" command to peers
            peers_cl = list(self.peers)
            if peers_cl:
                # Random subset fanout (Gossip)
                targets = random.sample(peers_cl, min(len(peers_cl), 5))

                async def gossip_delete(target_node):
                    try:
                        async with aiohttp.ClientSession() as session:
                            await session.delete(f"{target_node}/chunk/{chunk_id}", timeout=1)
                    except:
                        pass

                for p in targets:
                    asyncio.create_task(gossip_delete(p))
        # ------------------------------------------

        path = os.path.join(self.storage_dir, chunk_id)
        if os.path.exists(path):
            try:
                os.remove(path)
                logger.info(f"Deleted chunk {chunk_id}")
                return web.Response(text="Deleted")
            except Exception as e:
                return web.Response(status=500, text=str(e))
        return web.Response(status=404, text="Chunk not found")

    async def handle_delete_chunks_batch(self, request):
        """
        Handles batch deletion of chunks and propagates via Gossip.
        Payload: { "chunk_ids": ["id1", "id2", ...] }
        """
        try:
            data = await request.json()
            chunk_ids = data.get("chunk_ids", [])
        except:
            return web.Response(status=400, text="Invalid JSON")

        if not chunk_ids:
            return web.Response(status=200, text="No chunks provided")

        # Filter new IDs to propagate
        new_ids_to_propagate = []
        deleted_count = 0

        for chunk_id in chunk_ids:
            # 1. Local Delete (always attempt, regardless of seen_reclaims status)
            path = os.path.join(self.storage_dir, chunk_id)
            if os.path.exists(path):
                try:
                    os.remove(path)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting chunk {chunk_id}: {e}")

            # 2. DHT Index Cleanup
            # Even if we don't have the file physically, we might have the index reference.
            # Or if we just deleted it, we definitely want to remove the index reference.
            if self.dht.delete_local(chunk_id):
                logger.info(
                    f"DHT: Cleaned up index for deleted chunk {chunk_id}")

            # 3. Check Loop Prevention
            if chunk_id in self.seen_reclaims:
                continue

            self.seen_reclaims.append(chunk_id)
            new_ids_to_propagate.append(chunk_id)

        logger.info(
            f"Batch Delete: Removed {deleted_count} local chunks. Propagating {len(new_ids_to_propagate)} IDs.")

        # GOSSIP PROPAGATION
        if new_ids_to_propagate:
            peers_cl = list(self.peers)
            if peers_cl:
                targets = random.sample(
                    peers_cl, min(len(peers_cl), 5))  # Fanout 5

                payload = {"chunk_ids": new_ids_to_propagate}

                async def propagate_batch(target_url):
                    try:
                        async with aiohttp.ClientSession() as session:
                            await session.post(
                                f"{target_url}/chunks/delete_batch",
                                json=payload,
                                timeout=2
                            )
                    except Exception:
                        pass

                for p in targets:
                    asyncio.create_task(propagate_batch(p))

        return web.json_response({
            "status": "ok",
            "processed": len(chunk_ids),
            "locally_deleted": deleted_count,
            "propagated": len(new_ids_to_propagate)
        })

    async def handle_check_chunk(self, request):
        """Responds only with 200/404 if it has the chunk, without body."""
        chunk_id = request.match_info['id']
        path = os.path.join(self.storage_dir, chunk_id)
        if os.path.exists(path):
            return web.Response(status=200)
        return web.Response(status=404)

    async def handle_download_chunk(self, request):
        """Searches for chunk locally. If not present, searches via DHT then UDP."""
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

        # 2a. DHT Search (New)
        logger.info(f"Chunk {chunk_id} missing. Querying DHT...")

        # DHT call is async, await directly
        try:
            owner_url = await self.dht.iterative_find_value(chunk_id)
        except Exception as e:
            logger.error(f"DHT Lookup error: {e}")
            owner_url = None

        owners = []
        if owner_url:
            logger.info(f"DHT Found owner for {chunk_id}: {owner_url}")
            owners.append(owner_url)
        else:
            logger.info("DHT lookup failed. Falling back to UDP Search...")

            # 2b. UDP Search (Fallback)
            results = await udp_search_chunk_owners([chunk_id], timeout=2.0)
            owners = results.get(chunk_id, [])

        if owners:
            logger.info(f"Chunk {chunk_id} located on: {owners}")
            # Try to download from any available owner
            async with aiohttp.ClientSession() as session:
                for owner in owners:
                    try:
                        # Direct HTTP download from the discovered active node
                        async with session.get(f"{owner}/chunk/{chunk_id}", timeout=5) as resp:
                            if resp.status == 200:
                                return web.Response(body=await resp.read())
                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch {chunk_id} from {owner}: {e}")
                        continue

        return web.Response(status=404, text="Chunk not found in cluster")

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

    async def handle_openapi(self, request):
        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": "P2P Node API",
                "version": "1.0.0"
            },
            "paths": {
                "/status": {
                    "get": {
                        "summary": "Get node status",
                        "responses": {
                            "200": {
                                "description": "Node status",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "host": {"type": "string"},
                                                "port": {"type": "integer"},
                                                "peers": {
                                                    "type": "array",
                                                    "items": {"type": "string"}
                                                },
                                                "chunks_count": {"type": "integer"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/chunks": {
                    "get": {
                        "summary": "List stored chunks",
                        "responses": {
                            "200": {
                                "description": "List of chunk IDs",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "chunks": {
                                                    "type": "array",
                                                    "items": {"type": "string"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/chunk/{id}": {
                    "get": {
                        "summary": "Download a chunk",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True,
                                "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "Chunk binary data"},
                            "404": {"description": "Chunk not found"}
                        }
                    },
                    "put": {
                        "summary": "Upload a chunk",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True,
                                "schema": {"type": "string"}}
                        ],
                        "requestBody": {
                            "content": {
                                "application/octet-stream": {
                                    "schema": {"type": "string", "format": "binary"}
                                }
                            }
                        },
                        "responses": {
                            "200": {"description": "Chunk uploaded"}
                        }
                    },
                    "delete": {
                        "summary": "Delete a chunk",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True,
                                "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "Chunk deleted"}
                        }
                    }
                },
                "/peers": {
                    "get": {
                        "summary": "Get known peers",
                        "responses": {
                            "200": {
                                "description": "List of peers",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "peers": {
                                                    "type": "array",
                                                    "items": {"type": "string"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/join": {
                    "post": {
                        "summary": "Join the network",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "host": {"type": "string"},
                                            "port": {"type": "integer"}
                                        },
                                        "required": ["host", "port"]
                                    }
                                }
                            }
                        },
                        "responses": {
                            "200": {"description": "Joined successfully"}
                        }
                    }
                },
                "/unjoin": {
                    "post": {
                        "summary": "Leave the network gracefully",
                        "description": "Transfers all data to other peers, deletes local storage, and shuts down.",
                        "responses": {
                            "200": {"description": "Unjoined successfully"}
                        }
                    }
                }
            }
        }
        return web.json_response(spec)

    async def handle_unjoin(self, request):
        logger.info("Unjoin requested. Starting graceful shutdown...")

        # 1. Identify chunks
        try:
            chunks = [f for f in os.listdir(
                self.storage_dir) if not f.startswith('.')]
        except FileNotFoundError:
            chunks = []

        if chunks:
            # 2. Find targets: use discovered peers
            candidates = list(self.peers)
            if not candidates:
                # Try to update peers from discovery service just in case
                self.peers.update(self.discovery.get_discovered_peers())
                candidates = list(self.peers)

            if not candidates:
                logger.warning(
                    "No peers found to transfer data to! Unjoin will cause data loss.")
            else:
                logger.info(
                    f"Transferring {len(chunks)} chunks to peers: {candidates}")
                async with aiohttp.ClientSession() as session:
                    for chunk_id in chunks:
                        file_path = os.path.join(self.storage_dir, chunk_id)
                        try:
                            # Read content
                            with open(file_path, 'rb') as f:
                                content = f.read()

                            # Try to upload to active peers
                            random.shuffle(candidates)
                            transferred = False
                            for peer in candidates:
                                try:
                                    # We use PUT /chunk/{id} which validates if chunk exists, but here we enforce write
                                    url = f"{peer}/chunk/{chunk_id}"
                                    async with session.put(url, data=content, timeout=5) as resp:
                                        if resp.status == 200:
                                            transferred = True
                                            logger.info(
                                                f"Offloaded {chunk_id} to {peer}")
                                            break
                                except Exception as e:
                                    logger.warning(
                                        f"Failed offload {chunk_id} to {peer}: {e}")

                            if not transferred:
                                logger.error(
                                    f"Failed to offload {chunk_id} to any peer.")

                        except Exception as e:
                            logger.error(
                                f"Error handling backup of {chunk_id}: {e}")

        # 3. Stop Discovery
        self.discovery.stop()

        # 4. Delete Storage
        try:
            shutil.rmtree(self.storage_dir)
            logger.info(f"Deleted storage directory: {self.storage_dir}")
        except Exception as e:
            logger.error(f"Failed to delete storage: {e}")

        # 5. Shutdown Server
        asyncio.create_task(self._shutdown_server())

        return web.json_response({"status": "Unjoined. Node is shutting down."})

    async def _shutdown_server(self):
        logger.info("Waiting 1s before process exit...")
        await asyncio.sleep(1)
        os._exit(0)
