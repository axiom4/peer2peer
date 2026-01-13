import os
import json
import logging
import asyncio
import time
from typing import List, Optional

from libp2p import new_host
from libp2p.abc import IHost
from multiaddr import Multiaddr

from network.discovery import DiscoveryService
from network.p2p_protocol import P2PProtocol, CHUNK_PROTOCOL
from network.trio_bridge import TrioBridge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class P2PServer:
    def initialize_p2p_callbacks(self):
        # Catalog List Management
        self.catalog_file = os.path.join(
            self.storage_dir, "network_catalog.json")

        async def on_catalog_change(data, action="update"):
            # Load existing
            catalog = []
            if os.path.exists(self.catalog_file):
                try:
                    with open(self.catalog_file, 'r') as f:
                        catalog = json.load(f)
                except:
                    pass

            if action == "update":
                entry = data
                # Dedup
                existing_ids = {item.get('id') for item in catalog}
                if entry.get('id') not in existing_ids:
                    catalog.append(entry)
                    # Save
                    with open(self.catalog_file, 'w') as f:
                        json.dump(catalog, f)
                    logger.info(f"Catalog updated with {entry.get('name')}")

            elif action == "remove":
                manifest_id = data
                initial_len = len(catalog)
                catalog = [item for item in catalog if item.get(
                    'id') != manifest_id]
                if len(catalog) < initial_len:
                    with open(self.catalog_file, 'w') as f:
                        json.dump(catalog, f)
                    logger.info(f"Catalog removed entry {manifest_id}")

        async def get_catalog():
            # Combine network catalog with local manifests
            catalog = []
            if os.path.exists(self.catalog_file):
                try:
                    with open(self.catalog_file, 'r') as f:
                        catalog = json.load(f)
                except:
                    pass

            # Add local manifests
            import glob
            local_manifests = glob.glob("manifests/*.manifest")
            for f in local_manifests:
                try:
                    with open(f, 'r') as fp:
                        data = json.load(fp)
                        # Minimal metadata
                        meta = {
                            "id": data.get('id', 'local'),
                            "name": os.path.basename(f).replace('.manifest', ''),
                            "size": data.get('size', 0),
                            "chunks": len(data.get('chunks', [])),
                            "timestamp": data.get('timestamp', 0)
                        }
                        # Avoid dups
                        if not any(c.get('id') == meta['id'] for c in catalog):
                            catalog.append(meta)
                except:
                    pass

            return catalog

        return on_catalog_change, get_catalog

    def __init__(self, host_ip: str, port: int, storage_dir: str, known_peer: str = None):
        self.host_ip = host_ip
        self.port = port
        self.storage_dir = storage_dir
        self.known_peer_addr = known_peer
        self.peers = set()

        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

        self.bridge = TrioBridge()

        # Setup handlers
        cb_change, cb_get = self.initialize_p2p_callbacks()
        self.protocol_handler = P2PProtocol(
            self.storage_dir, cb_change, cb_get)

        self.discovery = DiscoveryService(self.port)
        self.discovery.set_storage_check(self._check_storage)

    async def initialize(self):
        """Initializes the host in the background Trio thread."""
        self.bridge.start()

        # Define the initialization logic to run in Trio
        async def _init_trio():
            import trio
            listen_addr = Multiaddr(f"/ip4/{self.host_ip}/tcp/{self.port}")
            # Pass listen_addrs to new_host so it configures transport (TCP)
            host = new_host(listen_addrs=[listen_addr])
            host.set_stream_handler(
                CHUNK_PROTOCOL, self.protocol_handler.handle_stream)
            peer_id = host.get_id().to_string()

            # Store host in bridge
            self.bridge.host = host

            # Start the host machinery (listeners, etc)
            # BasicHost.run(listen_addrs) is an async context manager that starts the services
            async def _run_host():
                try:
                    # Provide listeners here to actually bind and listen
                    async with host.run(listen_addrs=[listen_addr]):
                        await trio.sleep_forever()
                except Exception as e:
                    logger.error(f"Host run error: {e}")

            if self.bridge.nursery:
                self.bridge.nursery.start_soon(_run_host)
            else:
                logger.error("Bridge nursery not available!")

            return peer_id

        # Run initialization
        peer_id = await self.bridge.run_trio_task(_init_trio)

        self.discovery.set_peer_id(peer_id)
        logger.info(
            f"LibP2P Host initialized on /ip4/{self.host_ip}/tcp/{self.port}")
        logger.info(f"Peer ID: {peer_id}")

    @property
    def host(self):
        # Return the host object, but warn that it belongs to another thread
        return self.bridge.host

    async def get_active_peers(self):
        """Returns list of connected peers multiaddrs."""
        async def _get_peers():
            peers = []
            try:
                host = self.bridge.host
                if not host:
                    return []

                # Iterate over connected peer IDs
                for peer_id in host.get_network().connections:
                    # Get known addresses for this peer
                    addrs = host.get_peerstore().addrs(peer_id)
                    if addrs:
                        # Use the first address and encapsulate peer ID
                        # This creates /ip4/.../tcp/.../p2p/Qm...
                        maddr = addrs[0]
                        # Check if p2p protocol is already in maddr (it shouldn't be for transport addrs)
                        # but let's be safe
                        s = str(maddr)
                        if "/p2p/" not in s:
                            from multiaddr import Multiaddr
                            p2p_part = Multiaddr(f"/p2p/{peer_id.to_string()}")
                            maddr = maddr.encapsulate(p2p_part)
                        peers.append(str(maddr))
            except Exception as e:
                logger.error(f"Error getting active peers: {e}")
            return peers

        return await self.bridge.run_trio_task(_get_peers)

    async def start(self):
        if not self.bridge._running:
            await self.initialize()

        if self.known_peer_addr:
            await self.join_network(self.known_peer_addr)

        self.discovery.start()  # UDP

        asyncio.create_task(self._republish_catalog_loop())
        asyncio.create_task(self._sync_discovery())

        logger.info("P2P Server running...")

        # Keep alive loop (if needed by caller, otherwise assumes caller keeps loop)
        while True:
            await asyncio.sleep(3600)

    async def join_network(self, peer_maddr_str: str):
        try:
            from libp2p.peer.peerinfo import info_from_p2p_addr

            async def _join():
                maddr = Multiaddr(peer_maddr_str)
                info = info_from_p2p_addr(maddr)
                await self.bridge.host.connect(info)

            await self.bridge.run_trio_task(_join)

            self.peers.add(peer_maddr_str)
            logger.info(f"Joined network via {peer_maddr_str}")
        except Exception as e:
            logger.error(f"Failed to join network via {peer_maddr_str}: {e}")

    async def _sync_discovery(self):
        from libp2p.peer.peerinfo import info_from_p2p_addr

        while True:
            await asyncio.sleep(5)
            try:
                # Snapshot of discovered peers
                discovered = list(self.discovery.peers)
                if not discovered:
                    continue

                async def _sync_task():
                    host = self.bridge.host
                    if not host:
                        return

                    current_conns = host.get_network().connections

                    for p_str in discovered:
                        try:
                            # Parse multiaddr
                            maddr = Multiaddr(p_str)

                            # Extract PeerID
                            try:
                                info = info_from_p2p_addr(maddr)
                            except:
                                continue

                            # Skip self
                            if info.peer_id == host.get_id():
                                continue

                            # If not connected, connect
                            if info.peer_id not in current_conns:
                                # logger.info(f"Connecting to {p_str}...")
                                await host.connect(info)
                                logger.info(f"Auto-connected to {p_str}")
                        except Exception as ex:
                            # logging might be noisy
                            pass

                await self.bridge.run_trio_task(_sync_task)
            except Exception as e:
                logger.error(f"Sync discovery error: {e}")

    def _check_storage(self):
        import shutil
        total, used, free = shutil.disk_usage(self.storage_dir)
        return {"total": total, "used": used, "free": free}

    async def _republish_catalog_loop(self):
        while True:
            await asyncio.sleep(60)
            pass
