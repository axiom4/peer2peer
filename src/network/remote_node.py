import json
import logging
from typing import List
from network.node import StorageNode
from network.p2p_protocol import CHUNK_PROTOCOL
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import Multiaddr
import asyncio
from network.trio_bridge import TrioBridge

logger = logging.getLogger(__name__)


class RemoteLibP2PNode(StorageNode):
    """
    Client implementation of a node that speaks via LibP2P.
    Replaces the old RemoteHttpNode.
    Bridges Asyncio calls to the Trio-based LibP2P Host.
    """

    def __init__(self, multiaddr_str: str, bridge: TrioBridge, loop: asyncio.AbstractEventLoop):
        self.multiaddr_str = multiaddr_str
        self.bridge = bridge
        self.loop = loop
        self.node_id = multiaddr_str
        self._online = True

        try:
            self.maddr = Multiaddr(multiaddr_str)
            self.peer_info = info_from_p2p_addr(self.maddr)
        except Exception as e:
            logger.error(f"Invalid multiaddr {multiaddr_str}: {e}")
            self._online = False

    def get_id(self) -> str:
        return self.node_id

    def is_available(self) -> bool:
        return self._online

    async def _ensure_connection(self):
        if not self._online:
            return False

        async def _connect_trio():
            # Run inside Trio thread
            host = self.bridge.host
            if not host:
                raise RuntimeError("Host not initialized in bridge")

            # Check existing connections safely in Trio
            if hasattr(host, 'get_network') and self.peer_info.peer_id in host.get_network().connections:
                return True

            try:
                await host.connect(self.peer_info)
                return True
            except Exception as e:
                logger.warning(
                    f"Failed to connect to {self.multiaddr_str}: {e}")
                return False

        try:
            return await self.bridge.run_trio_task(_connect_trio)
        except Exception as e:
            logger.error(f"Bridge error during connection check: {e}")
            return False

    async def store_async(self, chunk_id: str, data: bytes) -> bool:
        # We wrap the entire sequence in one trio task to keep the stream open in the same context

        async def _store_task():
            host = self.bridge.host
            # Ensure connected (re-check inside trio)
            try:
                if hasattr(host, 'get_network') and self.peer_info.peer_id not in host.get_network().connections:
                    await host.connect(self.peer_info)
            except:
                pass  # Try stream anyway or fail

            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])

                # Message format: HLEN(4) + HEADER(JSON) + DATA
                header = json.dumps(
                    {'type': 'STORE', 'chunk_id': chunk_id}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')

                # Write header first (small)
                await stream.write(header_len + header)

                # Write data in chunks to avoid 65535 bytes limit in quic/yamux/mplex
                CHUNK_SIZE = 60000
                offset = 0
                while offset < len(data):
                    end = offset + CHUNK_SIZE
                    await stream.write(data[offset:end])
                    offset = end

                await stream.close()
                return True
            except Exception as e:
                logger.error(f"Error storing on {self.node_id}: {e}")
                return False

        try:
            return await self.bridge.run_trio_task(_store_task)
        except Exception as e:
            logger.error(f"Store failed via bridge: {e}")
            return False

    async def check_availability_async(self, chunk_id: str) -> bool:
        async def _check_task():
            host = self.bridge.host
            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])
                header = json.dumps(
                    {'type': 'CHECK', 'chunk_id': chunk_id}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')
                await stream.write(header_len + header)

                resp = await stream.read(1)
                await stream.close()
                return resp == b'1'
            except Exception as e:
                # logger.error(f"Error checking {chunk_id} on {self.node_id}: {e}")
                return False

        try:
            return await self.bridge.run_trio_task(_check_task)
        except:
            return False

    async def update_catalog_async(self, entry: dict) -> bool:
        async def _update_task():
            host = self.bridge.host
            try:
                if hasattr(host, 'get_network') and self.peer_info.peer_id not in host.get_network().connections:
                    await host.connect(self.peer_info)
            except:
                pass

            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])
                header = json.dumps(
                    {'type': 'CATALOG_UPDATE', 'entry': entry}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')
                await stream.write(header_len + header)
                await stream.close()
                return True
            except Exception as e:
                logger.error(
                    f"Failed to update catalog on {self.node_id}: {e}")
                return False

        try:
            return await self.bridge.run_trio_task(_update_task)
        except:
            return False

    async def remove_catalog_async(self, manifest_id: str) -> bool:
        async def _remove_task():
            host = self.bridge.host
            try:
                if hasattr(host, 'get_network') and self.peer_info.peer_id not in host.get_network().connections:
                    await host.connect(self.peer_info)
            except:
                pass

            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])
                header = json.dumps(
                    {'type': 'CATALOG_REMOVE', 'manifest_id': manifest_id}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')
                await stream.write(header_len + header)
                await stream.close()
                return True
            except Exception as e:
                logger.error(
                    f"Failed to remove catalog entry on {self.node_id}: {e}")
                return False

        try:
            return await self.bridge.run_trio_task(_remove_task)
        except:
            return False

    async def fetch_catalog_async(self) -> List[dict]:
        async def _fetch_task():
            host = self.bridge.host
            try:
                if hasattr(host, 'get_network') and self.peer_info.peer_id not in host.get_network().connections:
                    await host.connect(self.peer_info)
            except:
                pass

            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])
                header = json.dumps({'type': 'GET_CATALOG'}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')
                await stream.write(header_len + header)

                # Read response
                data = await stream.read()  # Read all
                await stream.close()
                # Debug logging
                if len(data) > 0:
                    logger.info(
                        f"Received catalog data from {self.node_id}: {len(data)} bytes")
                else:
                    logger.warning(
                        f"Received empty catalog data from {self.node_id}")

                return json.loads(data.decode('utf-8'))
            except Exception as e:
                logger.error(f"Fetch catalog failed on {self.node_id}: {e}")
                return []
                logger.error(
                    f"Failed to fetch catalog from {self.node_id}: {e}")
                return []

        try:
            res = await self.bridge.run_trio_task(_fetch_task)
            return res if res else []
        except:
            return []

    async def retrieve_async(self, chunk_id: str) -> bytes:

        async def _retrieve_task():
            host = self.bridge.host
            try:
                if hasattr(host, 'get_network') and self.peer_info.peer_id not in host.get_network().connections:
                    await host.connect(self.peer_info)
            except:
                pass

            try:
                stream = await host.new_stream(self.peer_info.peer_id, [CHUNK_PROTOCOL])

                header = json.dumps(
                    {'type': 'RETRIEVE', 'chunk_id': chunk_id}).encode('utf-8')
                header_len = len(header).to_bytes(4, 'big')

                await stream.write(header_len + header)

                # Read all response data
                data = await stream.read()
                await stream.close()

                if not data:
                    raise FileNotFoundError(f"Chunk {chunk_id} not found")
                return data

            except Exception as e:
                raise e

        try:
            return await self.bridge.run_trio_task(_retrieve_task)
        except Exception as e:
            # Re-raise exceptions from the trio side
            raise e

    def store(self, chunk_id: str, data: bytes) -> bool:
        """Synchronous wrapper for store_async"""
        if not self.loop:
            return False

        future = asyncio.run_coroutine_threadsafe(
            self.store_async(chunk_id, data), self.loop)
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Sync store failed: {e}")
            return False

    def retrieve(self, chunk_id: str) -> bytes:
        """Synchronous wrapper for retrieve_async"""
        if not self.loop:
            raise RuntimeError("No loop available for async retrieve")

        future = asyncio.run_coroutine_threadsafe(
            self.retrieve_async(chunk_id), self.loop)
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Sync retrieve failed: {e}")
            return b""
