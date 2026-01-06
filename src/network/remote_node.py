import urllib.request
import urllib.error
from typing import Optional
from network.node import StorageNode


class RemoteHttpNode(StorageNode):
    """Client implementation of a node that speaks via HTTP with the P2P Server."""

    def __init__(self, url: str):
        self.url = url.rstrip('/')
        self.node_id = self.url  # Use URL as ID
        self._online = True  # Assume online, store/retrieve will verify

    def get_id(self) -> str:
        return self.node_id

    def is_available(self) -> bool:
        return self._online

    def store(self, chunk_id: str, data: bytes) -> bool:
        # Use standard urllib to avoid issues with nested asyncio loops and overhead
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            req = urllib.request.Request(url, data=data, method='PUT')
            # Close connection explicitly
            req.add_header('Connection', 'close')
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status == 200
        except Exception as e:
            # print(f"Node {self.url} error: {e}")
            return False

    def retrieve(self, chunk_id: str) -> bytes:
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            # Close connection explicitly to avoid socket exhaustion
            req = urllib.request.Request(url, method='GET')
            req.add_header('Connection', 'close')

            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    return response.read()
                else:
                    raise Exception(f"Status {response.status}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(
                    f"Chunk {chunk_id} not found on {self.url}")
            raise e
        except Exception as e:
            raise e
