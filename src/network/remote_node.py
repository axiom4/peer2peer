import urllib.request
import urllib.error
import json
from typing import Optional, List
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

    def get_peers(self) -> List[str]:
        """
        Fetches the list of peers from this node.
        Returns a list of peer URLs.
        """
        try:
            url = f"{self.url}/peers"
            req = urllib.request.Request(url, method='GET')
            req.add_header('Connection', 'close')
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    data = json.loads(response.read())
                    return data.get('peers', [])
                return []
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError):
            # If we can't get peers due to network/parsing issues, return empty list
            return []

    def list_chunks(self) -> List[str]:
        """
        Fetches the list of all chunk IDs stored on this node.
        """
        try:
            url = f"{self.url}/chunks"
            req = urllib.request.Request(url, method='GET')
            req.add_header('Connection', 'close')
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    data = json.loads(response.read())
                    return data.get('chunks', [])
                return []
        except Exception:
            return []

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

    def check_exists(self, chunk_id: str) -> bool:
        """Checks if a chunk exists on this node (HEAD request)."""
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            req = urllib.request.Request(url, method='HEAD')
            req.add_header('Connection', 'close')
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            return False
        except Exception:
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
