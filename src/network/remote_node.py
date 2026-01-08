import requests
import json
from typing import Optional, List
from network.node import StorageNode


class RemoteHttpNode(StorageNode):
    """Client implementation of a node that speaks via HTTP with the P2P Server."""

    def __init__(self, url: str):
        self.url = url.rstrip('/')
        self.node_id = self.url  # Use URL as ID
        self._online = True  # Assume online, store/retrieve will verify
        self.session = requests.Session()
        # Tune connection pool
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=100)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

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
            resp = self.session.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('peers', [])
            return []
        except Exception:
            # If we can't get peers due to network/parsing issues, return empty list
            return []

    def list_chunks(self) -> List[str]:
        """
        Fetches the list of all chunk IDs stored on this node.
        """
        try:
            url = f"{self.url}/chunks"
            resp = self.session.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('chunks', [])
            return []
        except Exception:
            return []

    def store(self, chunk_id: str, data: bytes) -> bool:
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            resp = self.session.put(url, data=data, timeout=10)
            return resp.status_code == 200
        except Exception as e:
            # print(f"Node {self.url} error: {e}")
            return False

    def check_exists(self, chunk_id: str) -> bool:
        """Checks if a chunk exists on this node (HEAD request)."""
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            resp = self.session.head(url, timeout=2)
            return resp.status_code == 200
        except Exception:
            return False

    def retrieve(self, chunk_id: str) -> bytes:
        try:
            url = f"{self.url}/chunk/{chunk_id}"
            resp = self.session.get(url, timeout=10)
            
            if resp.status_code == 200:
                return resp.content
            elif resp.status_code == 404:
                raise FileNotFoundError(f"Chunk {chunk_id} not found on {self.url}")
            else:
                raise Exception(f"Status {resp.status_code}")
        except Exception as e:
            raise e
