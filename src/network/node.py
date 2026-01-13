import os
import shutil
import asyncio
from abc import ABC, abstractmethod


class StorageNode(ABC):
    @abstractmethod
    def store(self, chunk_id: str, data: bytes) -> bool:
        pass

    async def store_async(self, chunk_id: str, data: bytes) -> bool:
        """Default async implementation that wraps the sync method."""
        return self.store(chunk_id, data)

    @abstractmethod
    def retrieve(self, chunk_id: str) -> bytes:
        pass

    async def retrieve_async(self, chunk_id: str) -> bytes:
        """Default async implementation that wraps the sync method."""
        return self.retrieve(chunk_id)

    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def is_available(self) -> bool:
        pass


class LocalDirNode(StorageNode):
    """
    Simulates a remote node using a local directory.
    Useful for testing distribution and redundancy logic.
    """

    def __init__(self, node_id: str, base_path: str = "network_nodes"):
        self.node_id = node_id
        self.node_dir = os.path.join(base_path, node_id)
        if not os.path.exists(self.node_dir):
            os.makedirs(self.node_dir)
        self._online = True

    def get_id(self) -> str:
        return self.node_id

    def store(self, chunk_id: str, data: bytes) -> bool:
        if not self._online:
            return False
        path = os.path.join(self.node_dir, chunk_id)
        try:
            with open(path, 'wb') as f:
                f.write(data)
            return True
        except IOError:
            return False

    def retrieve(self, chunk_id: str) -> bytes:
        if not self._online:
            raise ConnectionError(f"Node {self.node_id} is offline")
        path = os.path.join(self.node_dir, chunk_id)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Chunk {chunk_id} not found on node {self.node_id}")
        with open(path, 'rb') as f:
            return f.read()

    def is_available(self) -> bool:
        return self._online

    def set_offline(self):
        """Simulates node crash."""
        self._online = False
