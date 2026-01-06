import os


class LocalStorage:
    def __init__(self, storage_dir: str = "data_store"):
        self.storage_dir = storage_dir
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def save_chunk(self, chunk_id: str, data: bytes) -> str:
        """Saves a chunk to disk using its ID."""
        path = os.path.join(self.storage_dir, chunk_id)
        with open(path, 'wb') as f:
            f.write(data)
        return path

    def get_chunk(self, chunk_id: str) -> bytes:
        """Retrieves a chunk from disk."""
        path = os.path.join(self.storage_dir, chunk_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Chunk {chunk_id} not found locally.")

        with open(path, 'rb') as f:
            return f.read()
