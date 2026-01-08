import json
import os
from .merkle import MerkleTree


class MetadataManager:
    def __init__(self, manifest_dir: str = "manifests"):
        self.manifest_dir = manifest_dir
        if not os.path.exists(self.manifest_dir):
            os.makedirs(self.manifest_dir)

    def save_manifest(self, filename: str, key: bytes, chunks_info: list, compression: bool = True):
        """Saves the file manifest. This file MUST remain private to the owner."""

        # Ensure chunks are sorted by index for consistent Merkle Root
        chunks_info.sort(key=lambda x: x["index"])

        # Calculate Merkle Root
        chunk_ids = [c["id"] for c in chunks_info]
        merkle_tree = MerkleTree(chunk_ids)
        merkle_root = merkle_tree.get_root()

        # Calculate total size if available in chunks metadata
        total_size = sum(c.get('original_size', 0) for c in chunks_info)

        manifest = {
            "filename": filename,
            "merkle_root": merkle_root,
            "size": total_size,
            "key": key.decode('utf-8'),  # The key is needed to decrypt
            "compression": compression,  # Algorithm info
            "chunks": [
                {
                    "index": c["index"],
                    "id": c["id"]
                    # "locations" removed for privacy/distributed logic
                } for c in chunks_info
            ]
        }

        manifest_path = os.path.join(self.manifest_dir, f"{filename}.manifest")
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=4)
        return manifest_path

    def load_manifest(self, manifest_path: str) -> dict:
        with open(manifest_path, 'r') as f:
            return json.load(f)

    def update_manifest_chunks(self, filename: str, chunks: list):
        """Updates just the chunks section of an existing manifest (e.g. after repair)."""
        manifest_path = os.path.join(self.manifest_dir, f"{filename}.manifest")
        if not os.path.exists(manifest_path):
            return

        with open(manifest_path, 'r') as f:
            data = json.load(f)

        # Update chunks
        data['chunks'] = chunks

        # Helper function to get chunk ID safely
        def get_chunk_id(c):
            # Handle both dict and object if necessary, assuming dict for now based on usage
            return c.get('id') if isinstance(c, dict) else c.id

        # Update Merkle Root
        # Ensure chunks are sorted by index
        chunks.sort(key=lambda x: x.get('index', 0))
        chunk_ids = [get_chunk_id(c) for c in chunks]
        merkle_tree = MerkleTree(chunk_ids)
        data['merkle_root'] = merkle_tree.get_root()

        with open(manifest_path, 'w') as f:
            json.dump(data, f, indent=4)
