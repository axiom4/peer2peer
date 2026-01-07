import json
import os


class MetadataManager:
    def __init__(self, manifest_dir: str = "manifests"):
        self.manifest_dir = manifest_dir
        if not os.path.exists(self.manifest_dir):
            os.makedirs(self.manifest_dir)

    def save_manifest(self, filename: str, key: bytes, chunks_info: list):
        """Saves the file manifest. This file MUST remain private to the owner."""
        
        # Calculate total size if available in chunks metadata
        total_size = sum(c.get('original_size', 0) for c in chunks_info)

        manifest = {
            "filename": filename,
            "size": total_size,
            "key": key.decode('utf-8'),  # The key is needed to decrypt
            "chunks": [
                {
                    "index": c["index"],
                    "id": c["id"]
                    # Locations removed for anonymity/decentralization
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
