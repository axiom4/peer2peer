import os
import hashlib
import lzma
import base64
from typing import List, Dict, Generator
from .crypto import CryptoManager

CHUNK_SIZE = 1024 * 1024  # 1MB chunks


class ShardManager:
    def __init__(self, key: bytes):
        self.crypto = CryptoManager(key)

    def process_file(self, file_path: str) -> List[Dict[str, any]]:
        """
        Reads a file, splits it into chunks, compresses, encrypts chunks and returns data ready for distribution.
        Returns a list of dictionaries containing minimal metadata (hash) and the encrypted blob.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        chunks_metadata = []

        with open(file_path, 'rb') as f:
            index = 0
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break

                # Compresses the chunk (Standard compression for speed/ratio balance)
                compressed_data = lzma.compress(data)

                # Encrypts the compressed chunk (Fernet returns a B64 token)
                fernet_token = self.crypto.encrypt(compressed_data)

                # BINARY PACKING: Decode B64 to save raw bytes (~33% savings)
                binary_data = base64.urlsafe_b64decode(fernet_token)

                # Calculate binary chunk hash (used as ID in storage)
                chunk_hash = hashlib.sha256(binary_data).hexdigest()

                chunk_info = {
                    "index": index,
                    "id": chunk_hash,
                    "data": binary_data,
                    "size": len(binary_data),
                    "original_size": len(data)  # Optional: for statistics
                }
                chunks_metadata.append(chunk_info)
                index += 1

        return chunks_metadata

    def reconstruct_file(self, chunks: List[Dict[str, any]], output_path: str):
        """
        Reconstructs the original file from encrypted chunks.
        Attempts decompression after decryption.
        Supports both old B64 and new binary formats.
        """
        # Sort by index for safety
        chunks.sort(key=lambda x: x['index'])

        with open(output_path, 'wb') as f:
            for chunk in chunks:
                raw_data = chunk['data']

                # Format detection: Fernet Binary starts with 0x80, Fernet B64 starts with 'g' (103)
                if raw_data and raw_data[0] == 0x80:
                    # Optimized Binary Format -> Encodes back to B64 for Fernet
                    token_to_decrypt = base64.urlsafe_b64encode(raw_data)
                else:
                    # Legacy B64
                    token_to_decrypt = raw_data

                decrypted_data = self.crypto.decrypt(token_to_decrypt)

                try:
                    # Attempt decompression
                    data_to_write = lzma.decompress(decrypted_data)
                except lzma.LZMAError:
                    # Fallback for old uncompressed chunks (backward compatibility)
                    data_to_write = decrypted_data

                f.write(data_to_write)
