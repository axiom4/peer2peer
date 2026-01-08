import os
import hashlib
import lzma
import zlib
import base64
from typing import List, Dict, Generator
from .crypto import CryptoManager

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks


class ShardManager:
    def __init__(self, key: bytes):
        self.crypto = CryptoManager(key)

    def process_file(self, file_path: str, compression=True) -> Generator[Dict[str, any], None, None]:
        """
        Reads a file, splits it into chunks, compresses (optional), encrypts chunks and yields data components.
        Yields dictionaries containing minimal metadata (hash) and the encrypted blob.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f:
            index = 0
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break

                # Compresses the chunk (Standard compression for speed/ratio balance)
                if compression:
                    # Switch to zlib (DEFLATE) for much faster performance than LZMA
                    # at the cost of slightly lower compression ratio.
                    processed_data = zlib.compress(data)
                else:
                    processed_data = data

                # Encrypts the compressed chunk (Fernet returns a B64 token)
                fernet_token = self.crypto.encrypt(processed_data)

                # BINARY PACKING: Decode B64 to save raw bytes (~33% savings)
                binary_data = base64.urlsafe_b64decode(fernet_token)

                # Calculate binary chunk hash (used as ID in storage)
                chunk_hash = hashlib.sha256(binary_data).hexdigest()

                chunk_info = {
                    "index": index,
                    "id": chunk_hash,
                    "data": binary_data,
                    "size": len(binary_data),
                    "original_size": len(data),
                    "total_file_size": file_size
                }
                yield chunk_info
                index += 1

    def reconstruct_file(self, chunks: List[Dict[str, any]], output_path: str, progress_cb=None):
        """
        Reconstructs the original file from encrypted chunks.
        Attempts decompression after decryption.
        Supports both old B64 and new binary formats.
        """
        # Sort by index for safety
        chunks.sort(key=lambda x: x['index'])
        total_chunks = len(chunks)

        with open(output_path, 'wb') as f:
            for i, chunk in enumerate(chunks):
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
                    # Attempt decompression (Auto-detect format)
                    # First try zlib (fast)
                    try:
                        data_to_write = zlib.decompress(decrypted_data)
                    except zlib.error:
                        # Fallback to lzma (legacy chunks)
                        data_to_write = lzma.decompress(decrypted_data)
                except lzma.LZMAError:
                    # Fallback for old uncompressed chunks (backward compatibility)
                    data_to_write = decrypted_data

                f.write(data_to_write)
                
                if progress_cb and i % 5 == 0:
                   progress_cb(i, total_chunks)

    def yield_reconstructed_chunks(self, chunks: List[Dict[str, any]]) -> Generator[bytes, None, None]:
        """
        Yields decrypted bytes of the file in order, without writing to disk.
        """
        chunks.sort(key=lambda x: x['index'])

        for chunk in chunks:
            raw_data = chunk['data']
            if raw_data and raw_data[0] == 0x80:
                token_to_decrypt = base64.urlsafe_b64encode(raw_data)
            else:
                token_to_decrypt = raw_data

            decrypted_data = self.crypto.decrypt(token_to_decrypt)
            try:
                try:
                    data = zlib.decompress(decrypted_data)
                except zlib.error:
                    data = lzma.decompress(decrypted_data)
            except lzma.LZMAError:
                data = decrypted_data

            yield data
