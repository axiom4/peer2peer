import json
import logging
from typing import Callable
from libp2p.abc import INetStream
# from libp2p.network.stream.net_stream_interface import INetStream
from libp2p.custom_types import TProtocol
# from libp2p.typing import TProtocol

logger = logging.getLogger(__name__)

CHUNK_PROTOCOL = TProtocol("/peer2peer/chunk/1.0.0")

class P2PProtocol:
    def __init__(self, storage_dir: str, catalog_handler: Callable = None, catalog_provider: Callable = None):
        self.storage_dir = storage_dir
        self.catalog_handler = catalog_handler
        self.catalog_provider = catalog_provider

    async def handle_stream(self, stream: INetStream):
        """Standard handler for incoming chunk streams."""
        try:
            # Read header length (4 bytes)
            len_bytes = await stream.read(4)
            if not len_bytes:
                return
            header_len = int.from_bytes(len_bytes, 'big')
            
            # Read header
            header_bytes = await stream.read(header_len)
            header = json.loads(header_bytes.decode('utf-8'))
            
            msg_type = header.get('type')
            
            if msg_type == 'STORE':
                await self._handle_store(stream, header)
            elif msg_type == 'RETRIEVE':
                await self._handle_retrieve(stream, header)
            elif msg_type == 'CATALOG_UPDATE':
                await self._handle_catalog_update(stream, header)
            elif msg_type == 'CATALOG_REMOVE':
                await self._handle_catalog_remove(stream, header)
            elif msg_type == 'GET_CATALOG':
                await self._handle_get_catalog(stream, header)
            elif msg_type == 'CHECK':
                await self._handle_check(stream, header)
            
            await stream.close()
        except Exception as e:
            logger.error(f"Error handling P2P stream: {e}")
            try:
                await stream.reset()
            except:
                pass
    
    async def _handle_check(self, stream: INetStream, header: dict):
        chunk_id = header.get('chunk_id')
        import os
        path = os.path.join(self.storage_dir, chunk_id)
        exists = os.path.exists(path)
        await stream.write(b'1' if exists else b'0')

    async def _handle_catalog_update(self, stream: INetStream, header: dict):
        if self.catalog_handler:
            entry = header.get('entry')
            if entry:
                await self.catalog_handler(entry, action="update")
                logger.info(f"Catalog update processed")

    async def _handle_catalog_remove(self, stream: INetStream, header: dict):
        if self.catalog_handler:
            manifest_id = header.get('manifest_id')
            if manifest_id:
                await self.catalog_handler(manifest_id, action="remove")
                logger.info(f"Catalog removal processed for {manifest_id}")

    async def _handle_get_catalog(self, stream: INetStream, header: dict):
        if self.catalog_provider:
             catalog_data = await self.catalog_provider()
             response = json.dumps(catalog_data).encode('utf-8')
             # Write response as simple stream
             await stream.write(response)
        else:
             await stream.write(b'[]')



    async def _handle_store(self, stream: INetStream, header: dict):
        chunk_id = header['chunk_id']
        # Read the rest of the stream as data
        data = await stream.read()
        
        # Save to disk
        import os
        path = os.path.join(self.storage_dir, chunk_id)
        try:
            with open(path, 'wb') as f:
                f.write(data)
            logger.info(f"Accepted chunk {chunk_id} ({len(data)} bytes)")
        except IOError as e:
            logger.error(f"Failed to write chunk {chunk_id}: {e}")

    async def _handle_retrieve(self, stream: INetStream, header: dict):
        chunk_id = header['chunk_id']
        import os
        path = os.path.join(self.storage_dir, chunk_id)
        
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    data = f.read()
                
                # Write data in chunks to avoid buf size limit
                CHUNK_SIZE = 60000
                offset = 0
                while offset < len(data):
                    end = offset + CHUNK_SIZE
                    await stream.write(data[offset:end])
                    offset = end

                logger.info(f"Served chunk {chunk_id} ({len(data)} bytes)")
            except IOError as e:
                logger.error(f"Failed to read/send chunk {chunk_id}: {e}")
                # Maybe send an error header? Simpler to just close for now.
        else:
            logger.warning(f"Chunk {chunk_id} not found for retrieval")

