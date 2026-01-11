
import sys
import asyncio
import aiohttp
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from main import CatalogClient
from network.remote_node import RemoteHttpNode

async def force_remove_catalog_item(manifest_id):
    print(f"Force removing {manifest_id} from catalog...")
    
    # Construct targets manually (localhost 8000-8020)
    targets = [RemoteHttpNode(f"http://127.0.0.1:{p}") for p in range(8000, 8020)]
    
    # Also attempt standard LAN scan if possible, but let's trust localhost propagation for now
    # or just brute force the known CLI discovery result IPs
    extra_ips = [f"http://192.168.1.176:{p}" for p in range(8000, 8020)]
    for url in extra_ips:
        targets.append(RemoteHttpNode(url))

    cat = CatalogClient()
    await cat.delete(manifest_id, targets)
    print("Delete commands sent.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 force_remove.py <MANIFEST_ID>")
        sys.exit(1)
    
    mid = sys.argv[1]
    asyncio.run(force_remove_catalog_item(mid))
