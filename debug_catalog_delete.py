
import asyncio
import hashlib
import json
import time
import aiohttp
import sys

# Constants matching source code
CATALOG_KEY_BASE = "catalog_global_v1"
CATALOG_BUCKET_KEY = "catalog_" + \
    hashlib.sha256(CATALOG_KEY_BASE.encode()).hexdigest()[:56]
TARGET_ID = "038368e37f974fff5fc03f4585db2a46ccde6de18cc75c7ceea69751f7b0e763"

# Range of nodes to check
NODES = [f"http://127.0.0.1:{port}" for port in range(8000, 8010)]


async def inspect_catalog():
    print(f"Inspecting Catalog Bucket: {CATALOG_BUCKET_KEY}")
    async with aiohttp.ClientSession() as session:
        for node in NODES:
            try:
                # Find Value
                payload = {
                    "key": CATALOG_BUCKET_KEY,
                    "sender": {"sender_id": "debug", "host": "127.0.0.1", "port": 0}
                }
                async with session.post(f"{node}/dht/find_value", json=payload, timeout=1) as resp:
                    data = await resp.json()
                    if "value" in data:
                        items = data["value"]
                        print(f"[{node}] Catalog Items: {len(items)}")
                        found = False
                        for item in items:
                            try:
                                obj = json.loads(item)
                                if obj.get("id") == TARGET_ID:
                                    print(f"  -> FOUND TARGET: {item}")
                                    found = True
                            except:
                                if item == TARGET_ID:
                                    print(
                                        f"  -> FOUND TARGET (Raw String): {item}")
                                    found = True
                        if not found:
                            print(f"  -> Target NOT found.")
                    else:
                        print(f"[{node}] Catalog Key not found.")
            except Exception as e:
                print(f"[{node}] Connection Failed: {e}")


async def force_delete():
    print(f"\nForce Deleting Target ID: {TARGET_ID}")

    # Simulate the Payload sent by CatalogClient.delete
    # Payload 1: Remove from Catalog List
    nonce = str(time.time())
    payload_list = {
        "key": CATALOG_BUCKET_KEY,
        "value": TARGET_ID,  # The ID string itself
        "sender": {"sender_id": "debug_deleter", "host": "127.0.0.1", "port": 0},
        "nonce": nonce
    }

    async with aiohttp.ClientSession() as session:
        for node in NODES:
            try:
                url = f"{node}/dht/delete"
                async with session.post(url, json=payload_list, timeout=1) as resp:
                    res_json = await resp.json()
                    print(f"[{node}] Delete Response: {res_json}")
            except Exception as e:
                print(f"[{node}] Delete Req Failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "delete":
        asyncio.run(force_delete())

    asyncio.run(inspect_catalog())
