import os
import json
import hashlib
import heapq
import time
import aiohttp
import logging
import asyncio
from typing import List, Tuple, Dict, Optional

logger = logging.getLogger(__name__)

K_BUCKET_SIZE = 20
ALPHA = 3
ID_BITS = 256


class NodeId:
    def __init__(self, id_bytes: bytes):
        if len(id_bytes) != ID_BITS // 8:
            raise ValueError(f"ID must be {ID_BITS//8} bytes")
        self.bytes = id_bytes
        self.int = int.from_bytes(id_bytes, 'big')

    @classmethod
    def from_hex(cls, hex_str: str):
        return cls(bytes.fromhex(hex_str))

    @classmethod
    def from_str(cls, s: str):
        return cls(hashlib.sha256(s.encode('utf-8')).digest())

    @classmethod
    def random(cls):
        import os
        return cls(os.urandom(ID_BITS // 8))

    def xor_distance(self, other: 'NodeId') -> int:
        return self.int ^ other.int

    def hex(self) -> str:
        return self.bytes.hex()

    def __eq__(self, other):
        return self.int == other.int

    def __hash__(self):
        return self.int

    def __str__(self):
        return self.hex()


class Peer:
    def __init__(self, id: NodeId, host: str, port: int):
        self.id = id
        self.host = host
        self.port = port
        self.last_seen = time.time()

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"

    def to_dict(self):
        return {
            "id": self.id.hex(),
            "host": self.host,
            "port": self.port
        }

    @classmethod
    def from_dict(cls, d):
        return cls(NodeId.from_hex(d["id"]), d["host"], d["port"])


class KBucket:
    def __init__(self, range_start: int, range_end: int):
        self.range_start = range_start
        self.range_end = range_end
        self.peers: List[Peer] = []

    def get_peer(self, peer_id: NodeId) -> Optional[Peer]:
        for p in self.peers:
            if p.id == peer_id:
                return p
        return None

    def update(self, peer: Peer):
        existing = self.get_peer(peer.id)
        if existing:
            self.peers.remove(existing)
            self.peers.append(peer)  # Move to tail (most recently seen)
        elif len(self.peers) < K_BUCKET_SIZE:
            self.peers.append(peer)
        else:
            # Bucket full. logic for eviction ping usually goes here.
            # Simplified: Drop earliest seen (head) or keep assuming alive?
            # Standard Kademlia: Ping head, if online drop new, if offline drop head
            # Here: simple replacement for simulation
            self.peers.pop(0)
            self.peers.append(peer)


class RoutingTable:
    def __init__(self, local_node_id: NodeId):
        self.local_node_id = local_node_id
        self.buckets: List[KBucket] = [KBucket(0, 2**ID_BITS)]

    def add_contact(self, peer: Peer):
        if peer.id == self.local_node_id:
            return

        # Find appropriate bucket
        bucket = self._find_bucket(peer.id)
        bucket.update(peer)

        # Split bucket if full and holds local node range
        # (Simplified Kademlia Logic: we just add for now, real splitting is complex)
        # For this prototype we will skip dynamic splitting and just use one big bucket
        # or a fixed number of buckets?
        # Actually proper Kademlia splitting is needed for O(logN)
        # But for 50 nodes simulation, a simple list is fine.
        # Let's keep KBucket simple: Standard Kademlia logic is complicated for a single file.
        # We will iterate all peers for K-closest for now (since N=50 is small).
        pass

    def _find_bucket(self, id: NodeId) -> KBucket:
        # Simplified: just return the first bucket
        return self.buckets[0]

    def find_k_closest(self, target_id: NodeId, k=K_BUCKET_SIZE) -> List[Peer]:
        # Collect all peers from all buckets
        all_peers = []
        for b in self.buckets:
            all_peers.extend(b.peers)

        # Sort by XOR distance
        all_peers.sort(key=lambda p: p.id.xor_distance(target_id))
        return all_peers[:k]


class DHT:
    def __init__(self, host: str, port: int, storage_dir: str):
        self.node_id = NodeId.from_str(f"{host}:{port}")
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.routing_table = RoutingTable(self.node_id)
        # chunk_id -> provider_url (Indexing)
        self.storage: Dict[str, str] = {}

        # Re-enable Disk Persistence but with TTL logic handled in logic
        self.db_path = os.path.join(storage_dir, "dht_index.json")
        self._load_db()

    def _load_db(self):
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, 'r') as f:
                    self.storage = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load DHT DB: {e}")

    def _save_db(self):
        try:
            with open(self.db_path, 'w') as f:
                json.dump(self.storage, f)
        except Exception as e:
            logger.error(f"Failed to save DHT DB: {e}")
        # Note: Local chunks are also implicit storage? No, this is the DHT index.

    async def bootstrap(self, peers: List[str]):
        """Join the network via list of peer URLs."""
        for url in peers:
            try:
                # Parse URL to get host/port
                # Assumes format http://host:port
                from urllib.parse import urlparse
                u = urlparse(url)
                # We need to ask them their ID.
                # Optimized: We assume we can PING them or FIND_NODE ourself.
                await self.ping(u.hostname, u.port)
            except Exception as e:
                logger.debug(f"Bootstrap fail for {url}: {e}")

    async def ping(self, host: str, port: int):
        try:
            url = f"http://{host}:{port}/dht/ping"
            payload = {
                "sender_id": self.node_id.hex(),
                "host": self.host,
                "port": self.port
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=2) as resp:
                    pass
            # If success, we add them? The response should validation.
        except:
            pass

    def handle_ping(self, sender_info: dict):
        sender_id = NodeId.from_hex(sender_info["sender_id"])
        peer = Peer(sender_id, sender_info["host"], sender_info["port"])
        self.routing_table.add_contact(peer)
        return {"id": self.node_id.hex()}

    def handle_find_node(self, target_id_hex: str, sender_info: dict):
        # Add sender
        self.handle_ping(sender_info)

        target_id = NodeId.from_hex(target_id_hex)
        closest = self.routing_table.find_k_closest(target_id)
        return {"nodes": [p.to_dict() for p in closest]}

    def handle_find_value(self, key_hex: str, sender_info: dict):
        self.handle_ping(sender_info)

        # TTL cleanup on read: Filter out expired items (older than 24h)
        # Only for catalog items which have timestamps
        if key_hex.startswith("catalog_") and key_hex in self.storage:
            items = self.storage[key_hex]
            if isinstance(items, list):
                valid_items = []
                now = time.time()
                for item in items:
                    try:
                        obj = json.loads(item)
                        # TTL: 24 hours (86400 seconds)
                        if now - obj.get('ts', 0) < 86400:
                            valid_items.append(item)
                    except:
                        # Keep untimestamped legacy items
                        valid_items.append(item)

                # Check for updates
                if len(valid_items) != len(items):
                    self.storage[key_hex] = valid_items
                    self._save_db()  # Sync cleanup to disk

        # If we have the value (location of chunk), return it
        if key_hex in self.storage:
            return {"value": self.storage[key_hex]}

        # Else return k closest nodes
        try:
            target_id = NodeId.from_hex(key_hex)
        except ValueError:
            # Handle special keys like "catalog_" by hashing them to get a valid ID
            target_id = NodeId.from_str(key_hex)

        closest = self.routing_table.find_k_closest(target_id)
        return {"nodes": [p.to_dict() for p in closest]}

    def handle_store(self, key_hex: str, value: str, sender_info: dict):
        self.handle_ping(sender_info)

        # CATALOG FEATURE: List Append
        # If the value is a specific JSON command to append to a list
        if key_hex.startswith("catalog_"):
            if key_hex not in self.storage:
                self.storage[key_hex] = []  # Initialize as list

            if isinstance(self.storage[key_hex], list):
                # Avoid duplicates
                if value not in self.storage[key_hex]:
                    self.storage[key_hex].append(value)
                    # Cap size to prevent abuse (e.g. 50 items per bucket)
                    if len(self.storage[key_hex]) > 50:
                        self.storage[key_hex].pop(0)
            self._save_db()
            return {"status": "ok"}

        self.storage[key_hex] = value
        self._save_db()
        return {"status": "ok"}

    def handle_delete(self, key_hex: str, value: str, sender_info: dict):
        self.handle_ping(sender_info)

        # CATALOG FEATURE: List Remove
        if key_hex.startswith("catalog_"):
            if key_hex in self.storage and isinstance(self.storage[key_hex], list):
                # Safe Parsing Helper
                def get_id_safe(json_str):
                    try:
                        obj = json.loads(json_str)
                        return obj.get('id') if isinstance(obj, dict) else None
                    except:
                        return None

                original_len = len(self.storage[key_hex])

                # Determine Target ID
                target_id = None
                try:
                    obj = json.loads(value)
                    if isinstance(obj, dict) and 'id' in obj:
                        target_id = obj['id']
                except:
                    # Treat value as the ID itself if not valid JSON
                    target_id = value

                print(
                    f"DHT DELETE REQUEST [{self.port}]: Key={key_hex[:8]}... TargetID={target_id}")

                if target_id:
                    # Filter: Keep items where ID does NOT match AND full string does NOT match
                    new_list = []
                    for item in self.storage[key_hex]:
                        item_id = get_id_safe(item)
                        # Logic: Remove if ID matches OR if exact string matches
                        if item_id == target_id:
                            print(
                                f"  -> REMOVED item by ID match: {target_id}")
                            continue
                        if item == value:
                            print(f"  -> REMOVED item by String match")
                            continue
                        # If we have an ID mismatch, let's log it just in case of doubt (optional)
                        # print(f"  -> Keeping item: {item_id} != {target_id}")
                        new_list.append(item)

                    self.storage[key_hex] = new_list
                else:
                    # Strict removal if no ID could be discerned
                    if value in self.storage[key_hex]:
                        self.storage[key_hex].remove(value)
                        print(f"  -> Removed exact value match")

                if len(self.storage[key_hex]) != original_len:
                    self._save_db()
                    print(
                        f"  -> Catalog updated. Count: {len(self.storage[key_hex])}")
                    return {"status": "ok", "removed": True}
                else:
                    print(f"  -> Item not found to remove.")

                return {"status": "not_found", "removed": False}

        # Standard Key Deletion
        if key_hex in self.storage:
            del self.storage[key_hex]
            self._save_db()
            return {"status": "ok"}

        return {"status": "not_found"}

    # Client Side Operations

    async def iterative_find_node(self, target_id: NodeId) -> List[Peer]:
        # Kademlia Lookup Algorithm
        shortlist = self.routing_table.find_k_closest(target_id, ALPHA)
        tried_ids = set()

        if not shortlist:
            return []

        # Simplified Iterative Lookup
        # In real impl: parallel queries, verify liveness, etc.
        # Here: sequential for simplicity

        closest_node = shortlist[0]

        async with aiohttp.ClientSession() as session:
            while True:
                # Pick alpha nodes from shortlist that haven't been tried
                candidates = [p for p in shortlist if p.id.hex()
                              not in tried_ids][:ALPHA]
                if not candidates:
                    break

                updated = False
                for peer in candidates:
                    tried_ids.add(peer.id.hex())
                    try:
                        url = f"{peer.url}/dht/find_node"
                        async with session.post(url, json={
                            "target_id": target_id.hex(),
                            "sender": self._my_info()
                        }, timeout=1) as resp:
                            data = await resp.json()
                            found_nodes = [Peer.from_dict(
                                d) for d in data["nodes"]]
                            for n in found_nodes:
                                if n.id.hex() not in [p.id.hex() for p in shortlist]:
                                    shortlist.append(n)
                                    updated = True
                    except:
                        pass

                shortlist.sort(key=lambda p: p.id.xor_distance(target_id))
                shortlist = shortlist[:K_BUCKET_SIZE]

                if not updated:
                    break

        return shortlist

    async def iterative_find_value(self, key: str) -> Optional[str]:
        target_id = NodeId.from_hex(key)
        shortlist = self.routing_table.find_k_closest(target_id, ALPHA)
        tried_ids = set()

        async with aiohttp.ClientSession() as session:
            while True:
                candidates = [p for p in shortlist if p.id.hex()
                              not in tried_ids][:ALPHA]
                if not candidates:
                    break

                for peer in candidates:
                    tried_ids.add(peer.id.hex())
                    try:
                        url = f"{peer.url}/dht/find_value"
                        async with session.post(url, json={
                            "key": key,
                            "sender": self._my_info()
                        }, timeout=1) as resp:
                            data = await resp.json()

                            if "value" in data:
                                return data["value"]

                            found_nodes = [Peer.from_dict(d)
                                           for d in data.get("nodes", [])]
                            for n in found_nodes:
                                if n.id.hex() not in [p.id.hex() for p in shortlist]:
                                    shortlist.append(n)

                            shortlist.sort(
                                key=lambda p: p.id.xor_distance(target_id))
                            shortlist = shortlist[:K_BUCKET_SIZE]

                    except:
                        pass
        return None

    async def put(self, key: str, value: str):
        """Announce that 'value' (url) has 'key' (chunk_id) to the K closely nodes."""
        target_id = NodeId.from_hex(key)
        nodes = await self.iterative_find_node(target_id)

        async with aiohttp.ClientSession() as session:
            for peer in nodes:
                try:
                    await session.post(f"{peer.url}/dht/store", json={
                        "key": key,
                        "value": value,
                        "sender": self._my_info()
                    }, timeout=1)
                except:
                    pass

    def _my_info(self):
        return {
            "sender_id": self.node_id.hex(),
            "host": self.host,
            "port": self.port
        }
