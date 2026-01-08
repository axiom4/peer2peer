import socket
import threading
import json
import logging
import time
import asyncio
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

BROADCAST_PORT = 9999
BEACON_INTERVAL = 1


class DiscoveryService:
    def __init__(self, http_port):
        self.http_port = http_port
        self.running = False
        self.peers = set()
        self.broadcast_sock = None
        self.listen_sock = None
        self.storage_check_cb = None

    def set_storage_check(self, callback):
        self.storage_check_cb = callback

    def start(self):
        self.running = True

        # Setup broadcasting
        self.broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Setup listening
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # MacOS/Linux specific reuse port if available
        try:
            self.listen_sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass

        self.listen_sock.bind(('', BROADCAST_PORT))

        # Start threads
        threading.Thread(target=self._broadcast_loop, daemon=True).start()
        threading.Thread(target=self._listen_loop, daemon=True).start()
        logger.info("Discovery Service started (UDP Broadcast)")

    def stop(self):
        self.running = False
        if self.broadcast_sock:
            self.broadcast_sock.close()
        if self.listen_sock:
            self.listen_sock.close()

    def _broadcast_loop(self):
        while self.running:
            try:
                # Announce my presence
                message = json.dumps({
                    "type": "HELLO",
                    "port": self.http_port
                }).encode('utf-8')

                self.broadcast_sock.sendto(
                    message, ('<broadcast>', BROADCAST_PORT))
            except Exception as e:
                logger.debug(f"Broadcast error: {e}")

            time.sleep(BEACON_INTERVAL)

    def _listen_loop(self):
        while self.running:
            try:
                data, addr = self.listen_sock.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))

                if message.get("type") == "HELLO":
                    peer_host = addr[0]  # IP of the sender
                    peer_port = message.get("port")

                    if peer_port and peer_port != self.http_port:
                        peer_url = f"http://{peer_host}:{peer_port}"
                        if peer_url not in self.peers:
                            logger.info(f"Discovered peer via UDP: {peer_url}")
                            self.peers.add(peer_url)
                            # Here we could trigger a callback to the P2PServer to add this peer

                elif message.get("type") == "QUERY_CHUNK":
                    # Another node is asking who has a chunk
                    # Listen for questions on broadcast
                    chunk_id = message.get("chunk_id")
                    if chunk_id and self.storage_check_cb and self.storage_check_cb(chunk_id):
                        # I have it! Reply directly to sender (unicast)
                        sender_ip = addr[0]
                        # This is the source port of the UDP packet
                        sender_port = addr[1]

                        reply = json.dumps({
                            "type": "I_HAVE",
                            "chunk_id": chunk_id,
                            "url": f"http://{socket.gethostbyname(socket.gethostname())}:{self.http_port}"
                        }).encode('utf-8')

                        # We send reply from our broadcast socket (or listen socket, doesn't matter much for UDP)
                        self.listen_sock.sendto(
                            reply, (sender_ip, sender_port))

            except Exception as e:
                logger.debug(f"Discovery listen error: {e}")

    def get_discovered_peers(self):
        return list(self.peers)


async def scan_network(timeout=3):
    """Utility for clients to find active nodes without being a full node."""
    logger.info("Scanning local network for P2P nodes...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass

    # For scanning, we listen to HELLO broadcasts. We MUST bind to BROADCAST_PORT.
    # If the local P2P server is running, we rely on SO_REUSEPORT.

    sock.settimeout(timeout)

    # Force reuse port again
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', BROADCAST_PORT))
    except OSError:

        # If we can't bind 9999, we can try to send a query and wait for reply?
        # But HELLO is unsolicited.
        # Fallback: Just return empty or try to bind random and maybe some broadcast goes there? No.
        logger.warning(
            "Could not bind to broadcast port for scan. Scanning might fail if port 9999 is taken.")
        sock.close()
        return []

    found_peers = set()
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        remaining = timeout - elapsed
        if remaining <= 0:
            break

        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(1024)
            message = json.loads(data.decode('utf-8'))
            if message.get("type") == "HELLO":
                peer_url = f"http://{addr[0]}:{message['port']}"
                found_peers.add(peer_url)
        except socket.timeout:
            break
        except Exception:
            continue

    sock.close()
    return list(found_peers)


def _udp_search_sync(chunk_ids, timeout):
    """Sync implementation of UDP search."""
    if not chunk_ids:
        return {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)
    try:
        sock.bind(('', 0))
    except Exception:
        sock.close()
        return {cid: [] for cid in chunk_ids}

    # Send Queries
    for cid in chunk_ids:
        msg = json.dumps(
            {"type": "QUERY_CHUNK", "chunk_id": cid}).encode('utf-8')
        try:
            sock.sendto(msg, ('<broadcast>', BROADCAST_PORT))
        except Exception:
            pass
        try:
            sock.sendto(msg, ('255.255.255.255', BROADCAST_PORT))
        except Exception:
            pass

    # Collect Replies
    results = {cid: set() for cid in chunk_ids}

    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        remaining = timeout - elapsed
        if remaining <= 0:
            break

        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(4096)
            msg = json.loads(data.decode('utf-8'))

            if msg.get("type") == "I_HAVE":
                cid = msg.get("chunk_id")
                raw_url = msg.get("url")

                # Normalize URL to use the observed IP address (addr[0])
                # This ensures consistency with scan_network which uses addr[0]
                # preventing mismatch between "candidates" and "live_locations"
                try:
                    parsed = urlparse(raw_url)
                    if parsed.port:
                        url = f"http://{addr[0]}:{parsed.port}"
                    else:
                        url = raw_url
                except:
                    url = raw_url

                if cid in results:
                    results[cid].add(url)

        except socket.timeout:
            break
        except Exception:
            pass

    sock.close()

    # Convert sets to lists
    return {k: list(v) for k, v in results.items()}


async def udp_search_chunk_owners(chunk_ids, timeout=2):
    """
    Broadcasts a query for chunks and collects replies via UDP.
    Returns: dict { chunk_id: [node_url, ...] }
    NON-BLOCKING wrapper.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _udp_search_sync, chunk_ids, timeout)
