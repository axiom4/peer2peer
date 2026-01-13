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
    def __init__(self, http_port, peer_id=None):
        self.http_port = http_port
        self.peer_id = peer_id
        self.running = False
        self.peers = set()
        self.broadcast_sock = None
        self.listen_sock = None
        self.storage_check_cb = None

    def set_peer_id(self, peer_id: str):
        self.peer_id = peer_id

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
                msg_dict = {
                    "type": "HELLO",
                    "port": self.http_port
                }
                if self.peer_id:
                    msg_dict["peer_id"] = self.peer_id

                message = json.dumps(msg_dict).encode('utf-8')

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
                    remote_peer_id = message.get("peer_id")

                    if peer_port and peer_port != self.http_port:
                        # Construct Multiaddr
                        if remote_peer_id:
                            peer_url = f"/ip4/{peer_host}/tcp/{peer_port}/p2p/{remote_peer_id}"
                        else:
                            # Fallback (might fail later but better than nothing)
                            peer_url = f"/ip4/{peer_host}/tcp/{peer_port}"

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

                        # Reply with Multiaddr
                        my_ip = socket.gethostbyname(socket.gethostname())

                        my_url = f"/ip4/{my_ip}/tcp/{self.http_port}"
                        if self.peer_id:
                            my_url += f"/p2p/{self.peer_id}"

                        reply = json.dumps({
                            "type": "I_HAVE",
                            "chunk_id": chunk_id,
                            "url": my_url
                        }).encode('utf-8')

                        # We send reply from our broadcast socket (or listen socket, doesn't matter much for UDP)
                        self.listen_sock.sendto(
                            reply, (sender_ip, sender_port))

            except Exception as e:
                logger.debug(f"Discovery listen error: {e}")

    def get_discovered_peers(self):
        return list(self.peers)


def _scan_network_sync(timeout=3):
    """Utility for clients to find active nodes without being a full node. (Blocking Version)"""
    logger.info("Scanning local network for P2P nodes...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass

    sock.settimeout(timeout)

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', BROADCAST_PORT))
    except OSError:
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
                remote_port = int(message.get('port', 0))
                if remote_port <= 0:
                    continue

                remote_peer_id = message.get("peer_id")
                if remote_peer_id:
                    peer_url = f"/ip4/{addr[0]}/tcp/{remote_port}/p2p/{remote_peer_id}"
                else:
                    peer_url = f"/ip4/{addr[0]}/tcp/{remote_port}"
                found_peers.add(peer_url)
        except socket.timeout:
            break
        except Exception:
            continue

    sock.close()
    return list(found_peers)


async def scan_network(timeout=3):
    """Async wrapper for network scanning."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: _scan_network_sync(timeout))


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
                # We assume raw_url is a multiaddr like /ip4/x.x.x.x/tcp/port
                # We want to replace x.x.x.x with addr[0]
                url = raw_url
                try:
                    if raw_url.startswith("/ip4/"):
                        parts = raw_url.split("/")
                        # parts = ['', 'ip4', '1.2.3.4', 'tcp', '8080']
                        if len(parts) >= 5:
                            parts[2] = addr[0]
                            url = "/".join(parts)
                except:
                    pass

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
