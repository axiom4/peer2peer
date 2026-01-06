import socket
import threading
import json
import logging
import time
import asyncio

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

    sock.bind(('', BROADCAST_PORT))
    sock.settimeout(timeout)

    found_peers = set()
    start_time = time.time()

    while time.time() - start_time < timeout:
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
