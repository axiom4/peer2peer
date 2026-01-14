from core.merkle import MerkleTree
# from network.discovery import scan_network # UDP discovery logic needs review
import argparse
import os
import sys
import json
import asyncio
import hashlib
import random
import time
import concurrent.futures
from typing import List
from core.crypto import CryptoManager
from core.sharding import ShardManager
from core.metadata import MetadataManager
from core.distribution import DistributionStrategy
from network.node import LocalDirNode, StorageNode
from network.p2p_server import P2PServer
from network.remote_node import RemoteLibP2PNode
# from libp2p.host.host_interface import IHost
from libp2p.abc import IHost
from web_ui.server import start_web_server
from commands import (
    distribute_wrapper,
    reconstruct_wrapper,
    start_server_cmd  # Start server command is still local for main but maybe it should be moved?
)

# Global reference to loop for sync wrappers
_global_loop = None


def main():
    parser = argparse.ArgumentParser(
        description="Secure P2P Storage Tool (LibP2P)")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    server_parser = subparsers.add_parser(
        "start-node", help="Starts a storage server node")
    server_parser.add_argument(
        "--port", type=int, default=10000, help="Port to listen on")
    server_parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host")
    server_parser.add_argument(
        "--storage-dir", type=str, default="network_data/node_data", help="Data folder")
    server_parser.add_argument(
        "--join", type=str, help="MultiAddr of peer to join")

    dist_parser = subparsers.add_parser("distribute", help="Distribute file")
    dist_parser.add_argument("file", help="Input file")
    dist_parser.add_argument("--entry-node", help="MultiAddr of entry node")
    dist_parser.add_argument(
        "--local", action="store_true", help="Use local simulation")

    rec_parser = subparsers.add_parser("reconstruct", help="Reconstruct file")
    rec_parser.add_argument("manifest", help="Manifest file")
    rec_parser.add_argument("output", help="Output file")
    rec_parser.add_argument("--entry-node", help="MultiAddr of entry node")
    rec_parser.add_argument("--local", action="store_true",
                            help="Use local simulation")

    web_parser = subparsers.add_parser("web-ui", help="Start Web UI")
    web_parser.add_argument(
        "--port", type=int, default=8888, help="Port to listen on")
    web_parser.add_argument(
        "--storage-dir", type=str, default="network_data/web_ui_storage", help="Data folder for Web UI node")

    args = parser.parse_args()

    if args.command == "start-node":
        start_server_cmd(args)
    elif args.command == "distribute":
        asyncio.run(distribute_wrapper(args))
    elif args.command == "reconstruct":
        asyncio.run(reconstruct_wrapper(args))
    elif args.command == "web-ui":
        start_web_server(port=args.port, storage_dir=args.storage_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
