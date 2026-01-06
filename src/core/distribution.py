import random
from typing import List, Dict
from network.node import StorageNode


class DistributionStrategy:
    def __init__(self, nodes: List[StorageNode], redundancy_factor: int = 5):
        self.nodes = nodes
        self.redundancy_factor = redundancy_factor

    def distribute_chunk(self, chunk_id: str, data: bytes) -> List[str]:
        """
        Distributes a chunk to N nodes, where N is the redundancy factor.
        Returns the IDs of the nodes where the storage was successful.
        """
        available_nodes = [n for n in self.nodes if n.is_available()]

        if len(available_nodes) < self.redundancy_factor:
            raise RuntimeError(
                "Not enough available nodes to meet the required redundancy.")

        # Random node selection for uniform distribution
        # In a real system, a load or distance metric (DHT) would be used
        selected_nodes = random.sample(available_nodes, self.redundancy_factor)

        success_nodes = []
        for node in selected_nodes:
            if node.store(chunk_id, data):
                success_nodes.append(node.get_id())
            else:
                print(
                    f"Warning: Failed to save chunk {chunk_id} on node {node.get_id()}")

        if not success_nodes:
            raise RuntimeError(
                f"Unable to save chunk {chunk_id} on any node.")

        return success_nodes

    def retrieve_chunk(self, chunk_id: str, possible_locations: List[str] = None) -> bytes:
        """
        Attempts to retrieve a chunk from the network.
        If `possible_locations` is empty (as in the new design), queries the cluster nodes (Network Query).
        """
        if possible_locations:
            # Legacy mode: use locations if provided (for compatibility or optimization)
            candidates = []
            node_map = {n.get_id(): n for n in self.nodes}
            for loc in possible_locations:
                if loc in node_map:
                    candidates.append(node_map[loc])
        else:
            # Query Mode: Ask nodes we know
            candidates = [n for n in self.nodes if n.is_available()]
            random.shuffle(candidates)  # Load balancing and random walk start

        # Scan candidates
        for node in candidates:
            try:
                # For local nodes (simulation), retrieve() fails immediately if file is missing.
                # For remote nodes (P2PServer), retrieve() performs a GET that triggers internal server search.
                # print(f"Querying {node.get_id()} for {chunk_id[:8]}...")
                return node.retrieve(chunk_id)
            except Exception as e:
                # File not found on this node, try next
                # Silence log to reduce spam and use only print if debug active
                # print(f"Debug: Failed to retrieve from {node.get_id()}: {e}")

                # Short pause to let OS breathe (avoids 'Can't assign requested address' at high frequencies)
                import time
                time.sleep(0.05)
                continue

        raise RuntimeError(
            f"Chunk {chunk_id} not found on network (checked {len(candidates)} nodes/gateways).")
