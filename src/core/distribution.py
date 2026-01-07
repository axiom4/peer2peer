import random
import concurrent.futures
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

        def _store_task(node):
            try:
                if node.store(chunk_id, data):
                    return node.get_id()
            except Exception as e:
                print(f"Error saving to {node.get_id()}: {e}")
            return None

        # Parallel upload to replicas
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(selected_nodes)) as executor:
            futures = [executor.submit(_store_task, n) for n in selected_nodes]
            for f in concurrent.futures.as_completed(futures):
                res = f.result()
                if res:
                    success_nodes.append(res)
                else:
                    # Log handled in task
                    pass

        if not success_nodes:
            raise RuntimeError(
                f"Unable to save chunk {chunk_id} on any node.")

        return success_nodes

    def retrieve_chunk(self, chunk_id: str) -> bytes:
        """
        Attempts to retrieve a chunk from the network.
        Queries the cluster nodes (Network Query).
        """
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
