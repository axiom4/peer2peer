import asyncio
import aiohttp
import os
import random
from typing import List, Dict, Set
from core.metadata import MetadataManager
from core.distribution import DistributionStrategy
from network.remote_node import RemoteHttpNode
from network.discovery import scan_network


class RepairManager:
    def __init__(self, manifest_manager: MetadataManager):
        self.meta_mgr = manifest_manager

    async def check_node_health(self, node_url: str, session: aiohttp.ClientSession) -> bool:
        try:
            # Check basic connectivity
            async with session.get(f"{node_url.rstrip('/')}/status", timeout=2) as resp:
                return resp.status == 200
        except:
            return False

    async def repair_manifest(self, manifest_name: str, redundancy_target: int = 5, progress_cb=None):
        """
        Analyzes and repairs a file manifest.
        1. Checks all chunks locations.
        2. Identifies dead nodes.
        3. Replicates chunks to new nodes if redundancy < target.
        """
        if not manifest_name.endswith('.manifest'):
            manifest_name += '.manifest'

        manifest_path = os.path.join(self.meta_mgr.manifest_dir, manifest_name)

        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Manifest {manifest_name} not found")

        manifest = self.meta_mgr.load_manifest(manifest_path)
        chunks = manifest.get('chunks', [])

        # 1. Discover active network
        if progress_cb:
            progress_cb(5, "Scanning network for active nodes...")
        found_urls = await scan_network(timeout=2)
        active_nodes_urls = set(u.rstrip('/') for u in found_urls)

        if not active_nodes_urls:
            if progress_cb:
                progress_cb(-1, "No active nodes found in network!")
            return

        loop = asyncio.get_running_loop()

        # ------------------------------------------------------------------
        # NEW LOGIC: Ignore manifest locations, Scan network for chunks
        # ------------------------------------------------------------------

        if progress_cb:
            progress_cb(10, "Surveying network for chunks (HEAD check)...")

        active_nodes_objects = [RemoteHttpNode(u) for u in active_nodes_urls]

        # Map: chunk_id -> list of nodes that have it
        chunk_locations_map = {c['id']: [] for c in chunks}

        async def check_chunk_on_node(node_obj, c_id):
            exists = await loop.run_in_executor(None, node_obj.check_exists, c_id)
            if exists:
                return c_id, node_obj.get_id()
            return None

        # Create all survey tasks
        survey_tasks = []
        for c in chunks:
            for node in active_nodes_objects:
                survey_tasks.append(check_chunk_on_node(node, c['id']))

        results = await asyncio.gather(*survey_tasks)

        for res in results:
            if res:
                c_id, node_id = res
                chunk_locations_map[c_id].append(node_id)

        # 2. Repair Loop
        repaired_chunks_count = 0
        total_chunks = len(chunks)
        updates_performed = False

        for i, chunk in enumerate(chunks):
            chunk_id = chunk['id']
            # Locations discovered dynamically
            live_locations = chunk_locations_map.get(chunk_id, [])

            # Remove locations key if it existed (migration cleanup)
            if 'locations' in chunk:
                del chunk['locations']

            current_redundancy = len(live_locations)

            if current_redundancy < redundancy_target:
                updates_performed = True
                needed = redundancy_target - current_redundancy

                msg = f"Repairing Chunk {i} ({current_redundancy}/{redundancy_target} replicas)..."
                if progress_cb:
                    progress_cb(10 + int((i/total_chunks)*80), msg)

                # Retrieve content
                content = None
                try:
                    sources = [RemoteHttpNode(u) for u in live_locations]

                    if not sources:
                        # Data Loss detected or just not found on active nodes
                        # Try to use DistributionStrategy's retrieve which does a global query
                        # (Although we just surveyed all active nodes via HEAD, maybe GET works if forwarded?)
                        # But for now assume HEAD survey is authoritative for active nodes.
                        print(
                            f"CRITICAL: Chunk {i} lost completely (or nodes offline).")
                        continue

                    retriever = DistributionStrategy(sources)
                    content = await loop.run_in_executor(None, retriever.retrieve_chunk, chunk_id)
                except Exception as e:
                    print(
                        f"Failed to retrieve chunk {chunk_id} for repair: {e}")
                    continue

                # Upload to NEW nodes
                candidates = [
                    n for n in active_nodes_objects if n.get_id() not in live_locations]

                if not candidates:
                    continue

                if len(candidates) < needed:
                    needed = len(candidates)

                if needed > 0:
                    selected_targets = random.sample(candidates, needed)

                    # Manual store
                    for node in selected_targets:
                        try:
                            # Run synchronously blocking call in executor
                            success = await loop.run_in_executor(None, node.store, chunk_id, content)
                            if success:
                                repaired_chunks_count += 1
                        except:
                            pass

        if updates_performed or any('locations' in c for c in chunks):
            # Save to strip locations and persist any other metadata changes if any
            self.meta_mgr.update_manifest_chunks(manifest['filename'], chunks)
            msg = f"Repair completed. Repaired {repaired_chunks_count} chunks."
            if progress_cb:
                progress_cb(100, msg)
            return True
        else:
            if progress_cb:
                progress_cb(100, "Network healthy. No repairs needed.")
            return False
