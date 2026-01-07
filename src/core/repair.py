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

        loop = asyncio.get_running_loop()

        # Determine actual health of "known" locations in manifest
        all_manifest_locations = set()
        for c in chunks:
            for loc in c.get('locations', []):
                all_manifest_locations.add(loc.rstrip('/'))

        # Check health of ALL locations (discovered + listed)
        health_map = {}
        if progress_cb:
            progress_cb(10, "Checking node health...")

        async with aiohttp.ClientSession() as session:
            check_tasks = []
            distinct_hosts = active_nodes_urls.union(all_manifest_locations)
            distinct_hosts_list = list(distinct_hosts)  # indexing

            for url in distinct_hosts_list:
                check_tasks.append(self.check_node_health(url, session))

            if check_tasks:
                results = await asyncio.gather(*check_tasks)
                for i, is_up in enumerate(results):
                    health_map[distinct_hosts_list[i]] = is_up

        active_hosts = [url for url, is_up in health_map.items() if is_up]
        if not active_hosts:
            if progress_cb:
                progress_cb(-1, "No active nodes found in network!")
            return

        # 2. Repair Loop
        updates_needed = False
        total_chunks = len(chunks)

        # Helper for retrieval/storage
        # We need actual Node objects
        active_nodes_objects = [RemoteHttpNode(u) for u in active_hosts]

        # Distribution Strategy for retrieval uses ALL active nodes (to find content)
        # But for storage we select specific targets

        repaired_chunks_count = 0

        for i, chunk in enumerate(chunks):
            chunk_id = chunk['id']
            locations = chunk.get('locations', [])

            # Filter live/dead
            live_locations = [
                loc for loc in locations if health_map.get(loc.rstrip('/'), False)]

            # If a location has changed URL/IP it's tricky, but we assume static URLs/IDs for now.

            current_redundancy = len(live_locations)

            if current_redundancy < redundancy_target:
                updates_needed = True
                needed = redundancy_target - current_redundancy

                msg = f"Repairing Chunk {i} ({current_redundancy}/{redundancy_target} replicas)..."
                if progress_cb:
                    progress_cb(10 + int((i/total_chunks)*80), msg)

                # Retrieve content
                content = None
                try:
                    # Try to get from remaining live locations first
                    sources = [RemoteHttpNode(u) for u in live_locations]
                    if not sources:
                        # Desperation: try all active nodes, maybe one has it but wasn't in manifest
                        sources = active_nodes_objects

                    if not sources:
                        # Data Loss detected
                        print(f"CRITICAL: Chunk {i} lost completely.")
                        chunk['locations'] = []  # Data lost
                        continue

                    retriever = DistributionStrategy(sources)
                    content = await loop.run_in_executor(None, retriever.retrieve_chunk, chunk_id)
                except Exception as e:
                    print(
                        f"Failed to retrieve chunk {chunk_id} for repair: {e}")
                    # Keep old locations in hope they come back alive?
                    # Or update to only live ones?
                    # If we update to live ones, we admit data loss.
                    # If we keep dead ones, we retry later.
                    # Requirement says "migrate", implying we fix it now.
                    # If we can't retrieve, we can't migrate.
                    continue

                # Upload to NEW nodes
                candidates = [n for n in active_nodes_objects if n.get_id().rstrip(
                    '/') not in [l.rstrip('/') for l in live_locations]]

                if not candidates:
                    # No new nodes available to increase redundancy
                    # Just update manifest to remove dead nodes
                    chunk['locations'] = live_locations
                    continue

                if len(candidates) < needed:
                    needed = len(candidates)

                if needed > 0:
                    selected_targets = random.sample(candidates, needed)
                    new_locs = []

                    # Manual store
                    for node in selected_targets:
                        try:
                            # Run synchronously blocking call in executor
                            success = await loop.run_in_executor(None, node.store, chunk_id, content)
                            if success:
                                new_locs.append(node.get_id())
                        except:
                            pass

                    # Update locations
                    chunk['locations'] = live_locations + new_locs
                    repaired_chunks_count += 1

            elif len(locations) != len(live_locations):
                # We have enough redundancy, but some original nodes are dead.
                # Update manifest to reflect reality?
                # User said "if a node is turned off... migrate".
                # If we have 5 copies, and 1 dies, we have 4. If target is 5, we migrate (add 1).
                # This is handled by the block above.

                # If we have 10 copies, and 1 dies, and target is 5. We have 9. > 5.
                # We should just remove the dead one from manifest to keep it clean.
                chunk['locations'] = live_locations
                updates_needed = True

        if updates_needed:
            self.meta_mgr.update_manifest_chunks(manifest['filename'], chunks)
            msg = f"Repair completed. Repaired {repaired_chunks_count} chunks."
            if progress_cb:
                progress_cb(100, msg)
            return True
        else:
            if progress_cb:
                progress_cb(100, "Network healthy. No repairs needed.")
            return False
