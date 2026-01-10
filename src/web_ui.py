import os
import glob
import json
import asyncio
import aiohttp
from aiohttp import web
from types import SimpleNamespace

# Determine imports based on execution context
try:
    # If running as module or from root
    from src.main import distribute, reconstruct, prune_orphans, CatalogClient
    from src.network.discovery import scan_network
    from src.core.repair import RepairManager
    from src.core.metadata import MetadataManager
    from src.network.remote_node import RemoteHttpNode
    from src.core.distribution import DistributionStrategy
    from src.core.filesystem import FilesystemManager, DirectoryNode
except ImportError:
    try:
        # If running from src directory
        from main import distribute, reconstruct, prune_orphans, CatalogClient
        from network.discovery import scan_network
        from core.repair import RepairManager
        from core.metadata import MetadataManager
        from network.remote_node import RemoteHttpNode
        from core.distribution import DistributionStrategy
        from core.filesystem import FilesystemManager, DirectoryNode
    except ImportError as e:
        print(f"Import Error: {e}")
        # Last resort for direct execution
        import sys
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from src.main import distribute, reconstruct, prune_orphans, CatalogClient
        from src.core.distribution import DistributionStrategy
        from src.network.discovery import scan_network
        from src.core.repair import RepairManager
        from src.core.metadata import MetadataManager
        from src.network.remote_node import RemoteHttpNode
        from src.core.filesystem import FilesystemManager, DirectoryNode


import uuid
import hashlib
import random
from typing import Optional

# Global task tracker
TASKS = {}
FS_MANAGER = FilesystemManager()
FS_ROOT_KEY = "filesystem_root_v1"
FS_LOCK = asyncio.Lock()  # Serialize FS modifications
WEB_UI_NODE_ID = hashlib.sha256(b"web_ui_client").hexdigest()
CACHED_ROOT_ID = None  # In-memory cache for strict consistency during updates
NODE_CACHE = {}  # In-memory cache for directory manifests (ID -> JSON content)

# Peer Cache to avoid redundant network scans
PEER_CACHE = set()
LAST_PEER_SCAN = 0
PEER_SCAN_LOCK = asyncio.Lock()
CACHE_TTL = 300  # 5 minutes


async def get_active_peers():
    """Returns a list of cached peers or performs a scan if cache is stale."""
    global PEER_CACHE, LAST_PEER_SCAN
    import time

    async with PEER_SCAN_LOCK:
        now = time.time()
        if PEER_CACHE and (now - LAST_PEER_SCAN < CACHE_TTL):
            return list(PEER_CACHE)

        # Cache expired or empty, perform scan
        try:
            # Run scan in executor to avoid blocking if it was blocking (scan_network is async though)
            # scan_network is async, so we await it directly
            found = await scan_network(timeout=3)
            if found:
                PEER_CACHE.update(found)
                LAST_PEER_SCAN = now
                print(f"Updated Peer Cache: {len(PEER_CACHE)} peers")
            return list(PEER_CACHE)
        except Exception as e:
            print(f"Peer scan failed: {e}")
            return list(PEER_CACHE)


async def handle_index(request):
    path = os.path.join(os.path.dirname(__file__), 'web/index.html')
    return web.FileResponse(path)


async def handle_filesystem(request):
    path = os.path.join(os.path.dirname(__file__), 'web/filesystem.html')
    return web.FileResponse(path)


async def handle_topology(request):
    path = os.path.join(os.path.dirname(__file__), 'web/topology.html')
    return web.FileResponse(path)


async def get_progress(request):
    task_id = request.match_info['task_id']
    task = TASKS.get(task_id)
    if not task:
        return web.json_response({"status": "error", "message": "Task not found"}, status=404)
    return web.json_response(task)


async def list_manifests(request):
    manifests_dir = 'manifests'
    if not os.path.exists(manifests_dir):
        os.makedirs(manifests_dir)
    files = glob.glob(os.path.join(manifests_dir, "*.manifest"))

    results = []
    for f in files:
        try:
            with open(f, 'r') as fp:
                data = json.load(fp)
                results.append({
                    "filename": data.get("filename", os.path.basename(f).replace(".manifest", "")),
                    "size": data.get("size", 0)
                })
        except Exception:
            # Fallback if manifest is corrupted or old format
            results.append({
                "filename": os.path.basename(f).replace(".manifest", ""),
                "size": 0
            })

    return web.json_response(results)


async def upload_file(request):
    reader = await request.multipart()

    # Defaults
    filename = None
    temp_path = None
    redundancy = 5
    compression = True

    while True:
        field = await reader.next()
        if field is None:
            break

        if field.name == 'file':
            filename = field.filename
            # Security fix: Ensure strictly flat filename, no directories
            if filename:
                filename = os.path.basename(filename)

            upload_dir = "uploads_temp"
            os.makedirs(upload_dir, exist_ok=True)
            temp_path = os.path.join(upload_dir, filename)

            with open(temp_path, 'wb') as f:
                while True:
                    chunk = await field.read_chunk()
                    if not chunk:
                        break
                    f.write(chunk)

        elif field.name == 'redundancy':
            val = await field.read(decode=True)
            try:
                redundancy = int(val.decode('utf-8'))
            except:
                pass

        elif field.name == 'compression':
            val = await field.read(decode=True)
            val_str = val.decode('utf-8').lower()
            compression = (val_str == 'true')

    if not temp_path:
        return web.json_response({"status": "error", "message": "No file uploaded"}, status=400)

    # Resolve network peers once to reuse connection info
    cached_peers = await get_active_peers()
    entry_node = random.choice(cached_peers) if cached_peers else None
    use_scan = (entry_node is None)

    # Prepare args for distribute
    args = SimpleNamespace(
        file=temp_path,
        entry_node=entry_node,
        scan=use_scan,
        redundancy=redundancy,
        compression=compression
    )

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing",
                      "percent": 0, "message": "Starting..."}

    def progress_cb(percent, message):
        TASKS[task_id]["percent"] = percent
        TASKS[task_id]["message"] = message
        if percent == 100:
            TASKS[task_id]["status"] = "completed"
        elif percent == -1:
            TASKS[task_id]["status"] = "error"

    loop = asyncio.get_event_loop()

    # Wrapper to properly await the executor future and handle top-level exceptions
    async def background_distribute():
        print(f"[Upload Task {task_id}] Starting distribution for {temp_path}")
        try:
            await loop.run_in_executor(None, lambda: distribute(args, progress_callback=progress_cb))
        except Exception as e:
            print(
                f"[Upload Task {task_id}] Background distribution error: {e}")
            import traceback
            traceback.print_exc()
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Internal Error: {str(e)}"
        finally:
            # Clean up uploaded file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                    print(f"Cleaned up temp file: {temp_path}")
                except Exception as cleanup_error:
                    print(f"Error cleaning up temp file: {cleanup_error}")

    # Fire and forget (in background task)
    asyncio.create_task(background_distribute())

    return web.json_response({
        "status": "processing",
        "task_id": task_id,
        "message": "Upload received, distribution started."
    })

    return web.Response(status=400, text="No file provided")


async def download_file(request):
    try:
        data = await request.json()
    except:
        return web.json_response({"status": "error", "message": "Invalid JSON"}, status=400)

    manifest_name = data.get('manifest')
    if not manifest_name:
        return web.json_response({"status": "error", "message": "No manifest specified"}, status=400)

    # Check if input is a Manifest ID (64 char hex)
    is_id = len(manifest_name) == 64 and all(
        c in '0123456789abcdefABCDEF' for c in manifest_name)

    # Auto-append extension only if it's NOT an ID
    if not is_id and not manifest_name.endswith('.manifest'):
        manifest_name += '.manifest'

    # Determine paths
    if is_id:
        manifest_path = manifest_name  # Pass ID directly
        # Default output name if not provided
        default_out = f"download_{manifest_name[:8]}"
        output_name = data.get('output_name', default_out)
    else:
        output_name = data.get(
            'output_name', manifest_name.replace('.manifest', ''))
        manifest_path = os.path.join('manifests', manifest_name)

    output_path = os.path.join('downloads', output_name)
    os.makedirs('downloads', exist_ok=True)

    args = SimpleNamespace(
        manifest=manifest_path,
        output=output_path,
        entry_node=None,
        scan=True,
        local=False,
        kill_node=None
    )

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "status": "processing",
        "percent": 0,
        "message": "Initializing download..."
    }

    def progress_callback(percent, message):
        if task_id in TASKS:
            if percent == -1:
                TASKS[task_id]["status"] = "error"
                TASKS[task_id]["message"] = message
            elif percent == 100:
                TASKS[task_id]["status"] = "completed"
                TASKS[task_id]["percent"] = 100
                # Pass the download URL in the message or handle differently?
                # The frontend polls this task. When completed, it wants the URL.
                # We can add extra fields to the task dict.
                TASKS[task_id]["message"] = message
                TASKS[task_id]["download_url"] = f"/downloads/{output_name}"
            else:
                TASKS[task_id]["percent"] = percent
                TASKS[task_id]["message"] = message

    async def background_reconstruct():
        LOOP = asyncio.get_running_loop()
        try:
            # Run blocking reconstruct in executor
            await LOOP.run_in_executor(None, lambda: reconstruct(args, progress_callback))
        except Exception as e:
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Internal Error: {str(e)}"

    # Fire and forget
    asyncio.create_task(background_reconstruct())

    return web.json_response({
        "status": "processing",
        "task_id": task_id,
        "message": "Download started"
    })


async def repair_file(request):
    try:
        data = await request.json()
    except:
        return web.json_response({"status": "error", "message": "Invalid JSON"}, status=400)

    manifest_name = data.get('manifest')
    if not manifest_name:
        return web.json_response({"status": "error", "message": "No manifest specified"}, status=400)

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing",
                      "percent": 0, "message": "Starting repair..."}

    def progress_cb(percent, message):
        TASKS[task_id]["percent"] = percent
        TASKS[task_id]["message"] = message
        if percent == 100:
            TASKS[task_id]["status"] = "completed"
        elif percent == -1:
            TASKS[task_id]["status"] = "error"

    loop = asyncio.get_event_loop()

    async def background_repair():
        try:
            meta_mgr = MetadataManager()
            repair_mgr = RepairManager(meta_mgr)
            await repair_mgr.repair_manifest(manifest_name, redundancy_target=5, progress_cb=progress_cb)
        except Exception as e:
            print(f"Repair error: {e}")
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Error: {str(e)}"

    asyncio.create_task(background_repair())

    return web.json_response({
        "status": "processing",
        "task_id": task_id,
        "message": "Repair started"
    })


async def clean_orphans(request):
    """API Endpoint to delete orphan chunks."""
    try:
        # Run prune in executor to avoid blocking main loop (synchronous file IO)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: prune_orphans(None))
        return web.json_response({"status": "ok", "message": "Orphan chunks pruned successfully."})
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Prune failed: {e}"}, status=500)


async def get_network_graph(request):
    try:
        # 1. Initial Discovery
        # Timeout slightly larger than new BEACON_INTERVAL (1s)
        found = await scan_network(timeout=1.5)
        edges = []
        # Normalize URLs
        nodes = set(u.rstrip('/') for u in found)

        if not nodes:
            return web.json_response({"nodes": [], "edges": []})

        # 2. Parallel crawl of ALL found nodes to discover connections
        async with aiohttp.ClientSession() as session:
            tasks = []
            peers_list = list(nodes)

            async def fetch_peers(node_url):
                try:
                    async with session.get(f"{node_url}/peers", timeout=1.0) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return node_url, data.get('peers', [])
                except:
                    pass
                return node_url, []

            # Launch all requests in parallel
            tasks = [fetch_peers(p) for p in peers_list]
            results = await asyncio.gather(*tasks)

            # 3. Build the graph
            for node, neighbors in results:
                for neighbor in neighbors:
                    neighbor = neighbor.rstrip('/')
                    if neighbor != node:
                        # If neighbor was not in list, add it (optional, but useful)
                        nodes.add(neighbor)
                        edges.append({"from": node, "to": neighbor})

        return web.json_response({
            "nodes": [{"id": n, "label": n.replace("http://", "").rstrip('/')} for n in nodes],
            "edges": edges
        })
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)


async def check_chunk_task(session, chunk_id, peers):
    """
    Checks which nodes have a specific chunk.
    Executes HEAD requests in parallel for each known peer.
    """
    locations = []
    # Execute parallel check on all peers
    tasks = []
    peer_list = list(peers)  # Snap shot

    async def sub_check(p):
        try:
            # Increased timeout to 3.0s to ensure we don't miss nodes under load
            async with session.head(f"{p}/chunk/{chunk_id}", timeout=3.0) as resp:
                if resp.status == 200:
                    return p
        except:
            pass
        return None

    tasks = [sub_check(p) for p in peer_list]
    results = await asyncio.gather(*tasks)

    locations = [r for r in results if r]
    return chunk_id, locations


async def get_manifest_detail(request):
    name = request.match_info['name']
    if not name.endswith('.manifest'):
        name += '.manifest'

    manifest_path = os.path.join('manifests', name)
    if not os.path.exists(manifest_path):
        return web.json_response({"error": "Manifest not found"}, status=404)

    with open(manifest_path, 'r') as f:
        data = json.load(f)

    # The manifest on disk DOES NOT have locations (for privacy).
    # We must retrieve them dynamically querying the network now.

    # 1. Find active nodes (Discovery)
    # Use robust discovery (Scan + Crawl) similar to delete
    # Increased timeout for better discovery reliability
    found_peers = await scan_network(timeout=2.0)

    queue = list(found_peers)
    visited = set(found_peers)

    # Lightweight crawl (depth 1)
    if queue:
        async with aiohttp.ClientSession() as session:
            crawl_tasks = []
            for p in queue:
                crawl_tasks.append(session.get(f"{p}/peers", timeout=0.8))

            try:
                responses = await asyncio.gather(*crawl_tasks, return_exceptions=True)
                for res in responses:
                    if not isinstance(res, Exception) and res.status == 200:
                        try:
                            p_data = await res.json()
                            for extended_peer in p_data.get('peers', []):
                                visited.add(extended_peer.rstrip('/'))
                        except:
                            pass
            except:
                pass

    active_peers = list(visited)

    if not active_peers:
        # No nodes found, return "naked" manifest
        pass
    else:
        # 2. For each chunk, ask who has it
        #    This can be heavy for large files, limit parallelism if needed
        async with aiohttp.ClientSession() as session:
            chunk_tasks = []
            for chunk in data['chunks']:
                chunk_tasks.append(check_chunk_task(
                    session, chunk['id'], active_peers))

            # Execute all chunk queries in parallel
            results = await asyncio.gather(*chunk_tasks)

            # Map results
            lookup = {cid: locs for cid, locs in results}

            # Enrich response JSON
            for chunk in data['chunks']:
                chunk['locations'] = lookup.get(chunk['id'], [])

    return web.json_response(data)


async def delete_manifest(request):
    name = request.match_info['name']

    # Check if input is a Manifest ID (64 char hex)
    is_id = len(name) == 64 and all(
        c in '0123456789abcdefABCDEF' for c in name)

    # 1. Discover Peers
    # Use a stronger discovery method (Scan + Crawl)
    found_peers = await scan_network(timeout=3.0)

    # Crawl to find more peers
    visited = set(found_peers)
    queue = list(found_peers)

    if queue:
        print(f"Delete: Initial scan found {len(queue)} peers. Crawling...")
        async with aiohttp.ClientSession() as session:
            crawl_tasks = []
            for p in queue:
                crawl_tasks.append(session.get(f"{p}/peers", timeout=1.0))

            try:
                responses = await asyncio.gather(*crawl_tasks, return_exceptions=True)
                for res in responses:
                    if not isinstance(res, Exception) and res.status == 200:
                        try:
                            p_data = await res.json()
                            for extended_peer in p_data.get('peers', []):
                                visited.add(extended_peer.rstrip('/'))
                        except:
                            pass
            except:
                pass

    final_peers = list(visited)
    print(f"Delete: Target peers count: {len(final_peers)}")

    chunks_to_delete = []
    manifest_id_to_delete = None

    # 2. Resolve Manifest (Memory/Network vs Local File)
    if is_id:
        manifest_id_to_delete = name
        print(f"Delete: Resolving manifest ID {name} from network...")

        # Try to fetch manifest content to identify chunks
        manifest_data = None
        if final_peers:
            async with aiohttp.ClientSession() as session:
                for peer in final_peers:
                    try:
                        # Try to get the manifest blob
                        async with session.get(f"{peer}/chunk/{name}", timeout=2.0) as resp:
                            if resp.status == 200:
                                manifest_data = await resp.read()
                                print(f"Delete: Found manifest at {peer}")
                                break
                    except:
                        continue

        if manifest_data:
            try:
                data = json.loads(manifest_data)
                chunks_to_delete = [c['id'] for c in data.get('chunks', [])]
                print(
                    f"Delete: Found {len(chunks_to_delete)} content chunks to remove.")
            except Exception as e:
                print(f"Delete: Failed to parse manifest JSON: {e}")
        else:
            print(
                "Delete: Warning - Manifest content not found on network. Only deleting ID.")

    else:
        # Legacy File Mode
        if not name.endswith('.manifest'):
            name += '.manifest'

        manifest_path = os.path.join('manifests', name)
        if not os.path.exists(manifest_path):
            return web.json_response({"error": "Manifest not found (locally) and not a valid ID"}, status=404)

        try:
            # 1. First, search Catalog for this filename to get the authoritative ID
            # This fixes issues where local file hash differs from cloud catalog ID
            print(f"Delete: Searching catalog for ID of '{name}'...")
            from src.main import CatalogClient
            # We need remote nodes for catalog client
            cat_nodes = [RemoteHttpNode(u) for u in final_peers] if final_peers else [
                RemoteHttpNode(f"http://127.0.0.1:{p}") for p in range(8000, 8005)]

            cat_client = CatalogClient()
            items = await cat_client.fetch(cat_nodes)

            found_ids_in_catalog = []
            target_filename = name.replace('.manifest', '')

            for item in items:
                # Match Name (Case insensitive or exact?) Exact for now
                if item.get('name') == target_filename or item.get('name') == name:
                    found_ids_in_catalog.append(item['id'])

            if found_ids_in_catalog:
                print(
                    f"Delete: Found matching IDs in catalog: {found_ids_in_catalog}")
                # Take first match
                manifest_id_to_delete = found_ids_in_catalog[0]

            with open(manifest_path, 'r') as f:
                # Read content to calculate Hash ID for Catalog removal
                content_str = f.read()
                data = json.loads(content_str)
                chunks_to_delete = [c['id'] for c in data.get('chunks', [])]

                if not manifest_id_to_delete:
                    # Calculate Manifest ID (Hash of the JSON string as it was distributed)
                    try:
                        # Re-serialize to match standard (Compact, no space) if possible or use read content
                        # The Standard used in 'distribute' is: json.dumps(dict).encode('utf-8')
                        norm_bytes = json.dumps(data).encode('utf-8')
                        manifest_id_to_delete = hashlib.sha256(
                            norm_bytes).hexdigest()
                        print(
                            f"Delete: Calculated ID from local file: {manifest_id_to_delete}")
                    except Exception as e:
                        print(f"Delete: Could not calculate ID: {e}")

            # Delete local file
            os.remove(manifest_path)
        except Exception as e:
            return web.json_response({"error": f"Invalid manifest or file error: {e}"}, status=500)

    # 3. Execute Distributed Delete
    await _delete_file_from_network(manifest_id_to_delete)

    return web.json_response({"status": "ok", "message": f"Deleted manifest {name}"})


async def stream_download(request):
    """Streams a file directly to the client without saving to disk."""
    manifest_name = request.match_info['name']

    # Auto-append extension if missing
    if not manifest_name.endswith('.manifest'):
        manifest_name += '.manifest'

    manifest_path = os.path.join('manifests', manifest_name)
    if not os.path.exists(manifest_path):
        return web.Response(status=404, text="Manifest not found")

    # Load manifest to get filename
    try:
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
            original_filename = manifest_data.get(
                'filename', 'downloaded_file')
    except:
        original_filename = 'downloaded_file'

    args = SimpleNamespace(
        manifest=manifest_path,
        output="dummy_output",
        entry_node=None,
        scan=True,
        local=False,
        kill_node=None
    )

    loop = asyncio.get_event_loop()

    try:
        # Run blocking reconstruction in thread pool to avoid blocking the event loop
        result = await loop.run_in_executor(
            None,
            lambda: reconstruct(args, progress_callback=None, stream=True)
        )

        if not result:
            return web.Response(status=500, text="Reconstruction failed (Nodes not found or chunks missing)")

        shard_mgr, chunks_data, compression_mode, manifest_obj = result

    except Exception as e:
        return web.Response(status=500, text=f"Internal Error: {str(e)}")

    # Prepare Stream Response
    headers = {
        "Content-Disposition": f'attachment; filename="{original_filename}"',
        "Content-Type": "application/octet-stream"
    }

    response = web.StreamResponse(headers=headers)
    await response.prepare(request)

    try:
        for chunk_data in shard_mgr.yield_reconstructed_chunks(chunks_data, compression_mode=compression_mode):
            await response.write(chunk_data)
    except Exception as e:
        print(f"Streaming error: {e}")

    return response


async def stream_download_by_id(request):
    """Streams a file directly by ID, fetching the manifest first."""
    manifest_id = request.match_info['id']

    args = SimpleNamespace(
        manifest=manifest_id,  # reconstruct handles ID automatically
        output="dummy_output",
        entry_node=None,
        scan=True,
        local=False,
        kill_node=None
    )

    loop = asyncio.get_event_loop()

    try:
        # Run blocking reconstruction in thread pool
        result = await loop.run_in_executor(
            None,
            lambda: reconstruct(args, progress_callback=None, stream=True)
        )

        if not result:
            return web.Response(status=404, text=f"File content with ID {manifest_id} not found on the network (might be deleted).")

        shard_mgr, chunks_data, compression_mode, manifest_obj = result

        # Get filename directly from in-memory manifest object
        original_filename = manifest_obj.get(
            'filename', f"downloaded_{manifest_id}.bin")

    except RuntimeError as e:
        # Handle "Chunk not found" specifically
        error_msg = str(e)
        if "not found on network" in error_msg:
            return web.Response(status=404, text=f"Not Found: {error_msg}")
        return web.Response(status=500, text=f"Internal Error: {error_msg}")
    except Exception as e:
        return web.Response(status=500, text=f"Internal Error: {e}")

    headers = {
        "Content-Disposition": f'attachment; filename="{original_filename}"',
        "Content-Type": "application/octet-stream"
    }

    response = web.StreamResponse(headers=headers)
    await response.prepare(request)

    try:
        for chunk_data in shard_mgr.yield_reconstructed_chunks(chunks_data, compression_mode=compression_mode):
            await response.write(chunk_data)
    except Exception as e:
        print(f"Streaming error: {e}")

    return response


async def get_manifest_by_id(request):
    manifest_id = request.match_info['id']

    # 1. Generic scan attempt
    peers = await scan_network()
    if not peers:
        # Fallback to defaults if scan fails
        peers = ['http://127.0.0.1:8000',
                 'http://127.0.0.1:8001', 'http://127.0.0.1:8002']

    # Crawl to find more peers for robust retrieval and better graph
    queue = list(peers)
    visited = set(peers)

    if queue:
        async with aiohttp.ClientSession() as session:
            crawl_tasks = []
            for p in queue:
                crawl_tasks.append(session.get(f"{p}/peers", timeout=0.8))

            try:
                responses = await asyncio.gather(*crawl_tasks, return_exceptions=True)
                for res in responses:
                    if not isinstance(res, Exception) and res.status == 200:
                        try:
                            p_data = await res.json()
                            for extended_peer in p_data.get('peers', []):
                                visited.add(extended_peer.rstrip('/'))
                        except:
                            pass
            except:
                pass

    active_peers = list(visited)
    nodes = [RemoteHttpNode(u) for u in active_peers]
    distributor = DistributionStrategy(nodes)

    # 2. Fetch Manifest
    try:
        # If active_peers list is empty or small, retrieve_chunk might fail fast.
        # Ensure we have a valid distributor even if peers are few.
        if not nodes:
            # Last resort fallback
            nodes = [RemoteHttpNode(
                f"http://127.0.0.1:{8000+i}") for i in range(5)]
            distributor = DistributionStrategy(nodes)

        manifest_data_bytes = await asyncio.get_event_loop().run_in_executor(
            None, lambda: distributor.retrieve_chunk(manifest_id)
        )
        data = json.loads(manifest_data_bytes)
    except Exception as e:
        print(f"Map Error: Manifest {manifest_id} retrieval failed: {e}")
        return web.json_response({"error": f"Failed to retrieve manifest {manifest_id}: {str(e)}"}, status=404)

    # 3. Location Lookup (Enrich with 'locations')
    if active_peers:
        async with aiohttp.ClientSession() as session:
            chunk_tasks = []
            for chunk in data['chunks']:
                chunk_tasks.append(check_chunk_task(
                    session, chunk['id'], active_peers))

            # Execute all checks in parallel
            results = await asyncio.gather(*chunk_tasks)
            lookup = {cid: locs for cid, locs in results}

            for chunk in data['chunks']:
                chunk['locations'] = lookup.get(chunk['id'], [])

    return web.json_response(data)


# --- Filesystem Helpers ---

async def get_dht_nodes(count=5):
    """Returns a deterministic list of RemoteHttpNodes for metadata consistency."""
    # We prioritize a stable set of core nodes (8000+) to ensure Read-Your-Writes consistency for metadata.
    # In a real DHT, we would hash the key to find the node, but here we just replicate to the first N nodes.
    stable_peers = [f"http://127.0.0.1:{8000+i}" for i in range(20)]
    return [RemoteHttpNode(u) for u in stable_peers[:count]]


async def fs_fetch_node_fn(node_id: str) -> Optional[str]:
    # Check local cache first (Read-Your-Writes)
    if node_id in NODE_CACHE:
        # print(f"FS DEBUG: Cache HIT for {node_id[:8]}")
        return NODE_CACHE[node_id]

    print(f"FS DEBUG: Cache MISS for {node_id[:8]}, querying network...")
    # Query stable nodes
    gateways = await get_dht_nodes(10)

    # Parallel Fetch (Race)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for gateway in gateways:
            url = f"{gateway.url}/dht/find_value"
            payload = {"key": node_id, "sender": {
                "sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}}
            
            # Wrap in a task
            task = asyncio.create_task(session.post(url, json=payload, timeout=2))
            tasks.append(task)
        
        # Wait for first success
        pending = tasks
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    resp = await task
                    if resp.status == 200:
                        data = await resp.json()
                        if "value" in data:
                            val = data["value"]
                            # Cancel pending
                            for p in pending:
                                p.cancel()
                                
                            if isinstance(val, str):
                                NODE_CACHE[node_id] = val
                                return val
                            json_val = json.dumps(val)
                            NODE_CACHE[node_id] = json_val
                            return json_val
                except Exception:
                    pass
                    
    return None


async def fs_store_node_fn(node: DirectoryNode) -> str:
    # Replicate to multiple nodes
    gateways = await get_dht_nodes(5)
    node_hash = node.get_hash()

    # Update local cache immediately
    NODE_CACHE[node_hash] = node.to_json()

    success_count = 0
    async with aiohttp.ClientSession() as session:
        async def _pusher(gw):
            nonlocal success_count
            try:
                url = f"{gw.url}/dht/store"
                payload = {
                    "key": node_hash,
                    "value": node.to_json(),
                    "sender": {"sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}
                }
                async with session.post(url, json=payload, timeout=2) as resp:
                    if resp.status == 200:
                        success_count += 1
            except:
                pass

        await asyncio.gather(*[_pusher(g) for g in gateways])

    if success_count == 0:
        raise RuntimeError(
            f"Failed to store directory node {node_hash} on any gateway")

    return node_hash


async def fs_get_root_id() -> Optional[str]:
    global CACHED_ROOT_ID
    if CACHED_ROOT_ID:
        return CACHED_ROOT_ID

    # Try to find root key on any node
    gateways = await get_dht_nodes(10)
    async with aiohttp.ClientSession() as session:
        for gateway in gateways:
            try:
                url = f"{gateway.url}/dht/find_value"
                payload = {"key": FS_ROOT_KEY, "sender": {
                    "sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}}
                async with session.post(url, json=payload, timeout=2) as resp:
                    data = await resp.json()
                    if "value" in data:
                        CACHED_ROOT_ID = data["value"]
                        return data["value"]
            except Exception:
                pass
    return None


async def fs_set_root_id(new_root_id: str):
    global CACHED_ROOT_ID
    CACHED_ROOT_ID = new_root_id

    # Publish to MANY nodes to ensure availability
    gateways = await get_dht_nodes(10)

    success_count = 0
    async with aiohttp.ClientSession() as session:
        async def _pusher(gw):
            nonlocal success_count
            try:
                url = f"{gw.url}/dht/store"
                payload = {
                    "key": FS_ROOT_KEY,
                    "value": new_root_id,
                    "sender": {"sender_id": WEB_UI_NODE_ID, "host": "127.0.0.1", "port": 0}
                }
                async with session.post(url, json=payload, timeout=2) as resp:
                    if resp.status == 200:
                        success_count += 1
            except Exception:
                pass

        await asyncio.gather(*[_pusher(g) for g in gateways])

    if success_count == 0:
        print(
            f"Warning: Failed to update FS Root ID {FS_ROOT_KEY} on any node.")

# --- Filesystem Handlers ---


async def handle_fs_ls(request):
    """
    GET /api/fs/ls?path=/foo/bar
    """
    path = request.query.get("path", "/")

    root_id = await fs_get_root_id()
    if not root_id:
        return web.json_response({
            "path": path,
            "entries": []
        })

    # Resolve ID of the directory at path
    target_id = await FS_MANAGER.resolve_path(root_id, path, fs_fetch_node_fn)

    if not target_id:
        return web.json_response({"error": "Path not found"}, status=404)

    # Fetch content
    content_json = await fs_fetch_node_fn(target_id)
    if not content_json:
        return web.json_response({"error": "Object missing or unreachable"}, status=404)

    try:
        node = FS_MANAGER.load_directory(content_json)
        # Transform entries for UI
        entries = []
        for name, data in node.entries.items():
            entries.append({
                "name": name,
                "type": data["type"],
                "size": data.get("size", 0),
                "id": data["id"],
                "modified": data.get("modified")
            })

        return web.json_response({
            "path": path,
            "entries": entries
        })
    except ValueError:
        return web.json_response({"error": "Not a directory"}, status=400)


async def handle_fs_mkdir(request):
    """
    POST /api/fs/mkdir 
    { "path": "docs", "name": "work" } 
    """
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    base_path = data.get("path", "/")
    new_dir_name = data.get("name")

    if not new_dir_name:
        return web.json_response({"error": "Name required"}, status=400)

    # Sanitize inputs
    base_path = base_path.strip()
    new_dir_name = new_dir_name.strip().replace("/", "")

    full_path = ""
    if base_path == "/" or base_path == "":
        full_path = new_dir_name
    else:
        full_path = f"{base_path}/{new_dir_name}".replace("//", "/")

    async with FS_LOCK:
        root_id = await fs_get_root_id()

        # Idempotency Check: Don't overwrite if it exists!
        try:
            existing_id = await FS_MANAGER.resolve_path(root_id, full_path, fs_fetch_node_fn)
            if existing_id:
                # Directory already exists, return Success immediately
                # (We could check if it is indeed a directory, but resolving implies existence)
                return web.json_response({"status": "ok", "path": full_path, "message": "exists"})
        except Exception:
            pass

        # Create empty directory
        new_dir = DirectoryNode(new_dir_name)
        new_dir_id = await fs_store_node_fn(new_dir)

        entry_data = {
            "type": "directory",
            "id": new_dir_id,
            "size": 0
        }

        try:
            new_root_id = await FS_MANAGER.update_path(
                root_id,
                base_path,
                new_dir_name,
                entry_data,
                fs_fetch_node_fn,
                fs_store_node_fn
            )

            await fs_set_root_id(new_root_id)
            return web.json_response({"status": "ok", "path": full_path})
        except Exception as e:
            import traceback
            traceback.print_exc()
            return web.json_response({"error": str(e)}, status=500)


async def handle_fs_add_file(request):
    """
    POST /api/fs/add_file
    { "path": "/docs", "name": "foo.txt", "id": "manifest_id...", "size": 1234 }
    """
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    base_path = data.get("path", "/")
    file_name = data.get("name")
    file_id = data.get("id")
    file_size = data.get("size", 0)

    if not file_name or not file_id:
        return web.json_response({"error": "Name and ID required"}, status=400)

    async with FS_LOCK:
        root_id = await fs_get_root_id()

        entry_data = {
            "type": "file",
            "id": file_id,
            "size": file_size
        }

        try:
            new_root_id = await FS_MANAGER.update_path(
                root_id,
                base_path,
                file_name,
                entry_data,
                fs_fetch_node_fn,
                fs_store_node_fn
            )

            await fs_set_root_id(new_root_id)
            return web.json_response({"status": "ok", "path": f"{base_path}/{file_name}"})
        except Exception as e:
            print(f"Error adding file to FS: {e}")
            return web.json_response({"error": str(e)}, status=500)


async def _delete_file_from_network(manifest_id, name_hint=None):
    """
    Helper function to delete manifest and chunks from the network.
    Refactored from delete_manifest.
    """
    print(
        f"Delete: Starting network deletion for ID={manifest_id} Name={name_hint}")

    # 1. Discover Peers
    peers = await scan_network()
    # Unique list
    final_peers = list(set(peers)) if peers else []

    chunks_to_delete = []

    # 2. Fetch Manifest content to identify Chunks
    # We must do this BEFORE deleting the manifest itself :)
    distrib = DistributionStrategy([])
    manifest_data = None

    # Try fetching via multiple peers manually since DistributionStrategy needs nodes list
    if final_peers:
        # Use simple HTTP fetch first to find the manifest storage
        async with aiohttp.ClientSession() as session:
            for peer in final_peers[:10]:
                try:
                    # Retrieve the manifest chunk directly
                    async with session.get(f"{peer}/chunk/{manifest_id}", timeout=3) as r:
                        if r.status == 200:
                            manifest_data = await r.read()
                            break
                except:
                    continue

    if manifest_data:
        try:
            # It might be JSON string, bytes, or dict
            if isinstance(manifest_data, (str, bytes)):
                data = json.loads(manifest_data)
            else:
                data = manifest_data
            chunks_to_delete = [c['id'] for c in data.get('chunks', [])]
            print(
                f"Delete: Found {len(chunks_to_delete)} content chunks to remove.")
        except Exception as e:
            print(f"Delete: Failed to parse manifest JSON: {e}")
    else:
        print("Delete: Warning - Manifest content not found on network. Only deleting ID.")

    # 3. Execute Distributed Delete
    broadcast_targets = list(final_peers) if final_peers else []
    if not broadcast_targets:
        broadcast_targets = [
            f"http://127.0.0.1:{p}" for p in range(8000, 8003)]

    if broadcast_targets and (chunks_to_delete or manifest_id):
        async with aiohttp.ClientSession() as session:
            tasks = []

            # Delete content chunks
            if chunks_to_delete:
                batch_size = 500
                for i in range(0, len(chunks_to_delete), batch_size):
                    batch = chunks_to_delete[i:i + batch_size]
                    payload = {"chunk_ids": batch}
                    for peer in broadcast_targets:
                        tasks.append(session.post(
                            f"{peer}/chunks/delete_batch", json=payload, timeout=5))

            # Delete manifest ID itself
            if manifest_id:
                payload_manifest = {"chunk_ids": [manifest_id]}
                for peer in broadcast_targets:
                    tasks.append(session.post(
                        f"{peer}/chunks/delete_batch", json=payload_manifest, timeout=5))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    # 4. Remove from DHT Public Catalog
    if manifest_id:
        try:
            from network.remote_node import RemoteHttpNode
            try:
                from main import CatalogClient
            except ImportError:
                from src.main import CatalogClient

            dht_nodes = [RemoteHttpNode(url) for url in broadcast_targets]
            cat = CatalogClient()
            await cat.delete(manifest_id, dht_nodes)
            # Also try deleting by name if provided, just in case
            if name_hint:
                await cat.delete(name_hint, dht_nodes)

        except Exception as e:
            print(f"Delete: Failed to remove from DHT Catalog: {e}")


async def _recursive_collect_manifests(node_id):
    """
    Recursively collects all file manifest IDs from a directory structure.
    Used to clean up all files when a directory is deleted.
    Returns a list of tuples: (id, name)
    """
    manifests = []
    try:
        content = await fs_fetch_node_fn(node_id)
        if not content:
            return []

        # Ensure we handle bytes/str from the fetch
        if isinstance(content, bytes):
            content = content.decode('utf-8')

        try:
            node_data = json.loads(content)
        except:
            return []

        if node_data.get("type") != "directory":
            return []

        entries = node_data.get("entries", {})
        
        # Parallel collection tasks
        dir_tasks = []
        
        for item_name, info in entries.items():
            if info["type"] == "file":
                manifests.append((info["id"], item_name))
            elif info["type"] == "directory":
                dir_tasks.append(_recursive_collect_manifests(info["id"]))
        
        if dir_tasks:
            results = await asyncio.gather(*dir_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, list):
                    manifests.extend(res)
                    
    except Exception as e:
        print(f"Error recursively collecting manifests for {node_id}: {e}")

    return manifests


async def handle_fs_delete(request):
    """
    POST /api/fs/delete
    { "path": "/docs", "name": "foo.txt" }
    """
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    base_path = data.get("path", "/")
    name = data.get("name")

    if not name:
        return web.json_response({"error": "Name required"}, status=400)

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing", "progress": 5,
                      "message": "Initializing deletion..."}

    async def _cancel_task_fn():
        import traceback
        try:
            async with FS_LOCK:
                TASKS[task_id]["message"] = "Resolving path..."
                root_id = await fs_get_root_id()

                # Pre-check: Resolve parent to identify what we are deleting (file or dir)
                manifests_to_delete = []
                try:
                    # Resolve parent directory
                    parent_id = await FS_MANAGER.resolve_path(root_id, base_path, fs_fetch_node_fn)
                    if parent_id:
                        parent_json = await fs_fetch_node_fn(parent_id)
                        if parent_json:
                            parent_node = FS_MANAGER.load_directory(
                                parent_json)
                            if name in parent_node.entries:
                                entry = parent_node.entries[name]
                                if entry["type"] == "file":
                                    manifests_to_delete.append(
                                        (entry["id"], name))
                                elif entry["type"] == "directory":
                                    TASKS[task_id]["message"] = f"Scanning directory {name}..."
                                    print(
                                        f"Delete: Recursive cleanup target detected: {name}")
                                    collected = await _recursive_collect_manifests(entry["id"])
                                    manifests_to_delete.extend(collected)
                                    print(
                                        f"Delete: Found {len(collected)} nested files to remove contents for.")

                except Exception as e:
                    print(f"Delete Pre-check Error: {e}")

                TASKS[task_id]["message"] = "Updating filesystem structure..."
                TASKS[task_id]["progress"] = 10
                # Perform deletion
                new_root_id = await FS_MANAGER.delete_path(
                    root_id,
                    base_path,
                    name,
                    fs_fetch_node_fn,
                    fs_store_node_fn
                )

                await fs_set_root_id(new_root_id)
                TASKS[task_id]["progress"] = 20

            # Post-Lock: Trigger network deletion for all identified file manifests
            if manifests_to_delete:
                TASKS[task_id]["message"] = f"Cleaning up {len(manifests_to_delete)} files from network..."
                total = len(manifests_to_delete)
                completed = 0

                # Create a wrapper to return the name so we can update UI with specific file name
                async def delete_with_name(mid, fname):
                    await _delete_file_from_network(mid, name_hint=fname)
                    return fname

                pending = []
                for mid, fname in manifests_to_delete:
                    # Fire and forget deletion for each file
                    t = asyncio.create_task(delete_with_name(mid, fname))
                    pending.append(t)

                for i, t in enumerate(asyncio.as_completed(pending)):
                    finished_name = await t
                    completed += 1
                    # Progress from 20 to 100
                    percent = 20 + int((completed / total) * 80)
                    TASKS[task_id]["progress"] = percent
                    TASKS[task_id]["message"] = f"Deleted {completed}/{total}: {finished_name}"

            TASKS[task_id]["status"] = "completed"
            TASKS[task_id]["progress"] = 100
            TASKS[task_id]["message"] = "Deletion completed"

        except Exception as e:
            traceback.print_exc()
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = str(e)

    # Launch background task
    asyncio.create_task(_cancel_task_fn())

    return web.json_response({"status": "processing", "task_id": task_id})


async def handle_fs_move(request):
    """
    POST /api/fs/move
    { "source_path": "/", "source_name": "foo.txt", "dest_path": "/new_folder", "dest_name": "bar.txt" (optional) }
    """
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    source_path = data.get("source_path", "/")
    source_name = data.get("source_name")
    dest_path = data.get("dest_path", "/")
    # Use source name if dest_name not provided (move without rename)
    dest_name = data.get("dest_name") or source_name

    if not source_name:
        return web.json_response({"error": "source_name required"}, status=400)

    # Basic cycle check: if destination path starts with source path/name
    # Construct full source path
    full_src = (source_path.rstrip('/') + '/' + source_name).lstrip('/')
    full_dest = dest_path.lstrip('/')

    # Simple strings check for cycles (moving dir into itself)
    if full_dest.startswith(full_src):
        return web.json_response({"error": "Cannot move directory into itself"}, status=400)

    try:
        root_id = await fs_get_root_id()

        new_root_id = await FS_MANAGER.move_path(
            root_id,
            source_path,
            source_name,
            dest_path,
            dest_name,
            fs_fetch_node_fn,
            fs_store_node_fn
        )

        await fs_set_root_id(new_root_id)

        return web.json_response({"status": "ok"})
    except Exception as e:
        print(f"Error moving FS entry: {e}")
        return web.json_response({"error": str(e)}, status=500)


def start_web_server(port=8888):
    # Ensure downloads directory exists for static serving
    os.makedirs('downloads', exist_ok=True)

    app = web.Application()
    # Increase client_max_size for large uploads (e.g. 500MB)
    app._client_max_size = 500 * 1024 * 1024

    async def fetch_catalog(request):
        """
        Fetches the global public catalog from the network.
        """
        try:
            # 1. Quick discovery of some entry nodes
            peers = await scan_network()
            if not peers:
                # Fallback if scan fails
                peers = []

            nodes = [RemoteHttpNode(u) for u in peers]

            # If no nodes, try localhost:8000 as default entry
            if not nodes:
                nodes.append(RemoteHttpNode("http://127.0.0.1:8000"))

            cat = CatalogClient()
            items = await cat.fetch(nodes)

            # Returns list of {id, name, size, ts}
            return web.json_response(items)

        except Exception as e:
            print(f"Catalog fetch error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    app.add_routes([
        web.get('/', handle_index),
        web.get('/filesystem', handle_filesystem),
        web.get('/topology', handle_topology),
        web.get('/api/manifests', list_manifests),
        web.get('/api/manifests/{name}', get_manifest_detail),
        web.get('/api/manifests/id/{id}', get_manifest_by_id),
        web.get('/api/stream/{name}', stream_download),
        web.get('/api/stream_id/{id}', stream_download_by_id),
        web.delete('/api/manifests/{name}', delete_manifest),
        web.post('/api/upload', upload_file),
        web.get('/api/progress/{task_id}', get_progress),
        web.post('/api/download', download_file),
        web.get('/api/catalog', fetch_catalog),
        web.post('/api/repair', repair_file),
        web.post('/api/prune', clean_orphans),
        web.get('/api/network', get_network_graph),
        web.get('/api/fs/ls', handle_fs_ls),
        web.post('/api/fs/mkdir', handle_fs_mkdir),
        web.post('/api/fs/add_file', handle_fs_add_file),
        web.post('/api/fs/delete', handle_fs_delete),
        web.post('/api/fs/move', handle_fs_move),
        web.static('/downloads', 'downloads'),
    ])
    print(f"Web UI available at http://localhost:{port}")
    web.run_app(app, port=port)


if __name__ == '__main__':
    start_web_server()
