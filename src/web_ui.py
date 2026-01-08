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
    from src.main import distribute, reconstruct, prune_orphans
    from src.network.discovery import scan_network
    from src.core.repair import RepairManager
    from src.core.metadata import MetadataManager
except ImportError:
    try:
        # If running from src directory
        from main import distribute, reconstruct, prune_orphans
        from network.discovery import scan_network
        from core.repair import RepairManager
        from core.metadata import MetadataManager
    except ImportError as e:
        print(f"Import Error: {e}")
        # Last resort for direct execution
        import sys
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from src.main import distribute, reconstruct, prune_orphans
        from src.network.discovery import scan_network
        from src.core.repair import RepairManager
        from src.core.metadata import MetadataManager


import uuid

# Global task tracker
TASKS = {}


async def handle_index(request):
    path = os.path.join(os.path.dirname(__file__), 'web/index.html')
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

    # Prepare args for distribute
    args = SimpleNamespace(
        file=temp_path,
        entry_node=None,
        scan=True,
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
        try:
            await loop.run_in_executor(None, lambda: distribute(args, progress_callback=progress_cb))
        except Exception as e:
            print(f"Background distribution error: {e}")
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

    # Auto-append extension if missing
    if not manifest_name.endswith('.manifest'):
        manifest_name += '.manifest'

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
    if not name.endswith('.manifest'):
        name += '.manifest'

    manifest_path = os.path.join('manifests', name)
    if not os.path.exists(manifest_path):
        return web.json_response({"error": "Manifest not found"}, status=404)

    # 1. Load manifest to get chunks
    try:
        with open(manifest_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        return web.json_response({"error": f"Invalid manifest: {e}"}, status=500)

    # 2. Discover Peers
    # Use a stronger discovery method.
    # First, quick UDP scan
    found_peers = await scan_network(timeout=3.0)

    # If possible, crawl to find more peers from the found ones
    queue = list(found_peers)
    visited = set(found_peers)

    if queue:
        print(f"Delete: Initial scan found {len(queue)} peers. Crawling...")
        async with aiohttp.ClientSession() as session:
            # Shallow crawl (depth 1) to find more nodes specially for DELETE
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

    if final_peers:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for chunk in data.get('chunks', []):
                chunk_id = chunk['id']
                # Request deletion on all peers
                for peer in final_peers:
                    # Fire and forget mostly
                    tasks.append(session.delete(f"{peer}/chunk/{chunk_id}"))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    # 3. Delete local manifest file
    try:
        os.remove(manifest_path)
    except Exception as e:
        return web.json_response({"error": f"Failed to delete manifest file: {e}"}, status=500)

    return web.json_response({"status": "ok", "message": f"Deleted manifest {name} and requested chunk deletion on network."})


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

        shard_mgr, chunks_data = result

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
        for chunk_data in shard_mgr.yield_reconstructed_chunks(chunks_data):
            await response.write(chunk_data)

        await response.write_eof()
    except Exception as e:
        print(f"Streaming error: {e}")

    return response


def start_web_server(port=8888):
    # Ensure downloads directory exists for static serving
    os.makedirs('downloads', exist_ok=True)

    app = web.Application()
    # Increase client_max_size for large uploads (e.g. 500MB)
    app._client_max_size = 500 * 1024 * 1024

    app.add_routes([
        web.get('/', handle_index),
        web.get('/api/manifests', list_manifests),
        web.get('/api/manifests/{name}', get_manifest_detail),
        web.get('/api/stream/{name}', stream_download),
        web.delete('/api/manifests/{name}', delete_manifest),
        web.post('/api/upload', upload_file),
        web.get('/api/progress/{task_id}', get_progress),
        web.post('/api/download', download_file),
        web.post('/api/repair', repair_file),
        web.post('/api/prune', clean_orphans),
        web.get('/api/network', get_network_graph),
        web.static('/downloads', 'downloads'),
    ])
    print(f"Web UI available at http://localhost:{port}")
    web.run_app(app, port=port)


if __name__ == '__main__':
    start_web_server()
