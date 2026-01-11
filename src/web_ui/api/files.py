import os
import glob
import json
import uuid
import asyncio
import aiohttp
import random
import hashlib
from types import SimpleNamespace
from aiohttp import web

from ..deps import (
    distribute,
    reconstruct,
    prune_orphans,
    CatalogClient,
    scan_network,
    RepairManager,
    MetadataManager,
    RemoteHttpNode,
    DistributionStrategy
)
from ..state import TASKS
from ..services import get_active_peers, check_chunk_task, get_dht_nodes


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
            results.append({
                "filename": os.path.basename(f).replace(".manifest", ""),
                "size": 0
            })

    return web.json_response(results)


async def _delete_file_from_network(manifest_id, name_hint=None, peers=None, known_chunks=None, force=False):
    if peers is None:
        peers = await scan_network()

    final_peers = list(set(peers)) if peers else []
    chunks_to_delete = known_chunks if known_chunks else []

    if not chunks_to_delete:
        manifest_data = None
        if final_peers:
            async with aiohttp.ClientSession() as session:
                async def _fetch_manifest_race(peer_url):
                    try:
                        async with session.get(f"{peer_url}/chunk/{manifest_id}", timeout=3) as r:
                            if r.status == 200:
                                return await r.read()
                    except Exception:
                        pass
                    return None

                tasks = [asyncio.create_task(_fetch_manifest_race(
                    peer)) for peer in final_peers[:10]]
                for coro in asyncio.as_completed(tasks):
                    try:
                        result = await coro
                        if result:
                            manifest_data = result
                            break
                    except Exception:
                        pass
                for t in tasks:
                    if not t.done():
                        t.cancel()

        if manifest_data:
            try:
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
            if force:
                print(f"Delete: FORCE mode active for {manifest_id}")
            else:
                # Warning instead of Abort - Allows cleaning up "ghost" manifests
                print(f"Delete: Manifest content unavailable. Proceeding with ID removal to clear ghost entry.")
                # raise Exception("Safe Delete Abort: Manifest unavailable. Use force=true to override.")

    broadcast_targets = list(final_peers) if final_peers else []
    local_cluster = [f"http://127.0.0.1:{p}" for p in range(8000, 8020)]
    broadcast_targets = list(set(broadcast_targets + local_cluster))

    if broadcast_targets and (chunks_to_delete or manifest_id):
        async with aiohttp.ClientSession() as session:
            tasks = []
            if chunks_to_delete:
                batch_size = 500
                for i in range(0, len(chunks_to_delete), batch_size):
                    batch = chunks_to_delete[i:i + batch_size]
                    payload = {"chunk_ids": batch}
                    for peer in broadcast_targets:
                        tasks.append(session.post(
                            f"{peer}/chunks/delete_batch", json=payload, timeout=5))

            if manifest_id:
                payload_manifest = {"chunk_ids": [manifest_id]}
                for peer in broadcast_targets:
                    tasks.append(session.post(
                        f"{peer}/chunks/delete_batch", json=payload_manifest, timeout=5))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    if manifest_id:
        try:
            dht_nodes = [RemoteHttpNode(url) for url in broadcast_targets]
            cat = CatalogClient()
            await cat.delete(manifest_id, dht_nodes)
            if name_hint:
                await cat.delete(name_hint, dht_nodes)
        except Exception as e:
            print(f"Delete: Failed to remove from DHT Catalog: {e}")


async def delete_manifest(request):
    name = request.match_info['name']
    is_id = len(name) == 64 and all(
        c in '0123456789abcdefABCDEF' for c in name)

    found_peers = await scan_network(timeout=3.0)
    visited = set(found_peers)
    queue = list(found_peers)

    if queue:
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
    manifest_id_to_delete = None
    chunks_to_delete = []

    if is_id:
        manifest_id_to_delete = name
        manifest_data = None
        if final_peers:
            async with aiohttp.ClientSession() as session:
                for peer in final_peers:
                    try:
                        async with session.get(f"{peer}/chunk/{name}", timeout=2.0) as resp:
                            if resp.status == 200:
                                manifest_data = await resp.read()
                                break
                    except:
                        continue
        if manifest_data:
            try:
                data = json.loads(manifest_data)
                chunks_to_delete = [c['id'] for c in data.get('chunks', [])]
            except Exception as e:
                print(f"Delete: Failed to parse manifest JSON: {e}")
    else:
        if not name.endswith('.manifest'):
            name += '.manifest'
        manifest_path = os.path.join('manifests', name)
        if not os.path.exists(manifest_path):
            return web.json_response({"error": "Manifest not found"}, status=404)

        try:
            cat_nodes = [RemoteHttpNode(u) for u in final_peers] if final_peers else [
                RemoteHttpNode(f"http://127.0.0.1:{p}") for p in range(8000, 8005)]
            cat_client = CatalogClient()
            items = await cat_client.fetch(cat_nodes)
            found_ids = [item['id'] for item in items if item.get(
                'name') == name.replace('.manifest', '') or item.get('name') == name]

            if found_ids:
                manifest_id_to_delete = found_ids[0]

            with open(manifest_path, 'r') as f:
                content_str = f.read()
                data = json.loads(content_str)
                chunks_to_delete = [c['id'] for c in data.get('chunks', [])]
                if not manifest_id_to_delete:
                    # Calculate ID from local file if catalog search failed
                    norm_bytes = json.dumps(data).encode('utf-8')
                    manifest_id_to_delete = hashlib.sha256(
                        norm_bytes).hexdigest()
        except Exception as e:
            return web.json_response({"error": f"Invalid manifest: {e}"}, status=500)

    force_delete = request.query.get('force', '').lower() == 'true'

    try:
        await _delete_file_from_network(manifest_id_to_delete, known_chunks=chunks_to_delete, force=force_delete)
        if not is_id and 'manifest_path' in locals() and os.path.exists(manifest_path):
            os.remove(manifest_path)
    except Exception as e:
        return web.json_response({"error": f"Safe Delete Aborted: {str(e)}"}, status=500)

    return web.json_response({"status": "ok", "message": f"Deleted manifest {name}"})


async def get_manifest_detail(request):
    name = request.match_info['name']
    if not name.endswith('.manifest'):
        name += '.manifest'

    manifest_path = os.path.join('manifests', name)
    if not os.path.exists(manifest_path):
        return web.json_response({"error": "Manifest not found"}, status=404)

    with open(manifest_path, 'r') as f:
        data = json.load(f)

    found_peers = await scan_network(timeout=2.0)
    queue = list(found_peers)
    visited = set(found_peers)
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

    if active_peers:
        async with aiohttp.ClientSession() as session:
            chunk_tasks = []
            for chunk in data['chunks']:
                chunk_tasks.append(check_chunk_task(
                    session, chunk['id'], active_peers))
            results = await asyncio.gather(*chunk_tasks)
            lookup = {cid: locs for cid, locs in results}
            for chunk in data['chunks']:
                chunk['locations'] = lookup.get(chunk['id'], [])

    return web.json_response(data)


async def get_manifest_by_id(request):
    manifest_id = request.match_info['id']
    peers = await scan_network()
    if not peers:
        peers = ['http://127.0.0.1:8000',
                 'http://127.0.0.1:8001', 'http://127.0.0.1:8002']

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
    if not nodes:
        nodes = [RemoteHttpNode(
            f"http://127.0.0.1:{8000+i}") for i in range(5)]
        distributor = DistributionStrategy(nodes)

    try:
        manifest_data_bytes = await asyncio.get_event_loop().run_in_executor(
            None, lambda: distributor.retrieve_chunk(manifest_id)
        )
        data = json.loads(manifest_data_bytes)
    except Exception as e:
        return web.json_response({"error": f"Failed to retrieve manifest: {str(e)}"}, status=404)

    if active_peers:
        async with aiohttp.ClientSession() as session:
            chunk_tasks = []
            for chunk in data['chunks']:
                chunk_tasks.append(check_chunk_task(
                    session, chunk['id'], active_peers))
            results = await asyncio.gather(*chunk_tasks)
            lookup = {cid: locs for cid, locs in results}
            for chunk in data['chunks']:
                chunk['locations'] = lookup.get(chunk['id'], [])

    return web.json_response(data)


async def upload_file(request):
    reader = await request.multipart()
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
            compression = (val.decode('utf-8').lower() == 'true')

    if not temp_path:
        return web.json_response({"status": "error", "message": "No file uploaded"}, status=400)

    cached_peers = await get_active_peers()
    entry_node = random.choice(cached_peers) if cached_peers else None
    use_scan = (entry_node is None)

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

    async def background_distribute():
        try:
            await loop.run_in_executor(None, lambda: distribute(args, progress_callback=progress_cb))
        except Exception as e:
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Internal Error: {str(e)}"
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    asyncio.create_task(background_distribute())

    return web.json_response({"status": "processing", "task_id": task_id, "message": "Upload started"})


async def download_file(request):
    try:
        data = await request.json()
    except:
        return web.json_response({"status": "error", "message": "Invalid JSON"}, status=400)

    manifest_name = data.get('manifest')
    if not manifest_name:
        return web.json_response({"status": "error", "message": "No manifest specified"}, status=400)

    is_id = len(manifest_name) == 64 and all(
        c in '0123456789abcdefABCDEF' for c in manifest_name)

    if not is_id and not manifest_name.endswith('.manifest'):
        manifest_name += '.manifest'

    if is_id:
        manifest_path = manifest_name
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
    TASKS[task_id] = {"status": "processing", "percent": 0,
                      "message": "Initializing download..."}

    def progress_callback(percent, message):
        if task_id in TASKS:
            if percent == -1:
                TASKS[task_id]["status"] = "error"
                TASKS[task_id]["message"] = message
            elif percent == 100:
                TASKS[task_id]["status"] = "completed"
                TASKS[task_id]["percent"] = 100
                TASKS[task_id]["message"] = message
                TASKS[task_id]["download_url"] = f"/downloads/{output_name}"
            else:
                TASKS[task_id]["percent"] = percent
                TASKS[task_id]["message"] = message

    async def background_reconstruct():
        LOOP = asyncio.get_running_loop()
        try:
            await LOOP.run_in_executor(None, lambda: reconstruct(args, progress_callback))
        except Exception as e:
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Internal Error: {str(e)}"

    asyncio.create_task(background_reconstruct())

    return web.json_response({"status": "processing", "task_id": task_id, "message": "Download started"})


async def stream_download(request):
    manifest_name = request.match_info['name']
    if not manifest_name.endswith('.manifest'):
        manifest_name += '.manifest'

    manifest_path = os.path.join('manifests', manifest_name)
    if not os.path.exists(manifest_path):
        return web.Response(status=404, text="Manifest not found")

    try:
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
            original_filename = manifest_data.get(
                'filename', 'downloaded_file')
    except:
        original_filename = 'downloaded_file'

    args = SimpleNamespace(
        manifest=manifest_path, output="dummy_output", entry_node=None,
        scan=True, local=False, kill_node=None
    )

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None, lambda: reconstruct(
                args, progress_callback=None, stream=True)
        )
        if not result:
            return web.Response(status=500, text="Reconstruction failed")
        shard_mgr, chunks_data, compression_mode, manifest_obj = result
    except Exception as e:
        return web.Response(status=500, text=f"Internal Error: {str(e)}")

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
    manifest_id = request.match_info['id']
    args = SimpleNamespace(
        manifest=manifest_id, output="dummy_output", entry_node=None,
        scan=True, local=False, kill_node=None
    )
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None, lambda: reconstruct(
                args, progress_callback=None, stream=True)
        )
        if not result:
            return web.Response(status=404, text=f"File content {manifest_id} not found")
        shard_mgr, chunks_data, compression_mode, manifest_obj = result
        original_filename = manifest_obj.get(
            'filename', f"downloaded_{manifest_id}.bin")
    except Exception as e:
        return web.Response(status=500, text=f"Internal Error: {str(e)}")

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
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Error: {str(e)}"

    asyncio.create_task(background_repair())
    return web.json_response({"status": "processing", "task_id": task_id, "message": "Repair started"})


async def clean_orphans(request):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: prune_orphans(None))
        return web.json_response({"status": "ok", "message": "Orphan chunks pruned."})
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Prune failed: {e}"}, status=500)
