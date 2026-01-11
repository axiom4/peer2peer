import json
import uuid
import asyncio
from aiohttp import web
from ..deps import DirectoryNode
from ..state import TASKS, FS_MANAGER, FS_LOCK
from ..services import (
    fs_get_root_id,
    fs_set_root_id,
    fs_fetch_node_fn,
    fs_store_node_fn,
    get_active_peers,
    scan_network
)
from .files import _delete_file_from_network

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

async def _recursive_collect_manifests(node_id):
    """
    Recursively collects all file manifest IDs from a directory structure.
    Returns a list of tuples: (id, name)
    """
    manifests = []
    try:
        content = await fs_fetch_node_fn(node_id)
        if not content:
            return []

        if isinstance(content, bytes):
            content = content.decode('utf-8')

        try:
            node_data = json.loads(content)
        except:
            return []

        if node_data.get("type") != "directory":
            return []

        entries = node_data.get("entries", {})
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
                                    collected = await _recursive_collect_manifests(entry["id"])
                                    manifests_to_delete.extend(collected)
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

                active_peers = await get_active_peers()
                if not active_peers:
                    active_peers = await scan_network()

                sem = asyncio.Semaphore(10)

                async def delete_with_name_scan_optimized(mid, fname):
                    async with sem:
                        await _delete_file_from_network(mid, name_hint=fname, peers=active_peers)
                        return fname

                pending = []
                for mid, fname in manifests_to_delete:
                    t = asyncio.create_task(
                        delete_with_name_scan_optimized(mid, fname))
                    pending.append(t)

                for i, t in enumerate(asyncio.as_completed(pending)):
                    finished_name = await t
                    completed += 1
                    percent = 20 + int((completed / total) * 80)
                    TASKS[task_id]["progress"] = percent
                    TASKS[task_id]["message"] = f"Deleted {completed}/{total}: {finished_name}"
                    if i % 2 == 0:
                        await asyncio.sleep(0.01)

            TASKS[task_id]["status"] = "completed"
            TASKS[task_id]["progress"] = 100
            TASKS[task_id]["message"] = "Deletion completed"

        except Exception as e:
            traceback.print_exc()
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = str(e)

    asyncio.create_task(_cancel_task_fn())

    return web.json_response({"status": "processing", "task_id": task_id})


async def handle_fs_move(request):
    """
    POST /api/fs/move
    """
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    source_path = data.get("source_path", "/")
    source_name = data.get("source_name")
    dest_path = data.get("dest_path", "/")
    dest_name = data.get("dest_name") or source_name

    if not source_name:
        return web.json_response({"error": "source_name required"}, status=400)

    full_src = (source_path.rstrip('/') + '/' + source_name).lstrip('/')
    full_dest = dest_path.lstrip('/')

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
