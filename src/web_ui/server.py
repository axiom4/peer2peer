import os
import aiohttp
from aiohttp import web
from .views.pages import handle_index, handle_filesystem, handle_topology
from .api.files import (
    list_manifests, get_manifest_detail, get_manifest_by_id,
    stream_download, stream_download_by_id, delete_manifest,
    upload_file, get_progress, download_file, repair_file, clean_orphans
)
from .api.network import fetch_catalog, get_network_graph
from .api.filesystem import (
    handle_fs_ls, handle_fs_mkdir, handle_fs_add_file,
    handle_fs_delete, handle_fs_move
)


def start_web_server(port=8888):
    # Ensure downloads directory exists for static serving
    os.makedirs('downloads', exist_ok=True)

    app = web.Application()
    # Increase client_max_size for large uploads (e.g. 500MB)
    app._client_max_size = 500 * 1024 * 1024

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
        web.static('/static', os.path.join(os.path.dirname(__file__), 'static')),
    ])
    print(f"Web UI available at http://localhost:{port}")
    web.run_app(app, port=port)


if __name__ == '__main__':
    start_web_server()
