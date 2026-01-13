import os
import aiohttp
import aiohttp_jinja2
import jinja2
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
from . import state
try:
    from network.p2p_server import P2PServer
except ImportError:
    from src.network.p2p_server import P2PServer
import asyncio

async def on_startup(app):
    # Initialize Global P2P Server for the Web UI Node
    # Find a free random port to ensure valid advertising (port 0 is invalid for connect)
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    free_port = sock.getsockname()[1]
    sock.close()
    
    server = P2PServer("127.0.0.1", free_port, "uploads_temp")
    await server.initialize()
    # Start server logic (discovery etc) in background
    asyncio.create_task(server.start())
    state.GLOBAL_P2P_SERVER = server
    print(f"Global P2P Server initialized for Web UI: {server.host.get_id().to_string()} on port {free_port}")

async def on_cleanup(app):
    # Retrieve server from state if needed, but trio loop handling is complex.
    # For now, let it die with the process.
    pass

def start_web_server(port=8888):
    # Ensure downloads directory exists for static serving
    os.makedirs('downloads', exist_ok=True)

    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Setup Jinja2 template loader
    templates_path = os.path.join(os.path.dirname(__file__), 'templates')
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(templates_path))

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
