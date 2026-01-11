import os
from aiohttp import web

async def handle_index(request):
    path = os.path.join(os.path.dirname(__file__), '../static/index.html')
    return web.FileResponse(path)

async def handle_filesystem(request):
    path = os.path.join(os.path.dirname(__file__), '../static/filesystem.html')
    return web.FileResponse(path)

async def handle_topology(request):
    path = os.path.join(os.path.dirname(__file__), '../static/topology.html')
    return web.FileResponse(path)
