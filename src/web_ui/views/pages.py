import os
from aiohttp import web


async def handle_index(request):
    path = os.path.join(os.path.dirname(__file__), '../templates/index.html')
    return web.FileResponse(path)


async def handle_filesystem(request):
    path = os.path.join(os.path.dirname(__file__),
                        '../templates/filesystem.html')
    return web.FileResponse(path)


async def handle_topology(request):
    path = os.path.join(os.path.dirname(__file__),
                        '../templates/topology.html')
    return web.FileResponse(path)
