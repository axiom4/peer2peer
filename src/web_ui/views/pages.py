import os
import aiohttp_jinja2
from aiohttp import web


@aiohttp_jinja2.template('index.html')
async def handle_index(request):
    return {'active_page': 'dashboard'}


@aiohttp_jinja2.template('filesystem.html')
async def handle_filesystem(request):
    return {'active_page': 'filesystem'}


@aiohttp_jinja2.template('topology.html')
async def handle_topology(request):
    return {'active_page': 'topology'}
