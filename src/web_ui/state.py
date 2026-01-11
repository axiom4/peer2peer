import asyncio
import hashlib
from .deps import FilesystemManager

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
