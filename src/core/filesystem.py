import json
import hashlib
import time
from typing import Dict, List, Optional, Union, Callable, Awaitable


class DirectoryNode:
    """
    Represents a directory in the decentralized filesystem.
    Serialized as a JSON manifest.
    """

    def __init__(self, name: str, entries: Optional[Dict[str, dict]] = None):
        self.name = name
        self.type = "directory"
        self.entries = entries if entries is not None else {}
        self.timestamp = time.time()

    def add_entry(self, name: str, item_type: str, item_id: str, size: int = 0):
        """Adds or updates an entry (file or subdirectory)."""
        self.entries[name] = {
            "name": name,
            "type": item_type,
            "id": item_id,
            "size": size,
            "modified": time.time()
        }

    def remove_entry(self, name: str):
        if name in self.entries:
            del self.entries[name]

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "name": self.name,
            "entries": self.entries,
            "timestamp": self.timestamp
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    def get_hash(self) -> str:
        """Calculates the content-addressable hash of this directory."""
        return hashlib.sha256(self.to_json().encode()).hexdigest()


class FilesystemManager:
    """
    Manages the filesystem tree structure, validation, and traversal.
    """

    def __init__(self):
        pass

    def create_directory(self, name: str) -> DirectoryNode:
        return DirectoryNode(name)

    def load_directory(self, json_content: str) -> DirectoryNode:
        """Parses a JSON string into a DirectoryNode."""
        try:
            data = json.loads(json_content)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON content")

        # Basic validation
        if not isinstance(data, dict):
            raise ValueError("Manifest parse error: not a dict")

        # It might be a file manifest, check type
        if data.get("type") == "directory":
            dir_node = DirectoryNode(
                data.get("name", "unknown"), data.get("entries"))
            dir_node.timestamp = data.get("timestamp", time.time())
            return dir_node
        else:
            raise ValueError("Not a directory manifest")

    async def resolve_path(self, root_id: str, path: str, fetch_node_fn: Callable[[str], Awaitable[Optional[str]]]) -> Optional[str]:
        """
        Traverses a path from a root ID and returns the ID of the target object.
        root_id: ID of the root directory.
        path: Relative path string (e.g. 'documents/work/resume.pdf')
        fetch_node_fn: Async function to get manifest content by ID.
        """
        if not path or path == "/" or path == ".":
            return root_id

        parts = [p for p in path.split("/") if p and p != "."]
        current_id = root_id

        for part in parts:
            if not current_id:
                return None
            manifest_json = await fetch_node_fn(current_id)
            if not manifest_json:
                return None

            try:
                data = json.loads(manifest_json)
                # If we are traversing, current node MUST be a directory
                if data.get("type") != "directory":
                    return None

                entries = data.get("entries", {})
                if part in entries:
                    current_id = entries[part]["id"]
                else:
                    return None
            except Exception:
                return None

        return current_id

    async def delete_path(self, root_id: Optional[str], path: str, entry_name: str,
                          fetch_node_fn: Callable[[str], Awaitable[Optional[str]]],
                          store_node_fn: Callable[[DirectoryNode], Awaitable[str]]) -> str:
        """
        Removes an entry from the directory at 'path'.
        Returns the new Root ID.
        """
        parts = [p for p in path.split("/") if p and p != "."]
        return await self._recursive_delete(root_id, parts, entry_name, fetch_node_fn, store_node_fn)

    async def _recursive_delete(self, current_id: Optional[str], path_parts: List[str],
                                Name: str,
                                fetcher: Callable, storer: Callable) -> str:
        node = None
        if current_id:
            content = await fetcher(current_id)
            if content:
                try:
                    node = self.load_directory(content)
                except ValueError:
                    pass

        if node is None:
            return current_id

        # Base Case: We are at the target container directory
        if not path_parts:
            node.remove_entry(Name)
            return await storer(node)

        # Recursive Case
        next_part = path_parts[0]
        remaining_parts = path_parts[1:]

        if next_part in node.entries:
            child_id = node.entries[next_part]["id"]
            new_child_id = await self._recursive_delete(child_id, remaining_parts, Name, fetcher, storer)
            node.add_entry(next_part, "directory", new_child_id, 0)
            return await storer(node)
        else:
            return current_id

    async def update_path(self, root_id: Optional[str], path: str, new_entry_name: str, new_entry_data: dict,
                          fetch_node_fn: Callable[[str], Awaitable[Optional[str]]],
                          store_node_fn: Callable[[DirectoryNode], Awaitable[str]]) -> str:
        """
        Updates the tree by adding/updating an entry at the given path.
        Recursively recreates parent directories with new hashes.
        Returns the new Root ID.

        root_id: Current root Hash (can be None if initializing new tree)
        path: Path *containing* the new entry (e.g. "docs/work" to put file inside 'work')
        new_entry_name: Name of file/dir to add
        new_entry_data: Dict with keys {'type', 'id', 'size'}
        """
        parts = [p for p in path.split("/") if p and p != "."]
        return await self._recursive_update(root_id, parts, new_entry_name, new_entry_data, fetch_node_fn, store_node_fn)

    async def _recursive_update(self, current_id: Optional[str], path_parts: List[str],
                                Name: str, Data: dict,
                                fetcher: Callable, storer: Callable) -> str:

        # 1. Load current node or create new if doesn't exist
        node = None
        if current_id:
            content = await fetcher(current_id)
            if content:
                try:
                    node = self.load_directory(content)
                except ValueError:
                    print(f"FS WARNING: Failed to parse directory manifest {current_id}")
            else:
                print(f"FS WARNING: Directory manifest {current_id} not found in fetcher!")

        if node is None:
            # CRITICAL: If we are updating an existing path (current_id is set) and we failed to load it,
            # we are about to destroy data by creating a fresh directory.
            if current_id:
                print(f"FS CRITICAL: Data loss prevented. Cannot update path part because {current_id} is missing.")
                # We should probably raise an error here instead of proceeding with a fresh directory
                raise RuntimeError(f"Filesystem consistency error: Node {current_id} missing.")
            
            # Create new directory (Only valid for new paths or root initialization)
            node = DirectoryNode("dir")

        # Base Case: We are at the target container directory
        if not path_parts:
            node.add_entry(Name, Data["type"], Data["id"], Data.get("size", 0))
            return await storer(node)

        # Recursive Case: We need to go deeper
        next_part = path_parts[0]
        remaining_parts = path_parts[1:]

        # Check if next_part exists in current entries
        child_id = None
        if next_part in node.entries:
            child_id = node.entries[next_part]["id"]

        # Recurse to get new child hash
        # We are descending into 'next_part', so the path relative to *that* child is 'remaining_parts'
        new_child_id = await self._recursive_update(child_id, remaining_parts, Name, Data, fetcher, storer)

        # Update current node pointer to new child version
        node.add_entry(next_part, "directory", new_child_id, 0)

        return await storer(node)

    async def move_path(self, root_id: Optional[str], src_path: str, src_name: str,
                        dest_path: str, dest_name: str,
                        fetch_node_fn: Callable[[str], Awaitable[Optional[str]]],
                        store_node_fn: Callable[[DirectoryNode], Awaitable[str]]) -> str:
        """
        Moves an entry from source to destination.
        Returns the new Root ID.
        """
        # 1. Resolve source parent to get the entry metadata
        parent_id = await self.resolve_path(root_id, src_path, fetch_node_fn)
        if not parent_id:
            raise ValueError(f"Source path not found: {src_path}")

        parent_content = await fetch_node_fn(parent_id)
        if not parent_content:
            raise ValueError("Source parent node content missing")

        parent_node = self.load_directory(parent_content)
        if src_name not in parent_node.entries:
            raise ValueError(f"Source file not found: {src_name}")

        entry_data = parent_node.entries[src_name]

        # 2. Delete from source (create intermediate tree)
        intermediate_root = await self.delete_path(root_id, src_path, src_name, fetch_node_fn, store_node_fn)

        # 3. Add to destination (on the intermediate tree)
        # Note: We reuse the same ID, effectively moving the pointer
        new_entry_data = {
            "type": entry_data["type"],
            "id": entry_data["id"],
            "size": entry_data.get("size", 0)
        }

        final_root = await self.update_path(intermediate_root, dest_path, dest_name, new_entry_data, fetch_node_fn, store_node_fn)

        return final_root
