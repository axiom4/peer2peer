
import json
import sys
import os


def prune_dht(file_path):
    print(f"Pruning DHT file: {file_path}")

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        return

    # Identify Roots
    # 1. Filesystem Root
    fs_root_key = "filesystem_root_v1"

    # 2. Global Catalog Keys (handle potential prefixes or exact match)
    # The actual key is sha256("catalog_global_v1")
    # But usually it's stored as "catalog_<hash>" ?
    # Let's look for keys starting with "catalog_" OR specific known keys.

    reachable_ids = set()
    queue = []

    # Helper to add to queue if not visited
    def add_to_queue(item_id):
        if item_id and item_id not in reachable_ids and item_id in data:
            reachable_ids.add(item_id)
            queue.append(item_id)

    # 1. Start with FS Root
    if fs_root_key in data:
        root_id = data[fs_root_key]
        print(f"Found Filesystem Root: {root_id}")
        add_to_queue(root_id)
        # Keep the root key itself
        # (We treat the data dict as the source, we will construct a new dict or modify it)
    else:
        print("Warning: No filesystem_root_v1 found.")

    # 2. Start with Catalog
    # Search for catalog keys
    catalog_keys = [k for k in data.keys() if k.startswith("catalog_")]
    for cat_key in catalog_keys:
        print(f"Found Catalog Key: {cat_key}")
        # The value is a LIST of IDs
        cat_list = data[cat_key]
        if isinstance(cat_list, list):
            for item in cat_list:
                # item can be a string ID or a complex object?
                # Based on dht.py, it stores whatever is passed.
                # Usually it's just the manifest ID (string).
                if isinstance(item, str):
                    add_to_queue(item)
                elif isinstance(item, dict) and 'id' in item:
                    add_to_queue(item['id'])

    # Also keep the catalog keys themselves!
    # (We will filter the final dict based on keys kept)

    # BFS Traversal
    while queue:
        current_id = queue.pop(0)
        content_raw = data[current_id]

        # Content can be a string (JSON) or other Types?
        # In dht_index.json, everything seems to be stringified JSON or plain strings (like valid chunks or urls?)
        # See line 460: "http://0.0.0.0:8006"

        if not isinstance(content_raw, str):
            continue

        # Try to parse as JSON
        try:
            content = json.loads(content_raw)
        except:
            # Not JSON, maybe a URL or raw string?
            # If it's a URL, it doesn't point to anything.
            continue

        # Inspect Content
        if isinstance(content, dict):
            # Check for "entries" (Directory)
            if "entries" in content and isinstance(content["entries"], dict):
                for name, entry in content["entries"].items():
                    if "id" in entry:
                        add_to_queue(entry["id"])

            # Check for "chunks" (File Manifest)
            if "chunks" in content and isinstance(content["chunks"], list):
                # Chunks are usually stored as files on disk, NOT in DHT index.
                # BUT if they ARE in DHT index (small chunks optimization?), traverse them.
                for chunk in content["chunks"]:
                    if "id" in chunk:
                        # Usually chunks are NOT in dht_index.json, they are in storage_dir as files.
                        # But if they were, add them.
                        pass

    # Additional Step:
    # Some keys might be "system" keys or "bootstrap" keys.
    # e.g. "http://..." string values might be Node ID mappings?
    # In dht_index.json: "40b6...": "http://0.0.0.0:8006"
    # These seem to be Address cache?
    # If we prune them, we might lose connectivity info.
    # Let's conservatively KEEP all keys that have a value starting with "http".

    keep_keys = set(reachable_ids)
    if fs_root_key in data:
        keep_keys.add(fs_root_key)
    for k in catalog_keys:
        keep_keys.add(k)

    for k, v in data.items():
        if isinstance(v, str) and v.startswith("http"):
            keep_keys.add(k)

    # Calculate stats
    total_keys = len(data)
    keep_count = len(keep_keys)
    removed_count = total_keys - keep_count

    print(f"Total Entries: {total_keys}")
    print(f"Reachable/System Entries: {keep_count}")
    print(f"Orphan Entries to Remove: {removed_count}")

    if removed_count > 0:
        new_data = {k: data[k] for k in keep_keys if k in data}

        # Backup
        backup_path = file_path + ".bak"
        with open(backup_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Backup saved to {backup_path}")

        # Save Pruned
        with open(file_path, 'w') as f:
            # Minified to match style
            json.dump(new_data, f, separators=(',', ':'))
        print(f"Pruned file saved to {file_path}")
    else:
        print("No pruning needed.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python prune_node_dht.py <path_to_dht_index.json>")
    else:
        prune_dht(sys.argv[1])
