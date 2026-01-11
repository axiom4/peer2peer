
import sys
import os
import glob
import json


def clean_chunk(chunk_id):
    if not chunk_id:
        print("Error: No chunk ID provided")
        return

    print(f"Cleaning chunk: {chunk_id}")

    # 1. Remove Physical Files
    pattern = f"network_data/node_data_*/{chunk_id}"
    files = glob.glob(pattern)
    if files:
        print(f"Found {len(files)} physical files. Removing...")
        for f in files:
            try:
                os.remove(f)
                print(f" - Deleted {f}")
            except Exception as e:
                print(f" - Failed to delete {f}: {e}")
    else:
        print("No physical files found.")

    # 2. Remove from DHT Indexes
    dht_files = glob.glob("network_data/node_data_*/dht_index.json")
    for dht_file in dht_files:
        try:
            with open(dht_file, 'r') as f:
                data = json.load(f)

            if chunk_id in data:
                print(f"Found entry in {dht_file}. Removing...")
                del data[chunk_id]

                # Check for other references?
                # e.g. if chunk_id is used as a value?
                # This could be expensive to search values.
                # Assuming the main issue is the Top Level Key mapping to a provider.

                with open(dht_file, 'w') as f:
                    json.dump(data, f, separators=(',', ':'))
            else:
                pass
                # print(f" - Not found in {dht_file}")

        except Exception as e:
            print(f"Error processing {dht_file}: {e}")

    print("Cleanup complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python clean_chunk.py <chunk_id>")
    else:
        clean_chunk(sys.argv[1])
