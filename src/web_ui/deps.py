import os
import sys

# Determine imports based on execution context
try:
    # If running as module or from root
    from src.commands import distribute_wrapper as distribute, reconstruct_wrapper as reconstruct, prune_orphans, CatalogClient
    from src.network.discovery import scan_network
    from src.core.repair import RepairManager
    from src.core.metadata import MetadataManager
    from src.network.remote_node import RemoteLibP2PNode
    from src.core.distribution import DistributionStrategy
    from src.core.filesystem import FilesystemManager, DirectoryNode
except ImportError:
    try:
        # If running from src directory
        from commands import distribute_wrapper as distribute, reconstruct_wrapper as reconstruct, prune_orphans, CatalogClient
        from network.discovery import scan_network
        from core.repair import RepairManager
        from core.metadata import MetadataManager
        from network.remote_node import RemoteLibP2PNode
        from core.distribution import DistributionStrategy
        from core.filesystem import FilesystemManager, DirectoryNode
    except ImportError as e:
        print(f"Import Error: {e}")
        # Last resort for direct execution
        sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
        from src.commands import distribute_wrapper as distribute, reconstruct_wrapper as reconstruct, prune_orphans, CatalogClient
        from src.core.distribution import DistributionStrategy
        from src.network.discovery import scan_network
        from src.core.repair import RepairManager
        from src.core.metadata import MetadataManager
        from src.network.remote_node import RemoteLibP2PNode
        from src.core.filesystem import FilesystemManager, DirectoryNode
