import hashlib
from typing import List

class MerkleTree:
    def __init__(self, leaves: List[str]):
        """
        Initialize the Merkle Tree with a list of hashed leaves (hex strings).
        """
        self.leaves = leaves
        self.tree = []
        self._build_tree()

    def _build_tree(self):
        """
        Builds the Merkle Tree from the leaves up to the root.
        """
        if not self.leaves:
            self.tree = [[]]
            return

        current_level = self.leaves
        self.tree = [current_level]

        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                if i + 1 < len(current_level):
                    right = current_level[i + 1]
                else:
                    right = left  # Duplicate last element if odd number of leaves

                combined = left + right
                node_hash = hashlib.sha256(combined.encode('utf-8')).hexdigest()
                next_level.append(node_hash)
            
            self.tree.append(next_level)
            current_level = next_level

    def get_root(self) -> str:
        """
        Returns the Merkle Root of the tree.
        """
        if not self.tree or not self.tree[-1]:
            return None
        return self.tree[-1][0]

    def get_proof(self, index: int) -> List[dict]:
        """
        Generates a Merkle Proof for a leaf at a given index.
        Returns a list of dicts with 'pair' (hash) and 'position' ('left' or 'right').
        """
        if index < 0 or index >= len(self.leaves):
            raise IndexError("Leaf index out of bounds")

        proof = []
        for level in self.tree[:-1]: # Exclude root level
            is_right_child = index % 2 == 1
            sibling_index = index - 1 if is_right_child else index + 1
            
            if sibling_index < len(level):
                sibling_hash = level[sibling_index]
            else:
                # Handle odd number of nodes case where last node is duplicated
                sibling_hash = level[index]

            proof.append({
                "hash": sibling_hash,
                "position": "left" if is_right_child else "right"
            })
            
            index //= 2 # Move up to parent index
            
        return proof

    @staticmethod
    def verify_proof(leaf: str, proof: List[dict], root: str) -> bool:
        """
        Verifies a Merkle Proof.
        """
        current_hash = leaf
        for node in proof:
            sibling_hash = node['hash']
            if node['position'] == 'left':
                combined = sibling_hash + current_hash
            else:
                combined = current_hash + sibling_hash
            
            current_hash = hashlib.sha256(combined.encode('utf-8')).hexdigest()
            
        return current_hash == root
