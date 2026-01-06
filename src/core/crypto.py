from cryptography.fernet import Fernet
import base64
import os
from typing import Tuple


class CryptoManager:
    """Handles data encryption and decryption."""

    @staticmethod
    def generate_key() -> bytes:
        """Generates a new random encryption key."""
        return Fernet.generate_key()

    def __init__(self, key: bytes):
        self.fernet = Fernet(key)

    def encrypt(self, data: bytes) -> bytes:
        """Encrypts data."""
        return self.fernet.encrypt(data)

    def decrypt(self, token: bytes) -> bytes:
        """Decrypts data."""
        return self.fernet.decrypt(token)
