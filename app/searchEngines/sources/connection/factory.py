from .ssh import SSHConnection
from .smb import SMBConnection
from .local import LocalConnection
from .base import BaseConnection

class ConnectionFactory:
    @staticmethod
    def create(folder: str) -> BaseConnection:
        if folder.startswith("ssh://"):
            return SSHConnection(folder)
        if folder.startswith("smb://"):
            return SMBConnection(folder)
        return LocalConnection(folder)