import uuid
from smbprotocol.connection import Connection, Dialects
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open
from urllib.parse import urlparse
from .base import BaseConnection

class SMBConnection(BaseConnection):
    def __enter__(self):
        parsed = urlparse(self.folder)

        host = parsed.hostname
        share, *parts = parsed.path.lstrip("/").split("/")

        self.remote_path = "/".join(parts)

        self.conn = Connection(uuid.uuid4(), host, 445)
        self.conn.connect()

        self.session = Session(
            self.conn,
            parsed.username,
            parsed.password
        )
        self.session.connect()

        self.tree = TreeConnect(
            self.session,
            f"\\\\{host}\\{share}"
        )
        self.tree.connect()
        return self

    def list_files(self):
        directory = Open(self.tree, self.remote_path)
        directory.create()

        files = [
            f.file_name
            for f in directory.query_directory()
        ]

        directory.close()
        return files

    def download(self, remote: str, local: str, progress_callback=None):
        f = Open(self._smb_tree, remote)
        f.create()
        with open(local, "wb") as dst:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                if progress_callback:
                    progress_callback(len(chunk))
        f.close()

    def close(self):
        self.tree.disconnect()
        self.session.disconnect()
        self.conn.disconnect()

    def get_file_size(self, remote: str) -> int:
        f = Open(self._smb_tree, remote)
        f.create()
        size = f.size
        f.close()
        return size