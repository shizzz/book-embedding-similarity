import os
import zipfile
import uuid
import asyncio
from io import BytesIO
from urllib.parse import urlparse
from contextlib import asynccontextmanager

import paramiko

from smbprotocol.connection import Connection, Dialects
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open

from app.settings.config import CACHE_DIR


class RemoteBookScanner:
    TYPE = "remote"

    def __init__(self, folder: str, completed: set[str]):
        self.folder = folder
        self.completed = completed
        self._cache_dir = CACHE_DIR
        self._ssh_client = None
        self._sftp = None
        self._smb_connection = None
        self._smb_session = None
        self._smb_tree = None
        self._remote_path = None
        self._archive_locks: dict[str, asyncio.Lock] = {}

        if self._is_remote:
            os.makedirs(self._cache_dir, exist_ok=True)

        self._init_connection()

    # -------------------------
    # flags
    # -------------------------

    @property
    def _is_ssh(self):
        return self.folder.startswith("ssh://")

    @property
    def _is_smb(self):
        return self.folder.startswith("smb://")

    @property
    def _is_remote(self):
        return self._is_ssh or self._is_smb
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _init_connection(self):
        if self._is_ssh:
            parsed = urlparse(self.folder)
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )
            self._ssh_client.connect(
                hostname=parsed.hostname,
                port=parsed.port or 22,
                username=parsed.username,
                password=parsed.password,
            )
            self._sftp = self._ssh_client.open_sftp()
            self._remote_path = parsed.path

        elif self._is_smb:
            parsed = urlparse(self.folder)
            host = parsed.hostname
            share, *parts = parsed.path.lstrip("/").split("/")
            self._remote_path = "/".join(parts)
            self._smb_connection = Connection(
                uuid.uuid4(),
                host,
                445
            )
            self._smb_connection.connect(Dialects.SMB_3_1_1)
            self._smb_session = Session(
                self._smb_connection,
                parsed.username,
                parsed.password
            )
            self._smb_session.connect()
            self._smb_tree = TreeConnect(
                self._smb_session,
                f"\\\\{host}\\{share}"
            )

            self._smb_tree.connect()
        else:
            self._remote_path = self.folder

    def close(self):
        if self._sftp:
            self._sftp.close()

        if self._ssh_client:
            self._ssh_client.close()

        if self._smb_tree:
            self._smb_tree.disconnect()

        if self._smb_session:
            self._smb_session.disconnect()

        if self._smb_connection:
            self._smb_connection.disconnect()

    def list_archives(self) -> list[str]:
        if self._sftp:
            files = self._sftp.listdir(self._remote_path)

        elif self._smb_tree:
            directory = Open(
                self._smb_tree,
                self._remote_path
            )

            directory.create()
            files = [
                f.file_name
                for f in directory.query_directory()
            ]

            directory.close()
        else:
            files = os.listdir(self.folder)

        return [
            f for f in files
            if f.lower().endswith(".zip")
        ]

    async def _fetch_archive(
        self,
        archive_name: str
    ) -> str:
        if not self._is_remote:
            return os.path.join(
                self.folder,
                archive_name
            )

        local_path = os.path.join(
            self._cache_dir,
            archive_name
        )

        if os.path.exists(local_path):
            return local_path

        lock = self._archive_locks.get(archive_name)
        if lock is None:
            lock = asyncio.Lock()
            self._archive_locks[archive_name] = lock

        async with lock:
            if os.path.exists(local_path):
                return local_path

            await asyncio.to_thread(
                self._download_archive,
                archive_name,
                local_path
            )

            return local_path

    def _download_archive(
        self,
        archive_name: str,
        local_path: str
    ):
        if self._sftp:
            remote = os.path.join(
                self._remote_path,
                archive_name
            )

            self._sftp.get(remote, local_path)
        elif self._smb_tree:
            remote = (
                f"{self._remote_path}/{archive_name}"
            )

            f = Open(self._smb_tree, remote)
            f.create()

            with open(local_path, "wb") as out:
                out.write(f.read())

            f.close()

    async def scan_archive(
        self,
        archive_name: str
    ) -> list[tuple[str, str]]:
        async with self.open_zip_ctx(archive_name) as z:
            def _scan(zipf: zipfile.ZipFile):
                result = []
                for info in zipf.infolist():
                    if info.is_dir():
                        continue
                    if info.filename in self.completed:
                        continue
                    result.append(
                        (info.filename, archive_name)
                    )
                return result

            return await asyncio.to_thread(_scan, z)

    async def archive_book_total(self, archive_name: str) -> int:
        async with self.open_zip_ctx(archive_name) as z:
            def _count(zipf: zipfile.ZipFile):
                total = 0
                for info in zipf.infolist():
                    if info.is_dir():
                        continue
                    if info.filename in self.completed:
                        continue
                    total += 1
                return total

            return await asyncio.to_thread(_count, z)

    async def get_book_data(self, archive_name: str, book_name: str) -> bytes:
        async with self.open_zip_ctx(archive_name) as z:
            def _read(zipf: zipfile.ZipFile):
                with zipf.open(book_name) as f:
                    return f.read()

            return await asyncio.to_thread(_read, z)
    
    async def open_zip(self, archive_url: str) -> zipfile.ZipFile:
        if self._is_ssh:
            parsed = urlparse(archive_url)
            remote_path = parsed.path
            data = await asyncio.to_thread(lambda: self._sftp.file(remote_path, "rb").read())
            return zipfile.ZipFile(BytesIO(data))

        elif self._is_smb:
            parsed = urlparse(archive_url)
            path_parts = parsed.path.lstrip("/").split("/")
            remote_path = "/".join(path_parts[1:])
            def _read():
                f = Open(self._smb_tree, remote_path)
                f.create()
                data = f.read()
                f.close()
                return data
            data = await asyncio.to_thread(_read)
            return zipfile.ZipFile(BytesIO(data))

        else:
            return await asyncio.to_thread(zipfile.ZipFile, archive_url)
        
    @asynccontextmanager
    async def open_zip_ctx(self, archive_name: str):
        archive_path = await self._fetch_archive(archive_name)
        zipf = await asyncio.to_thread(zipfile.ZipFile, archive_path)
        try:
            yield zipf
        finally:
            await asyncio.to_thread(zipf.close)
    