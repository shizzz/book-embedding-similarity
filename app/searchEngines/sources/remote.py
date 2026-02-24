import os
import zipfile
import uuid
import asyncio
from app.workers.sources.ui import StatsUI
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
    CHUNK_SIZE: int = 1024 * 1024  # 1MB

    def __init__(self, folder: str, ui: StatsUI = None, locks: dict[str, asyncio.Lock] = {}):
        self.folder = folder
        self._cache_dir = CACHE_DIR
        self._ssh_client = None
        self._sftp = None
        self._smb_connection = None
        self._smb_session = None
        self._smb_tree = None
        self._remote_path = None
        self._archive_locks = locks
        self._ui = ui

        if self._is_remote:
            os.makedirs(self._cache_dir, exist_ok=True)

        self._init_connection()
            
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

    @staticmethod
    def _get_filename(url) -> str:    
        parsed = urlparse(url)
        remote_path = parsed.path
        return os.path.basename(remote_path)

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

        if not os.path.exists(local_path):
            lock = self._archive_locks.get(archive_name)
            if lock is None:
                lock = asyncio.Lock()
                self._archive_locks[archive_name] = lock

            async with lock:
                await asyncio.to_thread(
                    self._download_archive,
                    archive_name,
                    local_path
                )

        return local_path

    async def _download_archive(self, archive_url: str, local_path: str):
        def _sftp_download():
            with self._sftp.file(archive_url, "rb") as remote_file, open(local_path, "wb") as out_file:
                total_size = remote_file.stat().st_size
                downloaded = 0

                if self._ui: self._ui.update_total(total_size, self._progress_idx)

                while True:
                    chunk = remote_file.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if self._ui: self._ui.done(self._progress_idx, len(chunk))

                print()

        def _smb_download():
            path_parts = archive_url.split("/")
            remote_file_path = "/".join(path_parts[1:]) if len(path_parts) > 1 else path_parts[0]

            f = Open(self._smb_tree, remote_file_path)
            f.create()
            downloaded = 0
            total_size = f.size  # если Open предоставляет размер файла
            if self._ui: self._ui.update_total(total_size, self._progress_idx)

            with open(local_path, "wb") as out_file:
                while True:
                    chunk = f.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if self._ui: self._ui.done(self._progress_idx, len(chunk))

            f.close()
            print()

        if self._is_remote:         
            archive_name = RemoteBookScanner._get_filename(archive_url)
            if self._ui: self._progress_idx = self._ui.add_progress(f"Загрузка {archive_name}", "B")

        if self._is_ssh:
            await asyncio.to_thread(_sftp_download)
        elif self._is_smb:
            await asyncio.to_thread(_smb_download)
        else:
            raise ValueError("No valid remote connection")
        
        if self._ui: self._ui.remove_progress(self._progress_idx)

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
        remote = urlparse(archive_url)
        archive_name = RemoteBookScanner._get_filename(archive_url)

        if self._is_remote:     
            local_path = os.path.join(
                self._cache_dir,
                archive_name
            )

            lock = self._archive_locks.get(archive_name)
            if lock is None:
                lock = asyncio.Lock()
                self._archive_locks[archive_name] = lock

            async with lock:     
                if not os.path.exists(local_path):
                    await self._download_archive(remote.path, local_path)
        else:
            local_path = archive_url
            
        return zipfile.ZipFile(local_path)
        
    @asynccontextmanager
    async def open_zip_ctx(self, archive_name: str):
        archive_path = await self._fetch_archive(archive_name)
        zipf = await asyncio.to_thread(zipfile.ZipFile, archive_path)
        try:
            yield zipf
        finally:
            await asyncio.to_thread(zipf.close)
    