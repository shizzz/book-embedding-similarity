import os
import zipfile
import uuid
import asyncio
import paramiko
import io
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from smbprotocol.connection import Connection, Dialects
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open
from typing import Any
from app.settings.config import CACHE_DIR


class RemoteBookScanner:
    TYPE = "remote"
    CHUNK_SIZE: int = 1024 * 1024  # 1MB
    _zip_cache: dict[str, tuple[zipfile.ZipFile, io.BytesIO, int]] = {}
    _access_counter: int = 0
    _cache_lock = asyncio.Lock()
    _max_idle_accesses = 1000

    def __init__(self, folder: str, ui: Any = None, locks: dict[str, asyncio.Lock] = {}):
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
            parsed = urlparse(self.folder)
            if self._is_ssh:
                self._remote_path = parsed.path
            elif self._is_smb:
                _, *parts = parsed.path.lstrip("/").split("/")
                self._remote_path = "/".join(parts)
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _ensure_connection(self):
        if not self._is_remote:
            return

        if self._ssh_client or self._smb_connection:
            return

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

            self._smb_connection = Connection(uuid.uuid4(), host, 445)
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
        self._ensure_connection()
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
                url = os.path.join(
                    self._remote_path,
                    archive_name
                )
                await self._download_archive(url, local_path)

        return local_path

    async def _download_archive(self, archive_url: str, local_path: str):
        tmp_path = local_path + ".tmp"

        def _sftp_download():
            self._ensure_connection()
            archive_name = RemoteBookScanner._get_filename(archive_url)
            if self._ui:
                self._progress_idx = self._ui.add_progress(f"Загрузка {archive_name}", "B")

            # Проверяем, сколько уже скачано
            downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0

            with self._sftp.file(archive_url, "rb") as remote_file:
                total_size = remote_file.stat().st_size
                if downloaded:
                    remote_file.seek(downloaded)

                mode = "ab" if downloaded else "wb"
                with open(tmp_path, mode) as out_file:
                    if self._ui:
                        self._ui.update_total(total_size, self._progress_idx)

                    while True:
                        chunk = remote_file.read(self.CHUNK_SIZE)
                        if not chunk:
                            break
                        out_file.write(chunk)
                        downloaded += len(chunk)
                        if self._ui:
                            self._ui.done(self._progress_idx, len(chunk))

            os.rename(tmp_path, local_path)
            if self._ui:
                self._ui.remove_progress(self._progress_idx)

        def _smb_download():
            path_parts = archive_url.split("/")
            remote_file_path = "/".join(path_parts[1:]) if len(path_parts) > 1 else path_parts[0]

            f = Open(self._smb_tree, remote_file_path)
            f.create()
            total_size = f.size
            downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            if downloaded:
                f.seek(downloaded)

            mode = "ab" if downloaded else "wb"
            if self._ui:
                archive_name = os.path.basename(remote_file_path)
                self._progress_idx = self._ui.add_progress(f"Загрузка {archive_name}", "B")
                self._ui.update_total(total_size, self._progress_idx)

            with open(tmp_path, mode) as out_file:
                while True:
                    chunk = f.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if self._ui:
                        self._ui.done(self._progress_idx, len(chunk))

            f.close()
            os.rename(tmp_path, local_path)
            if self._ui:
                self._ui.remove_progress(self._progress_idx)

        if self._is_remote:
            self._ensure_connection()

        if self._is_ssh:
            await asyncio.to_thread(_sftp_download)
        elif self._is_smb:
            await asyncio.to_thread(_smb_download)
        else:
            raise ValueError("No valid remote connection")

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

        async with RemoteBookScanner._cache_lock:
            RemoteBookScanner._access_counter += 1
            access = RemoteBookScanner._access_counter

            cached = RemoteBookScanner._zip_cache.get(archive_name)

            if cached:
                zipf, mem, _ = cached
                RemoteBookScanner._zip_cache[archive_name] = (zipf, mem, access)
            else:
                data = await asyncio.to_thread(
                    lambda: open(archive_path, "rb").read()
                )

                mem = io.BytesIO(data)
                zipf = zipfile.ZipFile(mem)

                RemoteBookScanner._zip_cache[archive_name] = (
                    zipf,
                    mem,
                    access
                )

            # cleanup старых
            to_remove = [
                name
                for name, (_, _, last) in RemoteBookScanner._zip_cache.items()
                if access - last > RemoteBookScanner._max_idle_accesses
            ]

            for name in to_remove:
                zipf_old, mem_old, _ = RemoteBookScanner._zip_cache.pop(name)
                zipf_old.close()

        yield zipf
    