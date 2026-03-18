import os
import io
import zipfile
import asyncio
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict
from app.workers.stats import Stats
from app.searchEngines.sources.connection import ConnectionFactory
from app.settings import PathsConfig

class BookSourceManager:
    """
    Manager orchestrating access to book archives from
    local, SSH or SMB sources.
    """
    CHUNK_SIZE = 1024 * 1024

    # archive_name -> (ZipFile, BytesIO, last_access)
    _zip_cache: dict[str, tuple[zipfile.ZipFile, io.BytesIO, int]] = {}
    _cache_lock = asyncio.Lock()
    _access_counter = 0
    _max_idle_accesses = 1000

    def __init__(
        self,
        folder: str,
        stats: Stats = None
    ):
        self.folder = folder
        self.path = urlparse(folder).path
        self._stats = stats
        self._cache_dir = PathsConfig.CACHE_DIR
        os.makedirs(self._cache_dir, exist_ok=True)
        self._archive_locks: dict[str, asyncio.Lock] = {}
        self._archive_bytes_cache: Dict[str, io.BytesIO] = {}

    @property
    def is_remote(self) -> bool:
        return self.folder.startswith("ssh://") or self.folder.startswith("smb://")

    # ============================================================
    # public API
    # ============================================================
    def list_archives(self) -> list[str]:
        with ConnectionFactory.create(self.folder) as conn:
            files = conn.list_files()

        return [
            f for f in files
            if f.lower().endswith(".zip")
        ]

    async def scan_archive(
        self,
        archive_name: str
    ) -> list[tuple[str, str]]:
        async with self.open_zip_ctx(archive_name) as zipf:
            def _scan():
                result = []
                for info in zipf.infolist():
                    if info.is_dir():
                        continue
                    result.append(
                        (info.filename, archive_name)
                    )
                return result
            return await asyncio.to_thread(_scan)

    async def archive_book_total(
        self,
        archive_name: str
    ) -> int:
        async with self.open_zip_ctx(archive_name) as zipf:
            def _count():
                total = 0
                for info in zipf.infolist():
                    if info.is_dir():
                        continue
                    total += 1
                return total
            return await asyncio.to_thread(_count)

    async def get_book_data(
        self,
        archive_name: str,
        book_name: str
    ) -> bytes:
        async with self.open_zip_ctx(archive_name) as zipf:
            def _read():
                with zipf.open(book_name) as f:
                    return f.read()
            return await asyncio.to_thread(_read)

    async def _ensure_archive_on_disk(self, archive_url: str) -> tuple[str, str]:
        """
        Гарантирует, что архив скачан на диск.
        Возвращает (archive_name, local_path)
        """
        archive_name = self.get_archive_name(archive_url)
        path = urlparse(archive_url).path
        local_path = os.path.join(self._cache_dir, archive_name)

        lock = self._archive_locks.get(archive_name)
        if lock is None:
            lock = asyncio.Lock()
            self._archive_locks[archive_name] = lock

        async with lock:
            if not os.path.exists(local_path):
                await self._download_archive(path, local_path)

        return archive_name, local_path

    def unload_archive_from_memory(self, archive_url: str) -> None:
        archive_name = self.get_archive_name(archive_url)

        buffer = self._archive_bytes_cache.pop(archive_name, None)
        if buffer is not None:
            buffer.close()

    async def load_archive_to_memory(self, archive_url: str) -> None:
        archive_name, local_path = await self._ensure_archive_on_disk(archive_url)

        if archive_name in self._archive_bytes_cache:
            return

        # важно: после await могла появиться запись
        if archive_name in self._archive_bytes_cache:
            return

        with open(local_path, "rb") as f:
            data = f.read()

        self._archive_bytes_cache[archive_name] = io.BytesIO(data)

    async def open_zip(
        self,
        archive_url: str
    ) -> zipfile.ZipFile:
        archive_name = self.get_archive_name(archive_url)

        # 1. memory cache
        buffer = self._archive_bytes_cache.get(archive_name)
        if buffer is not None:
            buffer.seek(0)
            return zipfile.ZipFile(buffer)

        # 2. ensure disk
        _, local_path = await self._ensure_archive_on_disk(archive_url)

        return zipfile.ZipFile(local_path)

    # ============================================================
    # zip cache
    # ============================================================
    @asynccontextmanager
    async def open_zip_ctx(
        self,
        archive_name: str
    ) -> AsyncGenerator[zipfile.ZipFile, None]:
        buffer = self._archive_bytes_cache.get(archive_name)

        if buffer is not None:
            # читаем из памяти в отдельном потоке
            zipf = await asyncio.to_thread(lambda: zipfile.ZipFile(io.BytesIO(buffer.getvalue()), "r"))
        else:
            # fallback: диск
            archive_path = await self._ensure_local(archive_name)
            zipf = await asyncio.to_thread(lambda: zipfile.ZipFile(archive_path, "r"))

        try:
            yield zipf
        finally:
            zipf.close()

    # ============================================================
    # download logic
    # ============================================================
    async def _ensure_local(
        self,
        archive_name: str,
        url: str = None
    ) -> str:
        local_path = os.path.join(
            self._cache_dir,
            archive_name
        )

        if os.path.exists(local_path):
            return local_path

        lock = self._archive_locks.get(
            archive_name
        )

        if lock is None:
            lock = asyncio.Lock()
            self._archive_locks[archive_name] = lock

        async with lock:
            if os.path.exists(local_path):
                return local_path

            await self._download_archive(
                url or f"{self.path}{archive_name}",
                local_path
            )

        return local_path

    async def _download_archive(
        self,
        remote_path: str,
        local_path: str
    ):
        """
        Скачивает архив с поддержкой докачки, прогресса в МБ и корректного lock.
        """
        tmp_path = local_path + ".tmp"
        loop = asyncio.get_running_loop()
        archive_name = self.get_archive_name(remote_path)

        def _download():
            with ConnectionFactory.create(self.folder) as conn:

                # проверяем уже скачанные байты для докачки
                downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0

                if self._stats:
                    total_size = conn.get_file_size(remote_path)
                    loop.call_soon_threadsafe(
                        asyncio.create_task,
                        self._stats.set_total(archive_name, total_size)
                    )
                    loop.call_soon_threadsafe(
                        asyncio.create_task,
                        self._stats.done(archive_name, int(total_size / 1024 / 1024))
                    )
                    
                    if downloaded:
                        # сразу показываем уже скачанные МБ
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            self._stats.done(archive_name, int(downloaded / 1024 / 1024))
                        )

                def progress_callback(bytes_read: int):
                    if self._stats:
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            self._stats.done(archive_name, int(bytes_read / 1024 / 1024))
                        )

                # Скачиваем с resume_from
                conn.download(
                    remote_path,
                    tmp_path,
                    progress_callback=progress_callback,
                    resume_from=downloaded
                )

                os.rename(tmp_path, local_path)

        if self._stats: await self._stats.register_stage(archive_name, 1)
        await asyncio.to_thread(_download)
        if self._stats: await self._stats.unregister_stage(archive_name, 1)
    
    async def ensure_archive_in_cache(self, archive_name: str) -> str:
        local_path = os.path.join(self._cache_dir, archive_name)

        if os.path.exists(local_path):
            return local_path

        lock = self._archive_locks.get(archive_name)
        if lock is None:
            lock = asyncio.Lock()
            self._archive_locks[archive_name] = lock

        async with lock:
            if os.path.exists(local_path):
                return local_path

            await self._download_archive(f"{self.path}{archive_name}", local_path)
        return local_path
    
    # ============================================================
    # utility
    # ============================================================
    @staticmethod
    def get_archive_name(
        url: str
    ) -> str:
        parsed = urlparse(url)
        return os.path.basename(
            parsed.path
        )

    @staticmethod    
    def format_bytes(size: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"