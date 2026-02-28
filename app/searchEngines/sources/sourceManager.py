import os
import io
import zipfile
import asyncio
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
from app.searchEngines.sources.connection import ConnectionFactory
from app.settings.config import CACHE_DIR

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
        ui: Any = None
    ):
        self.folder = folder
        self._ui = ui
        self._cache_dir = CACHE_DIR
        os.makedirs(self._cache_dir, exist_ok=True)
        self._archive_locks: dict[str, asyncio.Lock] = {}

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

    async def open_zip(
        self,
        archive_url: str
    ) -> zipfile.ZipFile:
        """
        Открывает zip-архив по полной ссылке.
        Гарантирует, что архив скачан в кеш.
        """
        # Получаем имя архива из URL
        archive_name = self.get_archive_name(archive_url)

        local_path = os.path.join(self._cache_dir, archive_name)

        # Блокировка для параллельного скачивания
        lock = self._archive_locks.get(archive_name)
        if lock is None:
            lock = asyncio.Lock()
            self._archive_locks[archive_name] = lock

        async with lock:
            if not os.path.exists(local_path):
                # Используем приватный метод для скачивания полного URL
                await self._download_archive(archive_url, local_path)

        return zipfile.ZipFile(local_path)

    # ============================================================
    # zip cache
    # ============================================================
    @asynccontextmanager
    async def open_zip_ctx(
        self,
        archive_name: str
    ) -> AsyncGenerator[zipfile.ZipFile, None]:
        archive_path = await self._ensure_local(archive_name)

        async with BookSourceManager._cache_lock:
            BookSourceManager._access_counter += 1
            access = BookSourceManager._access_counter
            cached = BookSourceManager._zip_cache.get(
                archive_name
            )

            if cached:
                zipf, mem, _ = cached
                BookSourceManager._zip_cache[archive_name] = (
                    zipf,
                    mem,
                    access
                )

            else:
                data = await asyncio.to_thread(
                    lambda: open(
                        archive_path,
                        "rb"
                    ).read()
                )

                mem = io.BytesIO(data)
                zipf = zipfile.ZipFile(mem)

                BookSourceManager._zip_cache[archive_name] = (
                    zipf,
                    mem,
                    access
                )

            # cleanup old entries
            to_remove = [
                name
                for name, (_, _, last)
                in BookSourceManager._zip_cache.items()
                if access - last
                > BookSourceManager._max_idle_accesses
            ]

            for name in to_remove:
                zipf_old, mem_old, _ = \
                    BookSourceManager._zip_cache.pop(name)
                zipf_old.close()

        yield zipf

    # ============================================================
    # download logic
    # ============================================================
    async def _ensure_local(
        self,
        archive_name: str
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
                archive_name,
                local_path
            )

        return local_path

    async def _download_archive(
        self,
        archive_name: str,
        local_path: str
    ):
        """
        Скачивает архив с поддержкой докачки, прогресса в МБ и корректного lock.
        """
        tmp_path = local_path + ".tmp"

        def _download():
            with ConnectionFactory.create(self.folder) as conn:

                # проверяем уже скачанные байты для докачки
                downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0

                progress_idx = None
                if self._ui:
                    progress_idx = self._ui.add_progress(f"Загрузка {archive_name}", "B")
                    total_size = conn.get_file_size(archive_name)
                    self._ui.update_total(int(total_size / 1024 / 1024), progress_idx)
                    if downloaded:
                        # сразу показываем уже скачанные МБ
                        self._ui.done(progress_idx, int(downloaded / 1024 / 1024))

                def progress_callback(bytes_read: int):
                    if self._ui and progress_idx is not None:
                        self._ui.done(progress_idx, int(bytes_read / 1024 / 1024))

                # Скачиваем с resume_from
                conn.download(
                    archive_name,
                    tmp_path,
                    progress_callback=progress_callback,
                    resume_from=downloaded
                )

                os.rename(tmp_path, local_path)

                if self._ui and progress_idx is not None:
                    self._ui.remove_progress(progress_idx)

        # выполняем скачивание в отдельном потоке
        await asyncio.to_thread(_download)
    
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

            await self._download_archive(archive_name, local_path)
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