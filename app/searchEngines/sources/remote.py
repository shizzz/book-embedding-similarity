import os
import zipfile
from io import BytesIO
from urllib.parse import urlparse
import paramiko
from smbprotocol.connection import Connection, Dialects
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open
import uuid
from app.models import Book


class RemoteBookScanner:
    TYPE = "remote"

    def __init__(self, folder: str, completed: set[str]):
        self.folder = folder
        self.completed = completed

        self._ssh_client = None
        self._sftp = None

        self._smb_connection = None
        self._smb_session = None
        self._smb_tree = None

        self._init_connection()

    # -----------------------------
    # Инициализация соединения
    # -----------------------------
    def _init_connection(self):
        if self.folder.startswith("ssh://"):
            parsed = urlparse(self.folder)
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh_client.connect(
                hostname=parsed.hostname,
                port=parsed.port or 22,
                username=parsed.username,
                password=parsed.password
            )
            self._sftp = self._ssh_client.open_sftp()
            self._remote_path = parsed.path

        elif self.folder.startswith("smb://"):
            parsed = urlparse(self.folder)
            host = parsed.hostname
            share_name, *path_parts = parsed.path.lstrip("/").split("/")
            self._remote_path = "/".join(path_parts)
            username = parsed.username
            password = parsed.password

            self._smb_connection = Connection(uuid.uuid4(), host, 445)
            self._smb_connection.connect(Dialects.SMB_3_1_1)
            self._smb_session = Session(self._smb_connection, username, password)
            self._smb_session.connect()
            self._smb_tree = TreeConnect(self._smb_session, f"\\\\{host}\\{share_name}")
            self._smb_tree.connect()
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # -----------------------------
    # Закрытие соединений
    # -----------------------------
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

    # -----------------------------
    # Список архивов
    # -----------------------------
    def list_archives(self) -> list[str]:
        if self._sftp:
            files = self._sftp.listdir(self._remote_path)
            return [f for f in files if f.lower().endswith(".zip")]

        elif self._smb_tree:
            def list_files(path: str) -> list[str]:
                directory = Open(self._smb_tree, path)
                directory.create()
                files = [f.file_name for f in directory.query_directory()]
                directory.close()
                return files

            files = list_files(self._remote_path)
            return [f for f in files if f.lower().endswith(".zip")]

        else:
            return [f for f in os.listdir(self.folder) if f.lower().endswith(".zip")]

    # -----------------------------
    # Подсчет количества книг в архиве
    # -----------------------------
    def archive_book_total(self, archive_name: str) -> int:
        archive_bytes = self._read_archive(archive_name)
        total = 0
        with zipfile.ZipFile(archive_bytes) as z:
            for info in z.infolist():
                if info.is_dir() or info.filename in self.completed:
                    continue
                total += 1
        return total

    # -----------------------------
    # Сканирование архива
    # -----------------------------
    def scan_archive(self, archive_name: str) -> list[tuple[str, str]]:
        archive_bytes = self._read_archive(archive_name)
        result = []
        with zipfile.ZipFile(archive_bytes) as z:
            for info in z.infolist():
                if info.is_dir() or info.filename in self.completed:
                    continue
                result.append((info.filename, archive_name))
        return result

    # -----------------------------
    # Получение данных книги
    # -----------------------------
    def get_book_data(self, archive_name: str, book_name: str) -> bytes:
        archive_bytes = self._read_archive(archive_name)
        with zipfile.ZipFile(archive_bytes) as z:
            with z.open(book_name) as f:
                return f.read()

    # -----------------------------
    # Внутренний метод чтения архива в BytesIO
    # -----------------------------
    def _read_archive(self, archive_name: str) -> BytesIO:
        if self._sftp:
            path = os.path.join(self._remote_path, archive_name)
            data = self._sftp.file(path, "rb").read()
            return BytesIO(data)

        elif self._smb_tree:
            path = f"{self._remote_path}/{archive_name}"
            f = Open(self._smb_tree, path)
            f.create()
            data = f.read()
            f.close()
            return BytesIO(data)

        else:
            path = os.path.join(self.folder, archive_name)
            with open(path, "rb") as f:
                return BytesIO(f.read())
