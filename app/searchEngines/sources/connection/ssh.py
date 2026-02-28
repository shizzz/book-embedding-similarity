import paramiko
from urllib.parse import urlparse
from .base import BaseConnection

class SSHConnection(BaseConnection):
    def __enter__(self):
        parsed = urlparse(self.folder)

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        self.client.connect(
            hostname=parsed.hostname,
            port=parsed.port or 22,
            username=parsed.username,
            password=parsed.password,
        )

        self.sftp = self.client.open_sftp()
        self.remote_path = parsed.path

        return self

    def list_files(self):
        return self.sftp.listdir(self.remote_path)

    def download(
        self,
        remote: str,
        local: str,
        progress_callback=None,
        resume_from: int = 0
    ):
        full = f"{self.remote_path}/{remote}"
        mode = "ab" if resume_from else "wb"

        with self.sftp.file(full, "rb") as src, open(local, mode) as dst:
            if resume_from:
                src.seek(resume_from)
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)
                if progress_callback:
                    progress_callback(len(chunk))

    def close(self):
        if self.sftp:
            self.sftp.close()

        if self.client:
            self.client.close()

    def get_file_size(self, remote: str) -> int:
        full = f"{self.remote_path}/{remote}"
        return self.sftp.stat(full).st_size