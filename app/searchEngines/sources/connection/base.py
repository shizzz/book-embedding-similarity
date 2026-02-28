from abc import ABC, abstractmethod

class BaseConnection(ABC):
    def __init__(self, folder: str):
        self.folder = folder

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @abstractmethod
    def list_files(self) -> list[str]:
        pass

    @abstractmethod
    def download(self, remote: str, local: str, progress_callback=None, resume_from=0):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_file_size(self, remote: str) -> int:
        pass