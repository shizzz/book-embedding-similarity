import os
import shutil
from .base import BaseConnection

class LocalConnection(BaseConnection):

    def list_files(self):

        return os.listdir(self.folder)

    def download(self, remote, local):

        shutil.copy(
            os.path.join(self.folder, remote),
            local
        )

    def close(self):
        pass
    
    def get_file_size(self, remote: str) -> int:
        pass