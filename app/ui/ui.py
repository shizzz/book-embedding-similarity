from abc import ABC

class BaseUI(ABC):
    def add_progress(self, descr: str, unit: str, show_elapsed: bool = False) -> int:
        pass

    def remove_progress(self, idx: int) -> None:
        pass

    def update_total(self, total: int, idx: int = 0):
        pass

    def done(self, idx: int = 0, count: int = 1):
        pass

    def init(self):
        pass

    async def done_async(self, idx: int = 0, count: int = 1):
        pass

    async def update_total_async(self, total: int, idx: int = 0):
        pass

    async def decrease_total_async(self, decrease: int = 1):
        pass

    async def set_thread(self, worker_id: int, name: str):
        pass

    async def error(self, idx: int = 0):
        pass

    def tqdm(self, description: str, total: int = 0, unit: str = "", show_elapsed: bool = False):
        pass