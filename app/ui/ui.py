from abc import ABC, abstractmethod

class BaseUI(ABC):
    @abstractmethod
    def add_progress(self, descr: str, unit: str, show_elapsed: bool = False) -> int:
        pass

    @abstractmethod
    def remove_progress(self, idx: int) -> None:
        pass

    @abstractmethod
    def update_total(self, total: int, idx: int = 0):
        pass

    @abstractmethod
    def done(self, idx: int = 0, count: int = 1):
        pass

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    async def done_async(self, idx: int = 0, count: int = 1):
        pass

    @abstractmethod
    async def update_total_async(self, total: int, idx: int = 0):
        pass

    @abstractmethod
    async def decrease_total_async(self, decrease: int = 1):
        pass

    @abstractmethod
    async def set_thread(self, worker_id: int, name: str):
        pass

    @abstractmethod
    async def error(self, idx: int = 0):
        pass