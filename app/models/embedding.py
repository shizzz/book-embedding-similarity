import numpy as np

class Embedding:
    __slots__ = ("vec",)

    def __init__(self, vec: np.ndarray):
        self.vec = vec.astype(np.float32)

    @classmethod
    def from_db(cls, blob: bytes) -> "Embedding":
        if blob is None:
            return None
        arr = np.frombuffer(blob, dtype=np.float32).copy()
        norm = np.linalg.norm(arr)
        if norm < 1e-9:
            return None
        arr /= norm
        return cls(arr)

    def to_db(self) -> bytes:
        norm = np.linalg.norm(self.vec)
        if norm < 1e-9:
            vec = np.zeros_like(self.vec, dtype=np.float32)
        else:
            vec = self.vec / norm
        return vec.tobytes()
