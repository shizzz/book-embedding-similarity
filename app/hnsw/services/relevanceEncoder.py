import numpy as np

class RelevanceEncoder:
    def encode(self, raw_label: float) -> int:
        if raw_label <= 0:
            return 0
        return int(np.clip(round(raw_label * 10), 0, 10))