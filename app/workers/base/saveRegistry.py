class SaveRegistry:
    def __init__(self):
        self._savers = {}

    def register(self, dataset, func):
        self._savers[dataset] = func

    def get(self, dataset):
        return self._savers.get(dataset)