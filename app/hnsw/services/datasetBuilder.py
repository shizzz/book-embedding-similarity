from app.models import BookPair

class LTRDatasetAssembler:
    def __init__(
            self, 
            feature_extractor, 
            label_encoder
        ):
        self._features = feature_extractor
        self._labels = label_encoder

    def build(self, pairs: list[BookPair]):
        X, y, groups = [], [], []

        for pair in pairs:
            X.append(self._features.extract(pair))
            y.append(self._labels.encode(pair.raw_label))
            groups.append(pair.source.id)

        return X, y, groups