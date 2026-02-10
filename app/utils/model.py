import os
from sentence_transformers import SentenceTransformer
from app.settings.config import MODEL_NAME, DATA_DIR

class Model:
    MODEL_DIR = "models"

    def get(self) -> SentenceTransformer:    
        model_dir = DATA_DIR / Model.MODEL_DIR
        model_path = model_dir / MODEL_NAME

        model_dir.mkdir(parents=True, exist_ok=True)
        if os.path.exists(model_path):
            model = SentenceTransformer(str(model_path))
        else:
            model = SentenceTransformer(MODEL_NAME)
            model.save(str(model_path))
        
        return model