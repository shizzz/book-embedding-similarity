from dataclasses import dataclass
import zipfile
import pickle
import numpy as np
from typing import List
from app.settings.config import BOOK_FOLDER

@dataclass
class Book:
    archive_name: str
    file_name: str
    title: str = None
    embedding: np.ndarray = None
    authors: List[str] = None
    completed: bool = False

    def __init__(
            self,
            archive_name: str,
            file_name: str,
            title: str = None,
            embedding: str = None,
            authors: List[str] = None,
            completed: bool = None):
        self.archive_name = archive_name
        self.file_name = file_name
        self.title = title
        self.authors = authors
        self.completed = completed
        self.embedding = self.__norm_embedding(embedding)
        
    def __norm_embedding(self, embedding: str) -> np.ndarray | None:
        if self.embedding is not None:
            return self.embedding

        if embedding is None:
            return None

        try:
            emb = pickle.loads(embedding)

            if not isinstance(emb, np.ndarray):
                return None

            norm = np.linalg.norm(emb)
            if norm < 1e-9:
                return None

            return (emb / norm).astype(np.float32)

        except Exception:
            return None

    def get_file_bytes_from_zip(self) -> bytes:
        zip_path = f"{BOOK_FOLDER}/{self.archive_name}"

        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open(self.file_name) as f:
                return f.read()

class BookRegistry:
    books: list[Book]
    
    def __init__(self, books: list[Book] = None):
        if books:
            self.books = books
        else:
            self.books: List[Book] = []

    def add_books(self, books: list[Book]):
        self.books = books