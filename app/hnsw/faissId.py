class FaissId:
    BOOK_SHIFT = 32
    CHUNK_MASK = (1 << 32) - 1

    @staticmethod
    def pack(book_id: int, chunk_id: int) -> int:
        return (book_id << FaissId.BOOK_SHIFT) | chunk_id

    @staticmethod
    def unpack_book(faiss_id: int) -> int:
        return faiss_id >> FaissId.BOOK_SHIFT

    @staticmethod
    def unpack_chunk(faiss_id: int) -> int:
        return faiss_id & FaissId.CHUNK_MASK

    @staticmethod
    def unpack(faiss_id: int) -> tuple[int, int]:
        return (
            faiss_id >> FaissId.BOOK_SHIFT,
            faiss_id & FaissId.CHUNK_MASK
        )