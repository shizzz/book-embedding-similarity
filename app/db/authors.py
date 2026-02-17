class AuthorRepository:
    def save(conn, book_id: int, authors: list[str]):
        if not authors:
            return
        
        cur = conn.cursor()
        cur.execute("DELETE FROM book_authors WHERE book_id = ?", (book_id,))

        author_ids = []
        for name in authors:
            cur.execute("SELECT id FROM authors WHERE name = ?", (name,))
            row = cur.fetchone()
            if row:
                author_ids.append(row["id"])
            else:
                cur.execute("INSERT INTO authors (name) VALUES (?)", (name,))
                author_ids.append(cur.lastrowid)

        cur.executemany(
            "INSERT INTO book_authors (book_id, author_id) VALUES (?, ?)",
            [(book_id, aid) for aid in author_ids]
        )
    
    
    @staticmethod
    def save_bulk(conn, books: list):
        cursor = conn.cursor()

        # собрать всех авторов
        all_authors = set()
        for book in books:
            if book.authors:
                all_authors.update(book.authors)

        if not all_authors:
            return

        # получить существующих авторов одним запросом
        placeholders = ",".join("?" * len(all_authors))
        cursor.execute(
            f"SELECT id, name FROM authors WHERE name IN ({placeholders})",
            list(all_authors)
        )

        author_map = {row["name"]: row["id"] for row in cursor.fetchall()}

        # вставить новых авторов bulk
        new_authors = [(name,) for name in all_authors if name not in author_map]

        cursor.executemany(
            "INSERT INTO authors (name) VALUES (?)",
            new_authors
        )

        # получить id новых
        if new_authors:
            cursor.execute(
                f"SELECT id, name FROM authors WHERE name IN ({placeholders})",
                list(all_authors)
            )
            author_map = {row["name"]: row["id"] for row in cursor.fetchall()}

        # удалить старые связи bulk
        cursor.executemany(
            "DELETE FROM book_authors WHERE book_id = ?",
            [(book.id,) for book in books]
        )

        # создать новые связи bulk
        relations = []

        for book in books:
            if not book.authors:
                continue

            for name in book.authors:
                relations.append(
                    (book.id, author_map[name])
                )

        cursor.executemany(
            "INSERT INTO book_authors (book_id, author_id) VALUES (?, ?)",
            relations
        )