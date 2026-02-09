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