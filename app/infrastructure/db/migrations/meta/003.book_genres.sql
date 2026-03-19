CREATE TABLE IF NOT EXISTS book_genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    book_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL,

    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE,
    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE SET NULL,

    UNIQUE (book_id, genre_id, model_id)
);

CREATE TABLE IF NOT EXISTS book_cenroids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    book_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL,
    distance FLOAT NOT NULL,

    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE SET NULL,

    UNIQUE (book_id, genre_id, model_id)
);
