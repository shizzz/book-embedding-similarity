CREATE TABLE genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER,
    name_ru TEXT NOT NULL,
    name_en TEXT,
    FOREIGN KEY (parent_id) REFERENCES genres(id)
);