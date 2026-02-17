import zipfile
import os
from app.settings.config import BOOK_FOLDER

def get_file_bytes_from_zip(source_link: str) -> bytes:
    if not source_link:
        raise ValueError("source_link is empty")

    if ".zip/" not in source_link:
        raise ValueError(f"Invalid source_link format (expected .zip/...): {source_link}")

    zip_path, fb2_inside = source_link.split(".zip/", 1)

    if not zip_path:
        raise ValueError(f"zip_path is empty in source_link: {source_link}")

    if not fb2_inside:
        raise ValueError(f"fb2_inside is empty in source_link: {source_link}")

    book_folder = BOOK_FOLDER
    if not book_folder.endswith(os.sep):
        book_folder += os.sep

    full_zip_path = f"{book_folder}{zip_path}.zip"

    if not os.path.exists(full_zip_path):
        raise FileNotFoundError(f"ZIP archive not found: {full_zip_path}")

    try:
        with zipfile.ZipFile(full_zip_path, "r") as archive:
            if fb2_inside not in archive.namelist():
                raise FileNotFoundError(
                    f"File '{fb2_inside}' not found in archive '{full_zip_path}'"
                )

            with archive.open(fb2_inside) as f:
                return f.read()

    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"Invalid ZIP archive: {full_zip_path}") from e