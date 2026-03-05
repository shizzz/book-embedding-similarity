from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Literal

from app.settings import PathsConfig
from app.infrastructure.models import Book, Feedbacks
from .toSimilarBooks import to_similar_list

class Html:
    templates = Jinja2Templates(directory=f"{PathsConfig.BASE_DIR}/api/templates")
    SearchType = Literal["title", "author"]

    @staticmethod
    def make_lib_url(value: str, kind: SearchType) -> str:
        return f"{PathsConfig.LIB_URL}/#/{kind}?{kind}={value}"

    @staticmethod
    def render_similar_table(
        request: Request,
        base_book: Book,
        similars: list[tuple[float, int, int]],
        elapsed: float,
        feedbacks: Feedbacks
    ) -> HTMLResponse:
        similars_converted = to_similar_list(similars)

        rows = [
            {
                "file_name": s.candidate.file_name,
                "score": s.score,
                "title": s.candidate.title,
                "authors": s.candidate.authors,
                "rating": feedbacks.get_rating(base_book.id, s.candidate.id),
            }
            for s in similars_converted
        ]

        return Html.templates.TemplateResponse(
            "similar_table.html",
            {
                "request": request,
                "base_book": base_book,
                "rows": rows,
                "elapsed": elapsed,
                "source_file_name": base_book.file_name,
                "make_lib_url": Html.make_lib_url,
            }
        )