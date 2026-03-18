
import asyncio
from app.workers import ParseBooks
from app.parsers.book import ParserConfig

def run(args):
    cnf = ParserConfig(
        target_chars=args.target_chars,
        min_chars=args.min_chars,
        max_description_chars=args.max_description_chars,
        sections=args.chunks,
        prefix_buffer=args.prefix_buffer,
        sections_ratio=args.sections_ratio,
    )
    worker = ParseBooks(cnf)
    asyncio.run(worker.run())