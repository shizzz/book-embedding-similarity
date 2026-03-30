from app.ui import DummyUI
from app.workers.stats import Stats
from app.workers import ParseBooks
from app.parsers.book import ParserConfig

async def run(args, stats: Stats = None):
    cnf = ParserConfig(
        target_chars=args.target_chars,
        min_chars=args.min_chars,
        max_description_chars=args.max_description_chars,
        sections=args.chunks,
        prefix_buffer=args.prefix_buffer,
        sections_ratio=args.sections_ratio,
        ui=DummyUI() if args.disable_ui else None,
    )
    worker = ParseBooks(config=cnf, stats=stats)
    await worker.run()