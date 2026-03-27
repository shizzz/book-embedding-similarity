from app.utils import anonymize_fb2
from app.workers.stats import Stats

def run(args, stats: Stats = None):
    path = str(args.path)
    anonymize_fb2.anonymize_fb2(path, path)