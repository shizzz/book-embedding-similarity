from app.utils import anonymize_fb2

def run(args):
    path = str(args.path)
    anonymize_fb2.anonymize_fb2(path, path)