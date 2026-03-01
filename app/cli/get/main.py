def run(args):
    if args.similar:
        from app.cli.get.get_similar import main
        main(args.mode, args.file) 