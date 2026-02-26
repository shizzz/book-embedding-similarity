import logging

class ConsoleHandler(logging.Handler):
    """
    Логгер, который пишет в rich Console или в обычную консоль.
    """
    def __init__(self, console=None):
        super().__init__()
        self.console = console  # может быть None, тогда пишет в stdout

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        if self.console:
            self.console.log(msg)  # rich Console
        else:
            print(msg)  # fallback в stdout