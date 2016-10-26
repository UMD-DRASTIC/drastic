import logging


# Uses the ISO8601 date format, with the optional 'T' character, and a '.' as the decimal separator.
class LogFormatter(logging.Formatter):
    default_format = '%(name)-12s %(asctime)s.%(msecs)03dZ %(levelname)-8s%(message)s'
    debug_format = '%(name)-12s %(asctime)s.%(msecs)03dZ %(levelname)-8s' \
                   '[%(pathname)s:%(funcName)s:%(lineno)s] %(message)s'

    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        orig_fmt = self._fmt
        self._fmt = self.default_format
        self.datefmt = '%Y-%m-%dT%H:%M:%S'

        if record.levelno == logging.DEBUG:
            self._fmt = self.debug_format

        result = logging.Formatter.format(self, record)

        self._fmt = orig_fmt

        return result


def init_log(name):
    logging.basicConfig(level=logging.INFO)

    for handler in logging.root.handlers:
        handler.setFormatter(LogFormatter())

    return logging.getLogger(name)
