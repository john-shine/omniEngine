import sys
import logging

def get_logger(name, level=None):
    if level is None:
        level = logging.INFO

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.hasHandlers():
        fmt = logging.Formatter(
            fmt="%(asctime)-11s %(name)s:%(lineno)d %(levelname)s: %(message)s", 
            datefmt="[%Y/%m/%d-%H:%M:%S]"
        )
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(fmt)

        logger.addHandler(stream_handler)

    return logger
