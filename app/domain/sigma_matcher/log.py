import logging
import logging.handlers
import os
import time

from app.config.settings import get_settings


class DefaultFormatter(logging.Formatter):
    converter = time.gmtime
    fmt = "[%(asctime)s.%(msecs)03dZ][%(levelname)s][%(name)s] %(message)s"

    def __init__(self):
        datefmt = "%Y-%m-%dT%H:%M:%S"
        style = "%"
        super().__init__(fmt=self.fmt, datefmt=datefmt, style=style)


def setup_logger(name: str) -> None:
    conf = get_settings()
    logger = logging.getLogger(name)
    print(f"log_level: {conf.log_level}")
    logger.setLevel(conf.log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(DefaultFormatter())
    logger.addHandler(handler)

    if conf.enable_logging_to_file:
        logs_dir = conf.logs_dir
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        if not os.path.isdir(logs_dir):
            raise Exception(
                f"{logs_dir} is not a directory. Choose another logs directory in parameter LOGS_DIR"
            )
        file_path = os.path.join(logs_dir, f"{name}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            file_path, maxBytes=1000000, backupCount=5
        )
        file_handler.setFormatter(DefaultFormatter())
        logger.addHandler(file_handler)
