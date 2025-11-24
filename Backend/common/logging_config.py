import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(service_name: str, log_dir: str = "/app/logs"):
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(service_name)
    logger.setLevel(logging.DEBUG)  # capture everything

    # -------- Formatter --------
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # -------- Console Handler --------
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    # -------- File Handler --------
    file_handler = RotatingFileHandler(
        f"{log_dir}/{service_name}.log", 
        maxBytes=5 * 1024 * 1024,
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Prevent duplicate logs
    logger.propagate = False

    return logger
