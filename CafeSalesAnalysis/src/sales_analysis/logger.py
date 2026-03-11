import logging

def setup_logger(name, level: str, msg="", console=True, log=True):
    """Configure a custom logger.
    To initialize, use "logger = logger.setup_logger(__name__, "{level}")
    where level = debug, info, warning, error, or critical"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("app.log")
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt = '%H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        if console:
            logger.addHandler(console_handler)
        if log:
            logger.addHandler(file_handler)
    match(level.lower()):
        case "debug":
            logger.setLevel(logging.DEBUG)
            logger.debug(msg)
        case "info":
            logger.setLevel(logging.INFO)
            logger.info(msg)
        case "warning":
            logger.setLevel(logging.WARNING)
            logger.warning(msg)
        case "error":
            logger.setLevel(logging.ERROR)
            logger.error(msg)
        case "critical":
            logger.setLevel(logging.CRITICAL)
            logger.critical(msg)
        case _:
            raise ValueError(f"Invalid logging level {level}")
    return logger