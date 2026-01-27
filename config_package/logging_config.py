import logging
import sys

def setup_logging(level: int = logging.INFO) -> None:
    """
    Настройка глобального логирования.
    """
    # Сбрасываем (если было)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Форматтер
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    # Стрим хендлер (stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # Настройка рутового логгера
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(stream_handler)

    # Подавление шума от библиотек
    logging.getLogger("dotenv").setLevel(logging.ERROR)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    logging.info("Logging configured successfully")
