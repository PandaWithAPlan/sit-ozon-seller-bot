"""
Утилиты для работы с JSON файлами.
Централизованные функции для безопасного чтения/записи JSON.
"""

import json
import logging
import os

log = logging.getLogger("seller-bot.json_utils")


def safe_read_json(path: str) -> dict:
    """
    Безопасное чтение JSON файла.

    Args:
        path: Путь к JSON файлу

    Returns:
        Словарь с данными или пустой словарь при ошибке
    """
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                log.debug(f"Successfully read JSON from {path}")
                return data or {}
    except json.JSONDecodeError as e:
        log.warning(f"Failed to parse JSON from {path}: {e}")
    except UnicodeDecodeError as e:
        log.warning(f"Failed to decode {path} (encoding issue): {e}")
    except (IOError, OSError) as e:
        log.warning(f"Failed to read JSON file {path}: {e}")
    except Exception as e:
        log.error(f"Unexpected error reading JSON from {path}: {e}", exc_info=True)
    return {}


def safe_write_json(path: str, payload: dict) -> bool:
    """
    Безопасная запись JSON файла.

    Args:
        path: Путь к JSON файлу
        payload: Данные для записи

    Returns:
        True если запись успешна, False в противном случае
    """
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log.debug(f"Successfully wrote JSON to {path}")
        return True
    except (TypeError, ValueError) as e:
        log.error(f"Failed to serialize payload for {path}: {e}")
        return False
    except (IOError, OSError) as e:
        log.error(f"Failed to write JSON file {path}: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error writing JSON to {path}: {e}", exc_info=True)
        return False


def safe_read_json_bytes(path: str, default: bytes = b"") -> bytes:
    """
    Безопасное чтение бинарных данных из JSON файла.

    Args:
        path: Путь к JSON файлу
        default: Значение по умолчанию при ошибке

    Returns:
        JSON-данные в байтах или default при ошибке
    """
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = f.read()
                log.debug(f"Successfully read {len(data)} bytes from {path}")
                return data
    except (IOError, OSError) as e:
        log.warning(f"Failed to read bytes from {path}: {e}")
    except Exception as e:
        log.error(f"Unexpected error reading bytes from {path}: {e}", exc_info=True)

    return default


def safe_write_json_bytes(path: str, data: bytes) -> bool:
    """
    Безопасная запись бинарных данных в JSON файл.

    Args:
        path: Путь к JSON файлу
        data: Данные для записи

    Returns:
        True если запись успешна, False в противном случае
    """
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        with open(path, "wb") as f:
            f.write(data)

        log.debug(f"Successfully wrote {len(data)} bytes to {path}")
        return True
    except (IOError, OSError) as e:
        log.error(f"Failed to write bytes to {path}: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error writing bytes to {path}: {e}", exc_info=True)
        return False


def ensure_dir_exists(path: str) -> bool:
    """
    Обеспечивает существование директории.

    Args:
        path: Путь к директории

    Returns:
        True если директория существует или была создана
    """
    try:
        if path:
            os.makedirs(path, exist_ok=True)
        return True
        return True
    except (IOError, OSError) as e:
        log.error(f"Failed to create directory {path}: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error creating directory {path}: {e}", exc_info=True)
        return False
